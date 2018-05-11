#![feature(fn_traits, test)]

extern crate futures;
extern crate futures_cpupool;
extern crate test;

use std::cell::{RefCell, UnsafeCell};
use std::collections::LinkedList;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, MutexGuard};

use futures::*;
use futures_cpupool::CpuPool;

enum Frame {
    Read(task::Task),
    Write(task::Task),
}

enum HandlerState {
    Empty,
    Reading(usize),
    Writing,
}

struct Handler {
    state: HandlerState,
    frame_list: LinkedList<Frame>,
}

impl Handler {
    fn new() -> Self {
        Self {
            state: HandlerState::Empty,
            frame_list: LinkedList::new(),
        }
    }
}

struct Data<T>
    where T: Send
{
    u: UnsafeCell<T>,
}

impl<T> Data<T>
    where T: Send
{
    fn new(t: T) -> Self {
        Self { u: UnsafeCell::new(t) }
    }
}

unsafe impl<T> Send for Data<T> where T: Send {}

unsafe impl<T> Sync for Data<T> where T: Send {}

struct Inner<T>
    where T: Send
{
    pool: Option<CpuPool>,
    handler: Mutex<Handler>,
    data: Data<T>,
}

pub struct Var<T>
    where T: Send
{
    inner: Arc<Inner<T>>,
}

impl<T> Var<T>
    where T: 'static + Send
{
    pub fn new(t: T, pool: Option<CpuPool>) -> Self {
        Self {
            inner: Arc::new(Inner {
                pool: pool,
                handler: Mutex::new(Handler::new()),
                data: Data::new(t),
            }),
        }
    }

    pub fn poll<F, I, E>(&self, f: F) -> Poll<I, E>
        where F: FnOnce(&T, &Var<T>) -> Poll<I, E> + Send + 'static,
    {
        let mut with = With::new(self.clone(), move |i, v| {
            match f.call_once((i, v)) {
                Ok(Async::Ready(r)) => Ok::<Option<I>, E>(Some(r)),
                Ok(Async::NotReady) => Ok(None),
                Err(err) => Err(err),
            }
        });

        match with.poll() {
            Ok(Async::Ready(r)) => {
                match r {
                    Some(r) => Ok(Async::Ready(r)),
                    None => Ok(Async::NotReady),
                }
            }

            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(err) => Err(err),
        }
    }

    pub fn poll_mut<F, I, E>(&self, f: F) -> Poll<I, E>
        where F: FnOnce(&mut T, &Var<T>) -> Poll<I, E> + Send + 'static,
    {
        let mut with_mut = WithMut::new(self.clone(), move |i, v| {
            match f.call_once((i, v)) {
                Ok(Async::Ready(r)) => Ok::<Option<I>, E>(Some(r)),
                Ok(Async::NotReady) => Ok(None),
                Err(err) => Err(err),
            }
        });

        match with_mut.poll() {
            Ok(Async::Ready(r)) => {
                match r {
                    Some(r) => Ok(Async::Ready(r)),
                    None => Ok(Async::NotReady),
                }
            }

            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(err) => Err(err),
        }
    }

    pub fn with<F, R, I, E>(&self, f: F) -> With<T, F, R, I, E>
        where F: FnOnce(&T, &Var<T>) -> R + Send + 'static,
              R: IntoFuture<Item = I, Error = E>,
    {
        With::new(self.clone(), f)
    }

    pub fn with_mut<F, R, I, E>(&self, f: F) -> WithMut<T, F, R, I, E>
        where F: FnOnce(&mut T, &Var<T>) -> R + Send + 'static,
              R: IntoFuture<Item = I, Error = E>,
    {
        WithMut::new(self.clone(), f)
    }

    fn next(&self) {
        let do_next = |mut handler: MutexGuard<Handler>| {
            match handler.frame_list.pop_front() {
                Some(Frame::Read(task)) => {
                    handler.state = HandlerState::Reading(1);
                    drop(handler);
                    set_wake_flag();
                    task.notify();
                }

                Some(Frame::Write(task)) => {
                    handler.state = HandlerState::Writing;
                    drop(handler);
                    set_wake_flag();
                    task.notify();
                }

                None => (),
            }
        };

        {
            let mut handler = self.inner.handler.lock().unwrap();

            match handler.state {
                HandlerState::Reading(cnt) => {
                    if cnt != 1 {
                        handler.state = HandlerState::Reading(cnt-1);
                    } else {
                        handler.state = HandlerState::Empty;
                        do_next(handler);
                    }
                }

                HandlerState::Writing => {
                    handler.state = HandlerState::Empty;
                    do_next(handler);
                }

                _ => panic!(),
            }
        }
    }

    fn next_para(&self) {
        let do_next = |mut handler: MutexGuard<Handler>, pool: &CpuPool| {
            thread_local! {
                static ROUND: RefCell<i32> = RefCell::new(0);
            }

            let round_add = || {
                ROUND.with(|round| {
                    if *round.borrow() < 128 {
                        *round.borrow_mut() += 1;
                        true
                    } else {
                        *round.borrow_mut() = 0;
                        false
                    }
                })
            };

            let round_clear = || {
                ROUND.with(|round| {
                    *round.borrow_mut() = 0;
                })
            };

            match handler.frame_list.pop_front() {
                Some(Frame::Read(task)) => {
                    let mut cnt = 1usize;
                    handler.state = HandlerState::Reading(cnt);

                    loop {
                        if let Some(&Frame::Read(_)) = handler.frame_list.front() {
                            if let Some(Frame::Read(task)) = handler.frame_list.pop_front() {
                                cnt += 1;
                                handler.state = HandlerState::Reading(cnt);

                                pool.spawn_fn(move || {
                                    set_wake_flag();
                                    task.notify();
                                    Ok::<(), ()>(())
                                }).forget();
                            }
                        } else {
                            break;
                        }
                    }

                    drop(handler);

                    if round_add() {
                        set_wake_flag();
                        task.notify();
                    } else {
                        pool.spawn_fn(move || {
                            set_wake_flag();
                            task.notify();
                            Ok::<(), ()>(())
                        }).forget();
                    }
                }

                Some(Frame::Write(task)) => {
                    handler.state = HandlerState::Writing;
                    drop(handler);

                    if round_add() {
                        set_wake_flag();
                        task.notify();
                    } else {
                        pool.spawn_fn(move || {
                            set_wake_flag();
                            task.notify();
                            Ok::<(), ()>(())
                        }).forget();
                    }
                }

                None => {
                    drop(handler);
                    round_clear();
                }
            }
        };

        {
            let pool = self.inner.pool.as_ref().unwrap();
            let mut handler = self.inner.handler.lock().unwrap();
            match handler.state {
                HandlerState::Reading(cnt) => {
                    if cnt != 1 {
                        handler.state = HandlerState::Reading(cnt-1);
                    } else {
                        handler.state = HandlerState::Empty;
                        do_next(handler, pool);
                    }
                }

                HandlerState::Writing => {
                    handler.state = HandlerState::Empty;
                    do_next(handler, pool);
                }

                _ => panic!(),
            }
        }
    }
}

impl<T> Clone for Var<T>
    where T: Send
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

enum FutureState {
    First,
    Second,
}

thread_local! {
    static WAKE_FLAG: RefCell<bool> = RefCell::new(false);
}

fn set_wake_flag() {
    WAKE_FLAG.with(|w| {
        *w.borrow_mut() = true;
    })
}

fn clear_wake_flag() {
    WAKE_FLAG.with(|w| {
        *w.borrow_mut() = false;
    })
}

fn get_wake_flag() -> bool {
    WAKE_FLAG.with(|w| {
        *w.borrow()
    })
}

struct WithInner<T, F, R, I, E>
    where T: Send,
          F: FnOnce(&T, &Var<T>) -> R + Send + 'static,
          R: IntoFuture<Item = I, Error = E>,
{
    var: Var<T>,
    f: Option<F>,
    phantom: PhantomData<(R, I, E)>,
}

enum WithState<T, F, R, I, E>
    where T: Send,
          F: FnOnce(&T, &Var<T>) -> R + Send + 'static,
          R: IntoFuture<Item = I, Error = E>,
{
    Current(WithInner<T, F, R, I, E>),
    Next(R::Future),
}

pub struct With<T, F, R, I, E>
    where T: Send,
          F: FnOnce(&T, &Var<T>) -> R + Send + 'static,
          R: IntoFuture<Item = I, Error = E>,
{
    state: WithState<T, F, R, I, E>,
}

impl<T, F, R, I, E> With<T, F, R, I, E>
    where T: Send,
          F: FnOnce(&T, &Var<T>) -> R + Send + 'static,
          R: IntoFuture<Item = I, Error = E>,
{
    fn new(var: Var<T>, f: F) -> Self {
        Self {
            state: WithState::Current(WithInner {
                var: var,
                f: Some(f),
                phantom: PhantomData,
            }),
        }
    }
}

impl<T, F, R, I, E> Future for With<T, F, R, I, E>
    where T: Send + 'static,
          F: FnOnce(&T, &Var<T>) -> R + Send + 'static,
          R: IntoFuture<Item = I, Error = E>,
{
    type Item = I;
    type Error = E;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let (r, n) = match self.state {
            WithState::Current(ref mut inner) => {
                match inner.var.inner.pool {
                    Some(_) => {
                        if !get_wake_flag() {
                            let mut handler = inner.var.inner.handler.lock().unwrap();
                            match handler.state {
                                HandlerState::Empty => {
                                    handler.state = HandlerState::Reading(1);
                                    drop(handler);

                                    match inner.f.take() {
                                        Some(f) => {
                                            let mut n = (unsafe { f.call_once((&*inner.var.inner.data.u.get(), &inner.var)) }).into_future();
                                            inner.var.next_para();
                                            match n.poll() {
                                                Ok(Async::Ready(r)) => (Ok(Async::Ready(r)), None),
                                                Ok(Async::NotReady) => (Ok(Async::NotReady), Some(n)),
                                                Err(e) => (Err(e), None),
                                            }
                                        }

                                        None => panic!(),
                                    }
                                }

                                HandlerState::Reading(cnt) => {
                                    handler.state = HandlerState::Reading(cnt+1);
                                    drop(handler);

                                    match inner.f.take() {
                                        Some(f) => {
                                            let mut n = (unsafe { f.call_once((&*inner.var.inner.data.u.get(), &inner.var)) }).into_future();
                                            inner.var.next_para();
                                            match n.poll() {
                                                Ok(Async::Ready(r)) => (Ok(Async::Ready(r)), None),
                                                Ok(Async::NotReady) => (Ok(Async::NotReady), Some(n)),
                                                Err(e) => (Err(e), None),
                                            }
                                        }

                                        None => panic!(),
                                    }
                                }

                                HandlerState::Writing => {
                                    handler.frame_list.push_back(Frame::Read(task::current()));
                                    drop(handler);
                                    (Ok(Async::NotReady), None)
                                }
                            }
                        } else {
                            clear_wake_flag();

                            match inner.f.take() {
                                Some(f) => {
                                    let mut n = (unsafe { f.call_once((&*inner.var.inner.data.u.get(), &inner.var)) }).into_future();
                                    inner.var.next_para();
                                    match n.poll() {
                                        Ok(Async::Ready(r)) => (Ok(Async::Ready(r)), None),
                                        Ok(Async::NotReady) => (Ok(Async::NotReady), Some(n)),
                                        Err(e) => (Err(e), None),
                                    }
                                }

                                None => panic!(),
                            }
                        }
                    }

                    None => {
                        if !get_wake_flag() {
                            let mut handler = inner.var.inner.handler.lock().unwrap();
                            match handler.state {
                                HandlerState::Empty => {
                                    handler.state = HandlerState::Reading(1);
                                    drop(handler);

                                    match inner.f.take() {
                                        Some(f) => {
                                            let mut n = (unsafe { f.call_once((&*inner.var.inner.data.u.get(), &inner.var)) }).into_future();
                                            inner.var.next();
                                            match n.poll() {
                                                Ok(Async::Ready(r)) => (Ok(Async::Ready(r)), None),
                                                Ok(Async::NotReady) => (Ok(Async::NotReady), Some(n)),
                                                Err(e) => (Err(e), None),
                                            }
                                        }

                                        None => panic!(),
                                    }
                                }

                                HandlerState::Reading(cnt) => {
                                    handler.state = HandlerState::Reading(cnt+1);
                                    drop(handler);

                                    match inner.f.take() {
                                        Some(f) => {
                                            let mut n = (unsafe { f.call_once((&*inner.var.inner.data.u.get(), &inner.var)) }).into_future();
                                            inner.var.next();
                                            match n.poll() {
                                                Ok(Async::Ready(r)) => (Ok(Async::Ready(r)), None),
                                                Ok(Async::NotReady) => (Ok(Async::NotReady), Some(n)),
                                                Err(e) => (Err(e), None),
                                            }
                                        }

                                        None => panic!(),
                                    }
                                }

                                HandlerState::Writing => {
                                    handler.frame_list.push_back(Frame::Read(task::current()));
                                    (Ok(Async::NotReady), None)
                                }
                            }
                        } else {
                            clear_wake_flag();

                            match inner.f.take() {
                                Some(f) => {
                                    let mut n = (unsafe { f.call_once((&*inner.var.inner.data.u.get(), &inner.var)) }).into_future();
                                    inner.var.next_para();
                                    match n.poll() {
                                        Ok(Async::Ready(r)) => (Ok(Async::Ready(r)), None),
                                        Ok(Async::NotReady) => (Ok(Async::NotReady), Some(n)),
                                        Err(e) => (Err(e), None),
                                    }
                                }

                                None => panic!(),
                            }
                        }
                    }
                }
            }

            WithState::Next(ref mut r) => (r.poll(), None),
        };

        if let Some(n) = n {
            self.state = WithState::Next(n);
        }
        r
    }
}

struct WithMutInner<T, F, R, I, E>
    where T: Send,
          F: FnOnce(&mut T, &Var<T>) -> R + Send + 'static,
          R: IntoFuture<Item = I, Error = E>,
{
    state: FutureState,
    var: Var<T>,
    f: Option<F>,
    phantom: PhantomData<(R, I, E)>,
}

enum WithMutState<T, F, R, I, E>
    where T: Send,
          F: FnOnce(&mut T, &Var<T>) -> R + Send + 'static,
          R: IntoFuture<Item = I, Error = E>,
{
    Current(WithMutInner<T, F, R, I, E>),
    Next(R::Future),
}

pub struct WithMut<T, F, R, I, E>
    where T: Send,
          F: FnOnce(&mut T, &Var<T>) -> R + Send + 'static,
          R: IntoFuture<Item = I, Error = E>,
{
    state: WithMutState<T, F, R, I, E>,
}

impl<T, F, R, I, E> WithMut<T, F, R, I, E>
    where T: Send,
          F: FnOnce(&mut T, &Var<T>) -> R + Send + 'static,
          R: IntoFuture<Item = I, Error = E>,
{
    fn new(var: Var<T>, f: F) -> Self {
        Self {
            state: WithMutState::Current(WithMutInner {
                state: FutureState::First,
                var: var,
                f: Some(f),
                phantom: PhantomData,
            }),
        }
    }
}

impl<T, F, R, I, E> Future for WithMut<T, F, R, I, E>
    where T: Send + 'static,
          F: FnOnce(&mut T, &Var<T>) -> R + Send + 'static,
          R: IntoFuture<Item = I, Error = E>,
{
    type Item = I;
    type Error = E;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let (r, n) = match self.state {
            WithMutState::Current(ref mut inner) => {
                match inner.var.inner.pool {
                    Some(_) => {
                        if !get_wake_flag() {
                            let mut handler = inner.var.inner.handler.lock().unwrap();
                            match handler.state {
                                HandlerState::Empty => {
                                    handler.state = HandlerState::Writing;
                                    drop(handler);

                                    match inner.f.take() {
                                        Some(f) => {
                                            let mut n = (unsafe { f.call_once((&mut *inner.var.inner.data.u.get(), &inner.var)) }).into_future();
                                            inner.var.next_para();
                                            match n.poll() {
                                                Ok(Async::Ready(r)) => (Ok(Async::Ready(r)), None),
                                                Ok(Async::NotReady) => (Ok(Async::NotReady), Some(n)),
                                                Err(e) => (Err(e), None),
                                            }
                                        }

                                        None => panic!(),
                                    }
                                }

                                HandlerState::Reading(_) | HandlerState::Writing => {
                                    handler.frame_list.push_back(Frame::Write(task::current()));
                                    drop(handler);
                                    inner.state = FutureState::Second;
                                    (Ok(Async::NotReady), None)
                                }
                            }
                        } else {
                            clear_wake_flag();

                            match inner.f.take() {
                                Some(f) => {
                                    let mut n = (unsafe { f.call_once((&mut *inner.var.inner.data.u.get(), &inner.var)) }).into_future();
                                    inner.var.next_para();
                                    match n.poll() {
                                        Ok(Async::Ready(r)) => (Ok(Async::Ready(r)), None),
                                        Ok(Async::NotReady) => (Ok(Async::NotReady), Some(n)),
                                        Err(e) => (Err(e), None),
                                    }
                                }

                                None => panic!(),
                            }
                        }
                    }

                    None => {
                        if !get_wake_flag() {
                            let mut handler = inner.var.inner.handler.lock().unwrap();
                            match handler.state {
                                HandlerState::Empty => {
                                    handler.state = HandlerState::Writing;
                                    drop(handler);

                                    match inner.f.take() {
                                        Some(f) => {
                                            let mut n = (unsafe { f.call_once((&mut *inner.var.inner.data.u.get(), &inner.var)) }).into_future();
                                            inner.var.next();
                                            match n.poll() {
                                                Ok(Async::Ready(r)) => (Ok(Async::Ready(r)), None),
                                                Ok(Async::NotReady) => (Ok(Async::NotReady), Some(n)),
                                                Err(e) => (Err(e), None),
                                            }
                                        }

                                        None => panic!(),
                                    }
                                }

                                HandlerState::Reading(_) | HandlerState::Writing => {
                                    handler.frame_list.push_back(Frame::Write(task::current()));
                                    (Ok(Async::NotReady), None)
                                }
                            }
                        } else {
                            clear_wake_flag();

                            match inner.f.take() {
                                Some(f) => {
                                    let mut n = (unsafe { f.call_once((&mut *inner.var.inner.data.u.get(), &inner.var)) }).into_future();
                                    inner.var.next_para();
                                    match n.poll() {
                                        Ok(Async::Ready(r)) => (Ok(Async::Ready(r)), None),
                                        Ok(Async::NotReady) => (Ok(Async::NotReady), Some(n)),
                                        Err(e) => (Err(e), None),
                                    }
                                }

                                None => panic!(),
                            }
                        }
                    }
                }
            }

            WithMutState::Next(ref mut r) => (r.poll(), None),
        };

        if let Some(n) = n {
            self.state = WithMutState::Next(n);
        }
        r
    }
}

#[cfg(test)]
mod tests {
    use std::{panic, process};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};

    use test::Bencher;
    use futures_cpupool::CpuPool;
    use Var;

    #[test]
    fn test() {
        panic::set_hook(Box::new(|_| { process::exit(-1); }));

        let cnt = 10;
        let bar = Arc::new(Barrier::new(cnt+1));
        let pool = CpuPool::new_num_cpus();
        let v = Var::new(10, None);

        pool.clone().spawn(v.with_mut(move |_, v| {
            for _ in 0..cnt {
                pool.clone().spawn(v.with_mut(|i, _| {
                    *i = *i - 1;
                    println!("value: {}", *i);
                    Ok::<(), ()>(())
                })).forget();
            }
            Ok::<(), ()>(())
        })).forget();

        bar.wait();
    }

    #[bench]
    fn bench(b: &mut Bencher) {
        panic::set_hook(Box::new(|_| { process::exit(-1); }));

        let bar = Arc::new(Barrier::new(2));
        let pool = CpuPool::new_num_cpus();
        let v = Var::new((), Some(pool.clone()));

        b.iter(move || {
            let cnt = 10000;
            let i = Arc::new(AtomicUsize::new(0));

            for _ in 0..cnt {
                let i = i.clone();
                let bar = bar.clone();

                pool.spawn(v.with(move |_, _| {
                    if i.fetch_add(1, Ordering::SeqCst) == cnt-1 {
                        bar.wait();
                    }
                    Ok::<(), ()>(())
                })).forget();
            }

            bar.wait();
        });
    }
}
