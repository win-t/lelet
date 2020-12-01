use std::cell::Cell;
use std::cell::RefCell;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::RwLock;
use std::task::Poll;
use std::task::Waker;

use async_task::Runnable;
use crossbeam_utils::Backoff;

use lelet_defer::defer;

use crate::deque::Injector;
use crate::deque::Steal;
use crate::poll_fn::poll_fn;

thread_local! {
    static LOCAL: RefCell<Option<Local>> = RefCell::new(None);
}

struct Local {
    // number of task
    len: Cell<usize>,

    // queue of runnable task
    queue: Arc<Injector<Runnable>>,

    // to notify that there is a task to be run
    // atomic bool is for optimization, it is true when len of vec > 0
    // atomic bool MUST be writen when holding write lock to the vec
    wakers: Arc<(AtomicBool, RwLock<Vec<Waker>>)>,
}

// push new task
pub fn push(task: impl Future<Output = ()> + 'static) {
    LOCAL.with(|local| {
        // ensure we have the Local instance
        if local.borrow().is_none() {
            local.borrow_mut().replace(Local {
                len: Cell::new(0),
                queue: Arc::new(Injector::new()),
                wakers: Arc::new((AtomicBool::new(false), RwLock::new(Vec::new()))),
            });
        }

        let local = local.borrow();
        let local = local.as_ref().unwrap();

        // increment the len, and decrement it after the task done
        local.len.set(local.len.get() + 1);
        let task = async {
            defer! {
                LOCAL.with(|local| {
                    let local = local.borrow();
                    let local = local.as_ref().unwrap();
                    local.len.set(local.len.get() - 1);

                    // if this is last task
                    if local.len.get() == 0 {
                        wake_all(&local.wakers);
                    }
                });
            }
            task.await;
        };

        let schedule = {
            let queue = local.queue.clone();
            let wakers = local.wakers.clone();
            move |r| {
                queue.push(r);
                wake_all(&wakers);
            }
        };
        let (r, t) = unsafe { async_task::spawn_unchecked(task, schedule) };
        t.detach();
        r.schedule();
    });
}

#[inline(always)]
fn wake_all(wakers: &(AtomicBool, RwLock<Vec<Waker>>)) {
    if !wakers.0.load(Ordering::Relaxed) {
        return;
    }

    let mut wakers_lock = wakers
        .1
        .write()
        .expect("acquiring wakers write lock when wake_all");

    for w in wakers_lock.drain(..) {
        w.wake();
    }

    wakers.0.store(false, Ordering::Relaxed);

    drop(wakers_lock);
}

pub struct Poller<'a> {
    _lifetime: PhantomData<&'a ()>,

    // !Send + !Sync
    _not_send: PhantomData<*mut ()>,
}

pub fn poller<'a>() -> Poller<'a> {
    Poller {
        _lifetime: PhantomData,
        _not_send: PhantomData,
    }
}

impl<'a> Poller<'a> {
    // poll single task in the runnable queue
    #[inline(always)]
    pub fn poll_one(&self) -> bool {
        LOCAL.with(|local| {
            if local.borrow().is_none() {
                return false;
            }

            let local = local.borrow();
            let local = local.as_ref().unwrap();

            let backoff = Backoff::new();
            loop {
                match local.queue.steal() {
                    Steal::Success(r) => {
                        r.run();
                        return true;
                    }
                    Steal::Empty => {
                        return false;
                    }
                    Steal::Retry => backoff.snooze(),
                }
            }
        })
    }

    // wait until we have runnable task
    #[inline(always)]
    pub async fn wait(&self) -> bool {
        poll_fn(|cx| {
            LOCAL.with(|local| {
                if local.borrow().is_none() {
                    return Poll::Ready(false);
                }

                let local = local.borrow();
                let local = local.as_ref().unwrap();

                macro_rules! check {
                    () => {
                        if local.len.get() == 0 {
                            return Poll::Ready(false);
                        }

                        if !local.queue.is_empty() {
                            return Poll::Ready(true);
                        }
                    };
                }

                check!();

                let waker = cx.waker();
                let mut need_to_store = true;

                if local.wakers.0.load(Ordering::Relaxed) {
                    let wakers_lock = local
                        .wakers
                        .1
                        .read()
                        .expect("acquiring wakers read lock on poll");

                    for w in wakers_lock.iter() {
                        if w.will_wake(waker) {
                            need_to_store = false;
                            break;
                        }
                    }

                    drop(wakers_lock);
                }

                if need_to_store {
                    let mut wakers_lock = local
                        .wakers
                        .1
                        .write()
                        .expect("acquiring wakers write lock on poll");

                    // check again while holding write lock
                    check!();

                    wakers_lock.push(waker.clone());

                    local.wakers.0.store(true, Ordering::Relaxed);

                    drop(wakers_lock);
                }

                Poll::Pending
            })
        })
        .await
    }
}
