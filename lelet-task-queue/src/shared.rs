use std::cell::Cell;
use std::cell::RefCell;
use std::future::Future;
use std::marker::PhantomData;
use std::ptr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Once;
use std::sync::RwLock;
use std::task::Poll;
use std::task::Waker;

use async_task::Runnable;
use crossbeam_utils::sync::ShardedLock;
use crossbeam_utils::Backoff;

use lelet_defer::defer;

use crate::deque::Injector;
use crate::deque::Steal;
use crate::deque::Stealer;
use crate::deque::Worker;
use crate::poll_fn::poll_fn;

thread_local! {
    static LOCAL: RefCell<(usize, Option<Local>)> = RefCell::new((0, None));
}

struct Shared {
    // number of task
    len: AtomicUsize,

    // queue of runnable task
    queue: Injector<Runnable>,

    // to notify that there is a task to be run
    wakers: Arc<RwLock<Vec<Waker>>>,

    // stealers for all threads
    stealers: ShardedLock<Vec<(usize, Stealer<Runnable>)>>,
}

#[inline(always)]
fn shared() -> &'static Shared {
    static ONCE: Once = Once::new();
    static mut SHARED: *const Shared = ptr::null();
    ONCE.call_once(|| {
        let shared = Box::into_raw(Box::new(Shared {
            len: AtomicUsize::new(0),
            queue: Injector::new(),
            wakers: Arc::new(RwLock::new(Vec::new())),
            stealers: ShardedLock::new(Vec::new()),
        }));
        unsafe { SHARED = shared }
    });
    unsafe { &*SHARED }
}

// push new task
pub fn push(task: impl Future<Output = ()> + Send + 'static) {
    // increment the len, and decrement it after the task done
    shared().len.fetch_add(1, Ordering::Relaxed);
    let task = async {
        defer! {
            // if this is last task
            if shared().len.fetch_sub(1, Ordering::Relaxed) == 1 {
                wake_all(&shared().wakers);
            }
        }
        task.await;
    };

    let schedule = move |r| {
        let pushed_to_local = LOCAL.with(|local| {
            if local.borrow().1.is_none() {
                return Err(r);
            }

            let local = local.borrow();
            let local = local.1.as_ref().unwrap();

            local.queue.push(r);

            Ok(())
        });

        if let Err(r) = pushed_to_local {
            shared().queue.push(r)
        }

        wake_all(&shared().wakers);
    };
    let (r, t) = unsafe { async_task::spawn_unchecked(task, schedule) };
    t.detach();
    r.schedule();
}

#[inline(always)]
fn wake_all(wakers: &RwLock<Vec<Waker>>) {
    if wakers
        .read()
        .expect("acquiring wakers read lock when wake_all")
        .is_empty()
    {
        return;
    }

    let mut wakers_lock = wakers
        .write()
        .expect("acquiring wakers write lock when wake_all");

    for w in wakers_lock.drain(..) {
        w.wake();
    }

    drop(wakers_lock);
}

struct Local {
    id: usize,
    counter: Cell<usize>,
    queue: Worker<Runnable>,
}

impl Local {
    fn new() -> Local {
        static ID: AtomicUsize = AtomicUsize::new(0);

        let id = ID.fetch_add(1, Ordering::Relaxed);
        let queue = Worker::new();
        let stealer = queue.stealer();

        // add this stealer to SHARED
        let mut stealers_lock = shared()
            .stealers
            .write()
            .expect("acquiring stealers write lock when Local::new");

        stealers_lock.push((id, stealer));

        drop(stealers_lock);

        Local {
            id,
            counter: Cell::new(0),
            queue,
        }
    }
}

impl Drop for Local {
    fn drop(&mut self) {
        // move all runnable to SHARED
        while let Some(r) = self.queue.pop() {
            shared().queue.push(r);
        }

        // remove this stealer from SHARED
        let mut stealers_lock = shared()
            .stealers
            .write()
            .expect("acquiring stealers write lock when Local::drop");

        let last = stealers_lock.len() - 1;
        let pos = stealers_lock.iter().position(|s| self.id == s.0).unwrap();
        if pos != last {
            stealers_lock.swap(pos, last);
        }
        stealers_lock.pop();

        drop(stealers_lock);
    }
}

pub struct Poller<'a> {
    _lifetime: PhantomData<&'a ()>,

    // !Send + !Sync
    _not_send: PhantomData<*mut ()>,
}

pub fn poller<'a>() -> Poller<'a> {
    LOCAL.with(|local| {
        let mut local = local.borrow_mut();

        // init local queue if this is the first poller
        if local.0 == 0 {
            local.1.replace(Local::new());
        }

        local.0 += 1;
    });

    Poller {
        _lifetime: PhantomData,
        _not_send: PhantomData,
    }
}

impl<'a> Drop for Poller<'a> {
    fn drop(&mut self) {
        LOCAL.with(|local| {
            let mut local = local.borrow_mut();

            local.0 -= 1;

            // remove local queue if this is the last poller
            if local.0 == 0 {
                local.1.take();
            }
        });
    }
}

impl<'a> Poller<'a> {
    // poll single task in the runnable queue
    #[inline(always)]
    pub fn poll_one(&self) -> bool {
        LOCAL.with(|local| {
            let local = local.borrow();
            let local = local.1.as_ref().unwrap();

            macro_rules! run_runnable {
                ($r:expr) => {
                    if $r.run() {
                        local.queue.flush();
                    }
                    local.counter.set(local.counter.get() + 1);
                    return true;
                };
            }

            let backoff = Backoff::new();
            loop {
                let mut retry = false;

                // check SHARED first after 64 run to ensure fair scheduling
                if local.counter.get() >= 64 {
                    local.counter.set(0);

                    match shared().queue.steal_batch_and_pop(&local.queue.normal) {
                        Steal::Empty => {}
                        Steal::Success(r) => {
                            run_runnable!(r);
                        }
                        Steal::Retry => retry = true,
                    }

                    if let Some(r) = local.queue.pop() {
                        run_runnable!(r);
                    }
                } else {
                    if let Some(r) = local.queue.pop() {
                        run_runnable!(r);
                    }

                    match shared().queue.steal_batch_and_pop(&local.queue.normal) {
                        Steal::Empty => {}
                        Steal::Success(r) => {
                            run_runnable!(r);
                        }
                        Steal::Retry => retry = true,
                    }
                }

                // try to steal from others
                let stealers_lock = shared()
                    .stealers
                    .read()
                    .expect("acquiring stealers read lock when Poller::poll_one");

                let (l, r) = stealers_lock.split_at(fastrand::usize(..stealers_lock.len()));
                for t in r.iter().chain(l) {
                    match t.1.steal_batch_and_pop(&local.queue) {
                        Steal::Empty => {}
                        Steal::Success(r) => {
                            run_runnable!(r);
                        }
                        Steal::Retry => retry = true,
                    }
                }

                drop(stealers_lock);

                if !retry {
                    return false;
                }

                backoff.snooze();
            }
        })
    }

    // wait until we have runnable task
    #[inline(always)]
    pub async fn wait(&self) -> bool {
        poll_fn(|cx| {
            macro_rules! check {
                () => {
                    if shared().len.load(Ordering::Relaxed) == 0 {
                        return Poll::Ready(false);
                    }

                    if !shared().queue.is_empty() {
                        return Poll::Ready(true);
                    }

                    let stealers_lock = shared()
                        .stealers
                        .read()
                        .expect("acquiring stealers read lock when Poller::wait");

                    let (l, r) = stealers_lock.split_at(fastrand::usize(..stealers_lock.len()));
                    for t in r.iter().chain(l) {
                        if !t.1.is_empty() {
                            return Poll::Ready(true);
                        }
                    }

                    drop(stealers_lock);
                };
            }

            check!();

            let waker = cx.waker();
            let mut need_to_store = true;

            let wakers_lock = shared()
                .wakers
                .read()
                .expect("acquiring wakers read lock on poll");

            for w in wakers_lock.iter() {
                if w.will_wake(waker) {
                    need_to_store = false;
                    break;
                }
            }

            drop(wakers_lock);

            if need_to_store {
                let mut wakers_lock = shared()
                    .wakers
                    .write()
                    .expect("acquiring wakers write lock on poll");

                // check again while holding write lock
                check!();

                wakers_lock.push(waker.clone());

                drop(wakers_lock);
            }

            Poll::Pending
        })
        .await
    }
}
