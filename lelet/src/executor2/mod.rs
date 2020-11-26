#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(dead_code)]

use std::{
    future::Future,
    pin::Pin,
    ptr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Once,
    },
    task::{Context, Poll},
    thread,
    time::Duration,
};

use crossbeam_deque::{Injector, Steal, Stealer, Worker};

use async_task::{spawn as async_spawn, Runnable, Task};

use lelet_utils::{ready, SimpleLock};

use crate::thread_cache;

mod util;

macro_rules! try_lock_or_return {
    ($e:expr $(,)?) => {
        match $e.try_lock() {
            Some(l) => l,
            None => return,
        }
    };
}

const BLOCKING_TRHESHOLD: Duration = Duration::from_millis(100);

struct Job(Runnable);

struct System {
    lock: SimpleLock<()>,
    tick: AtomicUsize,

    global: Injector<Job>,

    executors: Vec<ExecutorData>,
}

struct ExecutorData(ExecutorData0, SimpleLock<ExecutorData1>);

struct ExecutorData0 {
    last_tick: AtomicUsize,
    to_steal: Vec<Stealer<Job>>,
}

struct ExecutorData1 {
    slot: Option<Job>,
    queue: Worker<Job>,
    counter: usize,
}

impl System {
    fn new() -> Box<System> {
        let num = util::max_procs();

        let mut system = Box::new(System {
            lock: SimpleLock::new(()),
            tick: AtomicUsize::new(0),

            global: Injector::new(),

            executors: (0..num)
                .map(|_| {
                    ExecutorData(
                        ExecutorData0 {
                            last_tick: AtomicUsize::new(0),
                            to_steal: Vec::new(),
                        },
                        SimpleLock::new(ExecutorData1 {
                            slot: None,
                            queue: Worker::new_fifo(),
                            counter: 0,
                        }),
                    )
                })
                .collect(),
        });

        (3..)
            .step_by(2)
            .filter(|&i| util::coprime(i, num))
            .take(num)
            .enumerate()
            .for_each(|(i, c)| {
                system.executors[i].0.to_steal = (0..)
                    .map(move |j| ((c * j) + i) % num)
                    .filter(move |&s| s != i)
                    .take(num - 1)
                    .map(|e| system.executors[e].1.try_lock().unwrap().queue.stealer())
                    .collect();
            });

        system
    }

    fn monitor_main_loop(&'static self) {
        let _guard = try_lock_or_return!(self.lock);

        self.executors
            .iter()
            .enumerate()
            .for_each(|(i, _)| self.spawn_executor(i));

        loop {
            let check_tick = self.tick.fetch_add(1, Ordering::Relaxed) + 1;

            thread::sleep(BLOCKING_TRHESHOLD);

            self.executors
                .iter()
                .enumerate()
                .filter(|(_, e)| e.0.last_tick.load(Ordering::Relaxed) < check_tick)
                .for_each(|(i, _)| self.spawn_executor(i));
        }
    }

    fn spawn_executor(&'static self, executor_index: usize) {
        // thread_cache::spawn(Box::new(move || {
        //     self.executor_main_loop(executor_index);
        // }));
    }

    fn executor_main_loop(&self, executor_index: usize) {
        let e = &self.executors[executor_index];
        let mut d = try_lock_or_return!(e.1);

        loop {
            e.0.last_tick
                .store(self.tick.load(Ordering::Relaxed), Ordering::Relaxed);

            let _t = self.get_job(&e.0, &mut d);
            // TODO
        }
    }

    fn get_job(&self, e: &ExecutorData0, d: &mut ExecutorData1) -> Option<Job> {
        d.counter += 1;
        loop {
            let mut retry = false;
            let mut global_only = false;

            if d.counter <= 61 {
                if let Some(t) = d.slot.take() {
                    return Some(t);
                }

                if let Some(t) = d.queue.pop() {
                    return Some(t);
                }
            } else {
                d.counter = 0;
                global_only = true;
                retry = true;
            }

            match self.global.steal_batch_and_pop(&d.queue) {
                Steal::Empty => {}
                Steal::Success(t) => return Some(t),
                Steal::Retry => retry = true,
            }

            if !global_only {
                for o in &e.to_steal {
                    match o.steal_batch_and_pop(&d.queue) {
                        Steal::Empty => {}
                        Steal::Success(t) => return Some(t),
                        Steal::Retry => retry = true,
                    }
                }
            }

            if !retry {
                return None;
            }
        }
    }

    fn push_runnable(&self, j: Job) {
        self.global.push(j);
        todo!();
    }
}

impl System {
    fn get() -> &'static System {
        static ONCE: Once = Once::new();
        static mut SYSTEM: *const System = ptr::null();
        ONCE.call_once(|| unsafe {
            SYSTEM = Box::into_raw(System::new());
            thread_cache::spawn(Box::new(|| System::get().monitor_main_loop()))
        });
        unsafe { &*SYSTEM }
    }
}

pub async fn spawn<F, R>(f: F) -> R
where
    F: Future<Output = R> + Send + 'static,
    R: Send + 'static,
{
    let system = System::get();

    let (r, t) = async_spawn(f, move |r| system.push_runnable(Job(r)));
    r.schedule();

    t.await
}
