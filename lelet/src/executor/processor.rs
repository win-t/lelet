use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering};

use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use crossbeam_utils::Backoff;

#[cfg(feature = "tracing")]
use log::trace;

use lelet_utils::{SimpleLock, SimpleLockGuard};

use crate::utils::Sleeper;

use super::machine::Machine;
use super::task::TaskTag;
use super::Task;

/// Processor is the one who run the task
pub struct Processor {
    pub index: usize,

    /// for blocking detection
    last_seen: AtomicU64,

    sleeping: AtomicBool,

    /// current machine that holding the processor
    current_machine: AtomicPtr<Machine>,

    current_task: AtomicPtr<TaskTag>,

    injector: Injector<Task>,
    queue: Queue,

    sleeper: Sleeper,
}

impl Processor {
    pub fn new(index: usize) -> Processor {
        #[allow(clippy::let_and_return)]
        let processor = Processor {
            index,
            last_seen: AtomicU64::new(0),
            sleeping: AtomicBool::new(false),
            current_machine: AtomicPtr::new(ptr::null_mut()),
            current_task: AtomicPtr::new(ptr::null_mut()),
            injector: Injector::new(),
            queue: Queue::new(),
            sleeper: Sleeper::new(),
        };

        #[cfg(feature = "tracing")]
        trace!("{:?} is created", processor);

        processor
    }

    #[inline(always)]
    pub fn run_on(&self, machine: &Machine) {
        // steal this processor from current machine
        self.current_machine
            .store(machine as *const _ as *mut _, Ordering::Relaxed);

        let mut worker = match self.acquire_worker(machine) {
            Some(worker) => worker,
            None => return,
        };

        self.last_seen.store(u64::MAX, Ordering::Relaxed);

        #[cfg(feature = "tracing")]
        crate::thread_pool::THREAD_ID.with(|tid| {
            trace!("{:?} is now running on {:?} on {:?}", self, machine, tid);
        });

        // Number of runs in a row before the global queue is inspected.
        const MAX_RUNS: usize = 61;
        let mut run_counter = 0;

        let backoff = Backoff::new();
        loop {
            macro_rules! run_task {
                ($task:expr) => {
                    #[cfg(feature = "tracing")]
                    let task_info = format!("{:?}", $task.tag());

                    #[cfg(feature = "tracing")]
                    trace!("{} is running on {:?}", task_info, self);

                    self.current_task
                        .store($task.tag() as *const _ as *mut _, Ordering::Relaxed);

                    self.last_seen
                        .store(machine.system.now(), Ordering::Relaxed);

                    $task.tag().set_index_hint(self.index);

                    worker = match self.unlock_worker(machine, worker, || {
                        $task.run();
                    }) {
                        Some(worker) => worker,
                        None => {
                            #[cfg(feature = "tracing")]
                            trace!("{} is done blocking {:?}", task_info, machine);

                            return;
                        }
                    };

                    self.last_seen.store(u64::MAX, Ordering::Relaxed);

                    self.current_task.store(ptr::null_mut(), Ordering::Relaxed);

                    #[cfg(feature = "tracing")]
                    trace!("{} is done running on {:?}", task_info, self);

                    run_counter += 1;

                    continue;
                };
            }

            macro_rules! run_local_task {
                () => {
                    if let Some(task) = worker.pop() {
                        run_task!(task);
                    }
                };
            }

            macro_rules! run_global_task {
                () => {
                    run_counter = 0;
                    if let Some(task) = machine.system.pop(worker.get_ref(), self.index) {
                        run_task!(task);
                    }
                };
            }

            macro_rules! run_stolen_task {
                () => {
                    if let Some(task) = machine.system.steal(worker.get_ref(), self.index) {
                        run_task!(task);
                    }
                };
            }

            if run_counter >= MAX_RUNS {
                run_global_task!();
            }

            run_local_task!();

            // when local queue is empty:

            // 1. get from global queue
            run_global_task!();

            // 2. steal from others
            run_stolen_task!();

            // 3.a. no more task for now, just sleep
            worker = match self.unlock_worker(machine, worker, || {
                self.sleep(machine, &backoff);
            }) {
                Some(worker) => worker,
                None => {
                    self.wake_up();
                    return;
                }
            };

            // 3.b. after sleep, pop from global queue
            run_global_task!();
        }
    }

    /// will fail if machine no longer hold the processor (stolen)
    #[inline(always)]
    fn acquire_worker(&self, machine: &Machine) -> Option<WorkerLock> {
        let backoff = Backoff::new();
        loop {
            // fast check, without lock
            if !ptr::eq(self.current_machine.load(Ordering::Relaxed), machine) {
                return None;
            }

            if let Some(lock) = self.queue.try_lock_worker() {
                // check again after locking
                if !ptr::eq(self.current_machine.load(Ordering::Relaxed), machine) {
                    drop(lock);
                    return None;
                }

                return Some(lock);
            }

            backoff.snooze();
        }
    }

    /// unlock the worker lock, run f, and then reacquire worker lock again,
    #[inline(always)]
    fn unlock_worker(&self, m: &Machine, w: WorkerLock, f: impl FnOnce()) -> Option<WorkerLock> {
        drop(w);
        f();
        self.acquire_worker(m)
    }

    #[inline(always)]
    pub fn get_last_seen(&self) -> u64 {
        self.last_seen.load(Ordering::Relaxed)
    }

    #[inline(always)]
    pub fn is_sleeping(&self) -> bool {
        self.sleeping.load(Ordering::Relaxed)
    }

    #[inline(always)]
    fn sleep(&self, machine: &Machine, backoff: &Backoff) {
        if backoff.is_completed() {
            self.sleeping.store(true, Ordering::Relaxed);
            self.sleeper.sleep();
            self.sleeping.store(false, Ordering::Relaxed);

            // wake the sysmon in case the sysmon is also sleeping
            machine.system.sysmon_wake_up();

            backoff.reset();
        } else {
            backoff.snooze();
        }
    }

    #[inline(always)]
    pub fn wake_up(&self) {
        self.sleeper.wake_up();
    }

    #[inline(always)]
    pub fn direct_push(&self, machine: &Machine, task: Task) -> Result<(), Task> {
        match self.acquire_worker(machine) {
            Some(mut worker) => {
                // if current task is pushed while running, do not prioritize it
                let prioritized = !ptr::eq(
                    self.current_task.load(Ordering::Relaxed),
                    task.tag() as *const _ as *mut _,
                );

                #[cfg(feature = "tracing")]
                {
                    if prioritized {
                        trace!(
                            "{:?} is directly pushed to {:?}'s local queue (prioritized)",
                            task.tag(),
                            self
                        );
                    } else {
                        trace!(
                            "{:?} is directly pushed to {:?}'s local queue",
                            task.tag(),
                            self
                        );
                    }
                }

                worker.push(prioritized, task);

                Ok(())
            }
            None => Err(task),
        }
    }

    #[inline(always)]
    pub fn steal(&self, worker: &Worker<Task>) -> Option<Task> {
        let backoff = Backoff::new();
        loop {
            match self.queue.stealer.steal_batch_and_pop(worker) {
                Steal::Success(task) => return Some(task),
                Steal::Empty => return None,
                Steal::Retry => backoff.spin(),
            }
        }
    }

    #[inline(always)]
    pub fn push(&self, task: Task) {
        #[cfg(feature = "tracing")]
        trace!("{:?} is pushed to {:?}'s global queue", task.tag(), self);

        self.injector.push(task);
    }

    #[inline(always)]
    pub fn pop(&self, worker: &Worker<Task>) -> Option<Task> {
        let backoff = Backoff::new();
        loop {
            match self.injector.steal_batch_and_pop(worker) {
                Steal::Success(task) => return Some(task),
                Steal::Empty => return None,
                Steal::Retry => backoff.spin(),
            }
        }
    }
}

#[cfg(feature = "tracing")]
impl std::fmt::Debug for Processor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(&format!("Processor({})", self.index))
    }
}

struct WorkerWrapper {
    prioritized: Option<Task>,
    wrapped: Worker<Task>,
}

impl WorkerWrapper {
    fn new(worker: Worker<Task>) -> WorkerWrapper {
        WorkerWrapper {
            prioritized: None,
            wrapped: worker,
        }
    }

    #[inline(always)]
    fn pop(&mut self) -> Option<Task> {
        self.prioritized.take().or_else(|| self.wrapped.pop())
    }

    #[inline(always)]
    fn push(&mut self, prioritized: bool, task: Task) {
        if prioritized {
            if let Some(task) = self.prioritized.replace(task) {
                self.wrapped.push(task)
            }
        } else {
            self.wrapped.push(task)
        }
    }

    #[inline(always)]
    fn get_ref(&mut self) -> &Worker<Task> {
        &self.wrapped
    }
}

struct Queue {
    worker: SimpleLock<WorkerWrapper>,
    stealer: Stealer<Task>,
}

type WorkerLock<'a> = SimpleLockGuard<'a, WorkerWrapper>;

impl Queue {
    fn new() -> Queue {
        let worker = Worker::new_fifo();
        let stealer = worker.stealer();
        Queue {
            worker: SimpleLock::new(WorkerWrapper::new(worker)),
            stealer,
        }
    }

    #[inline(always)]
    fn try_lock_worker(&self) -> Option<WorkerLock> {
        self.worker.try_lock()
    }
}
