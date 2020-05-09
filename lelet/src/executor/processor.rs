use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering};
use std::thread;

use crossbeam_deque::{Steal, Stealer, Worker};
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
            queue: Queue::new(),
            sleeper: Sleeper::new(),
        };

        #[cfg(feature = "tracing")]
        trace!("{:?} is created", processor);

        processor
    }

    pub fn run_on(&self, machine: &Machine) {
        // steal this processor from current machine
        self.current_machine
            .store(machine as *const _ as *mut _, Ordering::Relaxed);

        let mut worker = match self.check_machine_and_acquire_worker(machine) {
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

                    worker = match self.unlock_worker_and_then(machine, worker, || {
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
                    if let Some(task) = machine.system.pop(worker.get_ref()) {
                        run_task!(task);
                    }
                };
            }

            macro_rules! run_stolen_task {
                () => {
                    if let Some(task) = machine.system.steal(worker.get_ref()) {
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
            worker = match self.unlock_worker_and_then(machine, worker, || {
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
    fn check_machine_and_acquire_worker(
        &self,
        machine: &Machine,
    ) -> Option<SimpleLockGuard<'_, WorkerWrapper>> {
        const SPIN_THRESHOLD: usize = 10000;
        let mut counter = 0;

        let backoff = Backoff::new();
        loop {
            // fast check, without lock
            if !ptr::eq(self.current_machine.load(Ordering::Relaxed), machine) {
                return None;
            }

            if let Some(lock) = self.queue.worker.try_lock() {
                // check again after locking
                if !ptr::eq(self.current_machine.load(Ordering::Relaxed), machine) {
                    drop(lock);
                    return None;
                }

                return Some(lock);
            }

            if backoff.is_completed() {
                thread::yield_now();
                counter += 1;
                if counter > SPIN_THRESHOLD {
                    panic!("stuck in spin loop, please fill an issue on github.com/win-t/lelet");
                }
            } else {
                backoff.snooze();
            }
        }
    }

    /// unlock the worker lock, run f, and then reacquire worker lock again,
    #[inline(always)]
    fn unlock_worker_and_then(
        &self,
        machine: &Machine,
        lock: SimpleLockGuard<'_, WorkerWrapper>,
        f: impl FnOnce(),
    ) -> Option<SimpleLockGuard<'_, WorkerWrapper>> {
        drop(lock);

        f();

        self.check_machine_and_acquire_worker(machine)
    }

    #[inline(always)]
    pub fn get_last_seen(&self) -> u64 {
        self.last_seen.load(Ordering::Relaxed)
    }

    #[inline(always)]
    pub fn is_sleeping(&self) -> bool {
        self.sleeping.load(Ordering::Relaxed)
    }

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

    pub fn wake_up(&self) -> bool {
        self.sleeper.wake_up()
    }

    pub fn check_machine_and_push(&self, machine: &Machine, task: Task) -> Result<(), Task> {
        match self.check_machine_and_acquire_worker(machine) {
            Some(mut worker) => {
                #[cfg(feature = "tracing")]
                trace!(
                    "{:?} directly pushed to {:?}'s local queue",
                    task.tag(),
                    self
                );

                worker.push(self.current_task.load(Ordering::Relaxed), task);
                machine.system.processors_wake_up();

                Ok(())
            }
            None => Err(task),
        }
    }

    pub fn steal(&self, worker: &Worker<Task>) -> Option<Task> {
        std::iter::repeat_with(|| self.queue.stealer.steal_batch_and_pop(worker))
            .find(|s| !s.is_retry())
            .map(|s| match s {
                Steal::Success(task) => Some(task),
                Steal::Empty => None,
                Steal::Retry => unreachable!(), // already filtered
            })
            .flatten()
    }
}

#[cfg(feature = "tracing")]
impl std::fmt::Debug for Processor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

    fn pop(&mut self) -> Option<Task> {
        self.prioritized.take().or_else(|| self.wrapped.pop())
    }

    #[allow(clippy::collapsible_if)]
    fn push(&mut self, current_task: *mut TaskTag, task: Task) {
        if ptr::eq(current_task, task.tag() as *const _ as *mut _) {
            // current task is rescheduled when running, do not prioritize it
            self.wrapped.push(task)
        } else {
            #[cfg(feature = "tracing")]
            trace!("{:?} is prioritized", task.tag());

            if let Some(task) = self.prioritized.replace(task) {
                self.wrapped.push(task)
            }
        }
    }

    fn get_ref(&mut self) -> &Worker<Task> {
        &self.wrapped
    }
}

struct Queue {
    worker: SimpleLock<WorkerWrapper>,
    stealer: Stealer<Task>,
}

impl Queue {
    fn new() -> Queue {
        let worker = Worker::new_fifo();
        let stealer = worker.stealer();
        Queue {
            worker: SimpleLock::new(WorkerWrapper::new(worker)),
            stealer,
        }
    }
}
