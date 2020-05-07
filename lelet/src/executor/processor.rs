use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering};
use std::thread;

use crossbeam_deque::{Steal, Stealer, Worker};
use crossbeam_utils::Backoff;

#[cfg(feature = "tracing")]
use log::trace;

use lelet_utils::{Spinlock, SpinlockGuard};

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

        let mut queue = match self.check_and_acquire_queue(machine) {
            Ok(queue) => queue,
            Err(()) => return,
        };

        self.last_seen.store(u64::MAX, Ordering::Relaxed);

        macro_rules! run_without_lock {
            ($f:expr) => {
                queue = match self.unlock_queue_and_then(machine, queue, $f) {
                    Ok(queue) => queue,
                    Err(()) => return,
                };
            };
        }

        #[cfg(feature = "tracing")]
        crate::thread_pool::THREAD_ID.with(|tid| {
            trace!("{:?} is now running on {:?} on {:?}", self, machine, tid);
        });

        // Number of runs in a row before the global queue is inspected.
        const MAX_RUNS: usize = 61;
        let mut run_counter = 0;

        loop {
            macro_rules! run_task {
                ($task:expr) => {
                    #[cfg(feature = "tracing")]
                    trace!("{:?} is going to run on {:?}", $task.tag(), self);

                    self.current_task
                        .store($task.tag() as *const _ as *mut _, Ordering::Relaxed);

                    self.last_seen
                        .store(machine.system.now(), Ordering::Relaxed);

                    run_without_lock!(|| {
                        $task.run();
                    });

                    self.last_seen.store(u64::MAX, Ordering::Relaxed);

                    self.current_task.store(ptr::null_mut(), Ordering::Relaxed);

                    run_counter += 1;

                    continue;
                };
            }

            macro_rules! run_local_task {
                () => {
                    if let Some(task) = queue.pop() {
                        run_task!(task);
                    }
                };
            }

            macro_rules! run_global_task {
                () => {
                    run_counter = 0;
                    if let Some(task) = machine.system.pop(queue.as_worker_ref()) {
                        run_task!(task);
                    }
                };
            }

            macro_rules! run_stolen_task {
                () => {
                    if let Some(task) = machine.system.steal(queue.as_worker_ref()) {
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
            self.sleep(machine);

            // 3.b. after sleep, pop from global queue
            run_global_task!();
        }
    }

    /// will fail if machine no longer hold the processor (stolen)
    #[inline(always)]
    fn check_and_acquire_queue(&self, machine: &Machine) -> Result<QueueLock, ()> {
        let backoff = Backoff::new();
        loop {
            // fast check, without lock
            if !ptr::eq(self.current_machine.load(Ordering::Relaxed), machine) {
                return Err(());
            }

            if let Some(lock) = self.queue.try_lock() {
                // check again after locking
                if !ptr::eq(self.current_machine.load(Ordering::Relaxed), machine) {
                    drop(lock);
                    return Err(());
                }

                return Ok(lock);
            }

            if backoff.is_completed() {
                thread::yield_now()
            } else {
                backoff.snooze()
            }
        }
    }

    /// unlock the queue lock, run f, and then reacquire queue lock again,
    #[inline(always)]
    fn unlock_queue_and_then(
        &self,
        machine: &Machine,
        lock: QueueLock,
        f: impl FnOnce(),
    ) -> Result<QueueLock, ()> {
        drop(lock);
        f();
        self.check_and_acquire_queue(machine)
    }

    #[inline(always)]
    pub fn get_last_seen(&self) -> u64 {
        self.last_seen.load(Ordering::Relaxed)
    }

    #[inline(always)]
    pub fn is_sleeping(&self) -> bool {
        self.sleeping.load(Ordering::Relaxed)
    }

    fn sleep(&self, machine: &Machine) {
        self.sleeping.store(true, Ordering::Relaxed);
        self.sleeper.sleep();
        self.sleeping.store(false, Ordering::Relaxed);

        // wake the sysmon in case the sysmon is also sleeping
        machine.system.sysmon_wake_up();
    }

    pub fn wake_up(&self) -> bool {
        self.sleeper.wake_up()
    }

    pub fn check_and_push(&self, machine: &Machine, task: Task) -> Result<(), Task> {
        match self.check_and_acquire_queue(machine) {
            Ok(mut queue) => {
                #[cfg(feature = "tracing")]
                trace!("{:?} directly pushed to {:?} local queue", task.tag(), self);

                queue.push(self.current_task.load(Ordering::Relaxed), task);
                machine.system.processors_wake_up();

                Ok(())
            }
            Err(()) => Err(task),
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

struct WorkerWrapper(Option<Task>, Worker<Task>);

impl WorkerWrapper {
    fn new(worker: Worker<Task>) -> WorkerWrapper {
        WorkerWrapper(None, worker)
    }

    fn pop(&mut self) -> Option<Task> {
        self.0.take().or_else(|| self.1.pop())
    }

    fn push(&mut self, current_task: *mut TaskTag, task: Task) {
        if ptr::eq(current_task, task.tag() as *const _ as *mut _) {
            self.1.push(task)
        } else {
            #[cfg(feature = "tracing")]
            trace!("{:?} is prioritized", task.tag());

            if let Some(task) = self.0.replace(task) {
                self.1.push(task)
            }
        }
    }

    fn as_worker_ref(&mut self) -> &Worker<Task> {
        &self.1
    }
}

struct Queue {
    worker: Spinlock<WorkerWrapper>,
    stealer: Stealer<Task>,
}

type QueueLock<'a> = SpinlockGuard<'a, WorkerWrapper>;

impl Queue {
    fn new() -> Queue {
        let worker = Worker::new_fifo();
        let stealer = worker.stealer();
        Queue {
            worker: Spinlock::new(WorkerWrapper::new(worker)),
            stealer,
        }
    }

    fn try_lock(&self) -> Option<QueueLock> {
        self.worker.try_lock()
    }
}
