use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, TryLockError};

use crossbeam_deque::{Steal, Stealer, Worker};
use crossbeam_utils::Backoff;

#[cfg(feature = "tracing")]
use log::trace;

use crate::utils::Sleeper;

use super::machine::Machine;
use super::Task;

/// Processor is the one who run the task
pub struct Processor {
    pub index: usize,

    /// for blocking detection
    /// u64::MAX mean the processor is sleeping
    last_seen: AtomicU64,

    /// current machine that holding the processor
    /// usize::MAX mean the processor is not running on any machine
    current_machine_id: AtomicUsize,

    queue: Queue,

    sleeper: Sleeper,
}

impl Processor {
    pub fn new(index: usize) -> Processor {
        #[allow(clippy::let_and_return)]
        let processor = Processor {
            index,
            last_seen: AtomicU64::new(0),
            current_machine_id: AtomicUsize::new(usize::MAX),
            queue: Queue::new(),
            sleeper: Sleeper::new(),
        };

        #[cfg(feature = "tracing")]
        trace!("{:?} is created", processor);

        processor
    }

    pub fn run_on(&self, machine: &Machine) {
        let system = machine.system;

        assert_eq!(self.index, machine.processor_index);
        assert!(std::ptr::eq(self, &system.processors[self.index]));

        let mut queue = {
            // steal this processor from current machine
            self.current_machine_id.store(machine.id, Ordering::Relaxed);

            let lock = self.queue.lock();

            // check again after locking
            if self.current_machine_id.load(Ordering::Relaxed) != machine.id {
                drop(lock);
                return;
            }

            lock
        };

        macro_rules! without_lock {
            ($fn:expr) => {
                queue = match self.unlock_and_then(&machine, queue, $fn) {
                    Ok(queue) => queue,

                    // fail mean processor is no longer on machine (stolen),
                    // exit now
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
                    let mut woken = false;

                    #[cfg(feature = "tracing")]
                    trace!("{:?} is going to run on {:?}", $task.tag(), self);

                    without_lock!(|| woken = $task.run());

                    run_counter += 1;

                    if woken {
                        queue.flush();
                    }

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
                    if let Some(task) = system.pop(queue.as_worker_ref()) {
                        run_task!(task);
                    }
                };
            }

            macro_rules! run_stolen_task {
                () => {
                    if let Some(task) = system.steal(queue.as_worker_ref()) {
                        run_task!(task);
                    }
                };
            }

            // mark this processor on every iteration
            self.last_seen.store(system.now(), Ordering::Relaxed);

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
            {
                self.last_seen.store(u64::MAX, Ordering::Relaxed);
                self.sleeper.sleep();
                self.last_seen.store(system.now(), Ordering::Relaxed);

                // wake the sysmon in case the sysmon is also sleeping
                system.sysmon_wake_up();
            }

            // 3.b. after sleep, pop from global queue
            run_global_task!();
        }
    }

    /// unlock the queue lock, run f, and then reacquire queue lock,
    /// will fail if machine no longer hold the processor (stolen)
    #[inline(always)]
    fn unlock_and_then(
        &self,
        machine: &Machine,
        lock: QueueLock,
        f: impl FnOnce(),
    ) -> Result<QueueLock, ()> {
        drop(lock);

        f();

        let backoff = Backoff::new();
        loop {
            // fast check, without lock
            if self.current_machine_id.load(Ordering::Relaxed) != machine.id {
                return Err(());
            }

            if let Some(lock) = self.queue.try_lock() {
                // check again after locking
                if self.current_machine_id.load(Ordering::Relaxed) != machine.id {
                    drop(lock);
                    return Err(());
                }

                return Ok(lock);
            }

            if backoff.is_completed() {
                std::thread::sleep(std::time::Duration::from_millis(1));
            } else {
                backoff.snooze();
            }
        }
    }

    /// will return u64::MAX when processor is sleeping (always seen in the future)
    #[inline(always)]
    pub fn get_last_seen(&self) -> u64 {
        self.last_seen.load(Ordering::Relaxed)
    }

    #[inline(always)]
    pub fn is_sleeping(&self) -> bool {
        self.get_last_seen() == u64::MAX
    }

    pub fn wake_up(&self) -> bool {
        self.sleeper.wake_up()
    }

    pub fn check_and_push(&self, machine: &Machine, task: Task) -> Result<(), Task> {
        // fast check, without lock
        if self.current_machine_id.load(Ordering::Relaxed) != machine.id {
            return Err(task);
        }

        if let Some(mut queue) = self.queue.try_lock() {
            // check again after locking
            if self.current_machine_id.load(Ordering::Relaxed) != machine.id {
                drop(queue);
                return Err(task);
            }

            #[cfg(feature = "tracing")]
            trace!("{:?} directly pushed to {:?} local queue", task.tag(), self);

            queue.push(task);
            machine.system.processors_wake_up();

            Ok(())
        } else {
            Err(task)
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

    fn flush(&mut self) {
        if let Some(t) = self.0.take() {
            self.1.push(t)
        }
    }

    fn pop(&mut self) -> Option<Task> {
        self.0.take().or_else(|| self.1.pop())
    }

    fn push(&mut self, t: Task) {
        if let Some(t) = self.0.replace(t) {
            self.1.push(t)
        }
    }

    fn as_worker_ref(&mut self) -> &Worker<Task> {
        self.flush();
        &self.1
    }
}

struct Queue {
    worker: Mutex<WorkerWrapper>,
    stealer: Stealer<Task>,
}

type QueueLock<'a> = MutexGuard<'a, WorkerWrapper>;

impl Queue {
    fn new() -> Queue {
        let worker = Worker::new_fifo();
        let stealer = worker.stealer();
        Queue {
            worker: Mutex::new(WorkerWrapper::new(worker)),
            stealer,
        }
    }

    fn lock(&self) -> QueueLock {
        let backoff = Backoff::new();
        loop {
            if let Some(lock) = self.try_lock() {
                return lock;
            }

            if backoff.is_completed() {
                return self.worker.lock().unwrap();
            } else {
                backoff.snooze();
            }
        }
    }

    fn try_lock(&self) -> Option<QueueLock> {
        match self.worker.try_lock() {
            Ok(lock) => Some(lock),
            Err(TryLockError::WouldBlock) => None,
            Err(TryLockError::Poisoned(err)) => Err(err).unwrap(),
        }
    }
}
