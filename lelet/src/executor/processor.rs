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

    pub fn run(&self) {
        let machine = Machine::get();
        if machine.is_none() {
            return;
        }

        let machine = machine.unwrap();
        assert_eq!(self.index, machine.processor_index);

        let system = machine.system;
        assert!(std::ptr::eq(self, &system.processors[self.index]));

        let mut queue = self.queue.lock();
        self.current_machine_id.store(machine.id, Ordering::Relaxed);

        #[cfg(feature = "tracing")]
        crate::thread_pool::THREAD_ID.with(|tid| {
            trace!("{:?} is now running on {:?} on {:?}", self, machine, tid);
        });

        // Number of runs in a row before the global queue is inspected.
        const MAX_RUNS: usize = 16;
        let mut run_counter = 0;

        let backoff = Backoff::new();

        loop {
            macro_rules! run_task {
                ($task:ident) => {
                    let mut woken = false;

                    #[cfg(feature = "tracing")]
                    trace!("{:?} is going to run on {:?}", $task.tag(), self);

                    queue = match self.unlock_and_then(queue, || woken = $task.run()) {
                        Ok(queue) => queue,
                        Err(()) => return,
                    };

                    run_counter += 1;
                    if woken {
                        queue.flush();
                    }

                    continue;
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

            // mark this processor on every iteration
            self.last_seen.store(system.now(), Ordering::Relaxed);

            if run_counter >= MAX_RUNS {
                run_global_task!();
            }

            // run all task in the worker
            if let Some(task) = queue.pop() {
                run_task!(task);
            }

            // at this point, the worker is empty

            // 1. pop from global queue
            run_global_task!();

            // 2. steal from others
            if let Some(task) = system.steal(queue.as_worker_ref()) {
                run_task!(task);
            }

            // 3.a. no more task for now, just sleep
            if backoff.is_completed() {
                self.last_seen.store(u64::MAX, Ordering::Relaxed);
                self.sleeper.sleep();
                self.last_seen.store(system.now(), Ordering::Relaxed);

                // wake the sysmon in case the sysmon is also sleeping
                system.sysmon_wake_up();

                backoff.reset();
            } else {
                backoff.snooze();
            }

            // 3.b. after sleep, pop from global queue
            run_global_task!();
        }
    }

    #[inline(always)]
    fn unlock_and_then(&self, lock: QueueLock, f: impl FnOnce()) -> Result<QueueLock, ()> {
        let machine_before_unlock = self.current_machine_id.load(Ordering::Relaxed);
        drop(lock);

        f();

        let backoff = Backoff::new();
        loop {
            if machine_before_unlock != self.current_machine_id.load(Ordering::Relaxed) {
                return Err(());
            }

            if let Some(lock) = self.queue.try_lock() {
                if machine_before_unlock == self.current_machine_id.load(Ordering::Relaxed) {
                    return Ok(lock);
                } else {
                    drop(lock);
                    return Err(());
                }
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

    pub fn wake_up(&self) -> bool {
        self.sleeper.wake_up()
    }

    /// will fail if other machine hold the processor
    pub fn push(&self, task: Task) -> Result<(), Task> {
        let backoff = Backoff::new();
        loop {
            if let Some(mut queue) = self.queue.try_lock() {
                #[cfg(feature = "tracing")]
                trace!("{:?} directly pushed to {:?} local queue", task.tag(), self);

                queue.push(task);

                return Ok(());
            }

            if backoff.is_completed() {
                return Err(task);
            } else {
                backoff.snooze()
            }
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
        QueueLock(self.worker.lock().unwrap())
    }

    fn try_lock(&self) -> Option<QueueLock> {
        match self.worker.try_lock() {
            Ok(lock) => Some(QueueLock(lock)),
            Err(TryLockError::WouldBlock) => None,
            Err(TryLockError::Poisoned(err)) => Err(err).unwrap(),
        }
    }
}

struct QueueLock<'a>(MutexGuard<'a, WorkerWrapper>);

impl<'a> std::ops::Deref for QueueLock<'a> {
    type Target = WorkerWrapper;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> std::ops::DerefMut for QueueLock<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
