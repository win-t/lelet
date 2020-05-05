use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};

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

        self.last_seen.store(system.now(), Ordering::Relaxed);

        #[cfg(feature = "tracing")]
        crate::thread_pool::THREAD_ID.with(|tid| {
            trace!("{:?} is now running on {:?} on {:?}", self, machine, tid);
        });

        // Number of runs in a row before the global queue is inspected.
        const MAX_RUNS: usize = 16;
        let mut run_counter = 0;

        let sleep_backoff = Backoff::new();

        loop {
            macro_rules! run_task {
                ($task:ident) => {
                    queue = match self.unlock_and_then(queue, || {
                        #[cfg(feature = "tracing")]
                        trace!("{:?} is going to run on {:?}", $task.tag(), self);

                        $task.run();

                        run_counter += 1;
                    }) {
                        Ok(queue) => queue,
                        Err(()) => return,
                    };
                    continue;
                };
            }

            macro_rules! run_global_task {
                () => {
                    run_counter = 0;
                    if let Some(task) = system.pop(&queue) {
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
            if let Some(task) = system.steal(&queue) {
                run_task!(task);
            }

            // 3.a. no more task for now, just sleep
            if sleep_backoff.is_completed() {
                self.last_seen.store(u64::MAX, Ordering::Relaxed);
                self.sleeper.sleep();
                self.last_seen.store(system.now(), Ordering::Relaxed);

                // wake the sysmon in case the sysmon is also sleeping
                system.sysmon_wake_up();

                sleep_backoff.reset();
            } else {
                sleep_backoff.snooze();
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

        if machine_before_unlock == self.current_machine_id.load(Ordering::Relaxed) {
            let lock = Ok(self.queue.lock());
            if machine_before_unlock == self.current_machine_id.load(Ordering::Relaxed) {
                lock
            } else {
                drop(lock);
                Err(())
            }
        } else {
            Err(())
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
        match self.queue.worker.try_lock() {
            Ok(worker) => {
                #[cfg(feature = "tracing")]
                trace!("{:?} directly pushed to {:?} local queue", task.tag(), self);

                worker.push(task);

                Ok(())
            }
            Err(_) => Err(task),
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

struct Queue {
    worker: Mutex<Worker<Task>>,
    stealer: Stealer<Task>,
}

impl Queue {
    fn new() -> Queue {
        let worker = Worker::new_fifo();
        let stealer = worker.stealer();
        Queue {
            worker: Mutex::new(worker),
            stealer,
        }
    }

    fn lock(&self) -> QueueLock {
        QueueLock(self.worker.lock().unwrap())
    }
}

impl std::ops::Deref for Queue {
    type Target = Stealer<Task>;
    fn deref(&self) -> &Self::Target {
        &self.stealer
    }
}

struct QueueLock<'a>(MutexGuard<'a, Worker<Task>>);

impl<'a> std::ops::Deref for QueueLock<'a> {
    type Target = Worker<Task>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
