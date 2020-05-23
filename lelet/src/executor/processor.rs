use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering};

use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use crossbeam_utils::sync::{Parker, Unparker};
use crossbeam_utils::Backoff;

#[cfg(feature = "tracing")]
use log::trace;

use lelet_utils::{SimpleLock, SimpleLockGuard};

use super::machine::Machine;
use super::Task;

/// Processor is the one who run the task
pub struct Processor {
    pub index: usize,

    last_seen: AtomicU64,

    current_machine: AtomicPtr<Machine>,

    global: Injector<Task>,
    local: SimpleLock<Queue>,
    stealers: [Stealer<Task>; 2],

    parker: SimpleLock<Parker>,
    unparker: Unparker,
    sleeping: AtomicBool,
}

impl Processor {
    pub fn new(index: usize) -> Processor {
        let local = Queue::new();
        let stealers = [local.worker.stealer(), local.slot.stealer()];

        let parker = Parker::new();
        let unparker = parker.unparker().clone();

        #[allow(clippy::let_and_return)]
        let processor = Processor {
            index,

            last_seen: AtomicU64::new(0),

            current_machine: AtomicPtr::new(ptr::null_mut()),

            global: Injector::new(),
            local: SimpleLock::new(local),
            stealers,

            parker: SimpleLock::new(parker),
            unparker,
            sleeping: AtomicBool::new(false),
        };

        #[cfg(feature = "tracing")]
        trace!("{:?} is created", processor);

        processor
    }

    #[inline(always)]
    pub fn run_on(&self, machine: &Machine) {
        macro_rules! check {
            ($qlock:expr) => {
                match $qlock {
                    Some(qlock) => qlock,
                    None => return,
                }
            };
        }

        // just to make sure system and processor have consistent index
        assert!(ptr::eq(self, &machine.system.processors[self.index]));

        // steal this processor from old machine
        self.current_machine
            .store(machine as *const _ as *mut _, Ordering::Relaxed);

        // in case old machine it is sleeping
        self.wake_up();

        let mut qlock = check!(self.try_acquire_qlock(machine));

        // reset
        self.last_seen.store(u64::MAX, Ordering::Relaxed);
        self.sleeping.store(false, Ordering::Relaxed);

        #[cfg(feature = "tracing")]
        trace!("{:?} is now running on {:?} ", self, machine);

        loop {
            qlock.flush_slot();
            if let Some(task) = machine.system.pop(&qlock.worker, self) {
                qlock = check!(self.run_task(machine, qlock, task));
            }

            for _ in 0..37 {
                macro_rules! run_task {
                    ($task:expr) => {
                        qlock = check!(self.run_task(machine, qlock, $task));
                        continue;
                    };
                }

                if let Some(task) = qlock.pop() {
                    run_task!(task);
                }

                // when local queue is empty:

                // 1. get from global queue
                if let Some(task) = machine.system.pop(&qlock.worker, self) {
                    run_task!(task);
                }

                // 2. steal from others
                if let Some(task) = machine.system.steal(&qlock.worker, self) {
                    run_task!(task);
                }

                // 3. no more task for now, just sleep
                #[cfg(feature = "tracing")]
                trace!("{:?} entering sleep", self);
                self.sleeping.store(true, Ordering::Relaxed);
                qlock = check!(self.without_qlock(machine, qlock, || {
                    if let Some(parker) = self.parker.try_lock() {
                        parker.park();
                    }
                }));
                self.sleeping.store(false, Ordering::Relaxed);
                #[cfg(feature = "tracing")]
                trace!("{:?} exiting sleep", self);
            }
        }
    }

    #[inline(always)]
    fn run_task<'a>(
        &'a self,
        machine: &Machine,
        mut qlock: SimpleLockGuard<'a, Queue>,
        task: Task,
    ) -> Option<SimpleLockGuard<'a, Queue>> {
        #[cfg(feature = "tracing")]
        let task_info = format!("{:?}", task.tag());

        #[cfg(feature = "tracing")]
        trace!("{} is running on {:?} on {:?}", task_info, self, machine);

        self.last_seen
            .store(machine.system.now(), Ordering::Relaxed);

        task.tag().set_index_hint(self.index);

        let mut woken = false;
        qlock = match self.without_qlock(machine, qlock, || woken = task.run()) {
            Some(qlock) => qlock,
            None => {
                #[cfg(feature = "tracing")]
                trace!("{} is done running on {:?}", task_info, machine);
                return None;
            }
        };
        if woken {
            qlock.flush_slot();
        }

        self.last_seen.store(u64::MAX, Ordering::Relaxed);

        #[cfg(feature = "tracing")]
        trace!(
            "{} is done running on {:?} on {:?}",
            task_info,
            self,
            machine
        );

        Some(qlock)
    }

    /// will fail if machine no longer hold the processor (stolen)
    #[inline(always)]
    fn try_acquire_qlock(&self, machine: &Machine) -> Option<SimpleLockGuard<Queue>> {
        let backoff = Backoff::new();
        loop {
            // fast check, without lock
            if !ptr::eq(self.current_machine.load(Ordering::Relaxed), machine) {
                return None;
            }

            if let Some(qlock) = self.local.try_lock() {
                // check again after locking
                if !ptr::eq(self.current_machine.load(Ordering::Relaxed), machine) {
                    drop(qlock);
                    return None;
                }

                return Some(qlock);
            }

            backoff.snooze();
        }
    }

    #[inline(always)]
    fn without_qlock(
        &self,
        machine: &Machine,
        qlock: SimpleLockGuard<Queue>,
        f: impl FnOnce(),
    ) -> Option<SimpleLockGuard<Queue>> {
        drop(qlock);
        f();
        self.try_acquire_qlock(machine)
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
    pub fn wake_up(&self) {
        self.unparker.unpark();
    }

    #[inline(always)]
    pub fn push_local(&self, machine: &Machine, task: Task) -> Result<usize, Task> {
        match self.try_acquire_qlock(machine) {
            None => Err(task),
            Some(mut qlock) => {
                #[cfg(feature = "tracing")]
                trace!("{:?} is pushed to {:?}'s local queue", task.tag(), self);

                Ok(qlock.push(task))
            }
        }
    }

    #[inline(always)]
    pub fn steal_local(&self, worker: &Worker<Task>) -> Steal<Task> {
        match self.stealers[0].steal_batch_and_pop(worker) {
            Steal::Success(task) => Steal::Success(task),
            Steal::Empty => self.stealers[1].steal_batch_and_pop(worker),
            Steal::Retry => Steal::Retry,
        }
    }

    #[inline(always)]
    pub fn push_global(&self, task: Task) {
        #[cfg(feature = "tracing")]
        trace!("{:?} is pushed to {:?}'s global queue", task.tag(), self);

        self.global.push(task);
    }

    #[inline(always)]
    pub fn pop_global(&self, worker: &Worker<Task>) -> Steal<Task> {
        self.global.steal_batch_and_pop(worker)
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.global.is_empty() && self.stealers[0].is_empty() && self.stealers[1].is_empty()
    }
}

#[cfg(feature = "tracing")]
impl std::fmt::Debug for Processor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(&format!("Processor({})", self.index))
    }
}

struct Queue {
    counter: usize,
    slot: Worker<Task>,
    worker: Worker<Task>,
}

impl Queue {
    fn new() -> Queue {
        Queue {
            counter: 0,
            slot: Worker::new_lifo(),
            worker: Worker::new_fifo(),
        }
    }

    #[inline(always)]
    fn flush_slot(&mut self) {
        let slot_stealer = self.slot.stealer();
        loop {
            if let Steal::Empty = slot_stealer.steal_batch(&self.worker) {
                break;
            }
        }
        self.counter = 0;
    }

    #[inline(always)]
    fn pop(&mut self) -> Option<Task> {
        self.slot
            .pop()
            .map(|t| {
                self.counter -= 1;
                t
            })
            .or_else(|| {
                self.counter = 0;
                self.worker.pop()
            })
    }

    #[inline(always)]
    fn push(&mut self, task: Task) -> usize {
        self.counter += 1;
        self.slot.push(task);
        self.counter
    }
}
