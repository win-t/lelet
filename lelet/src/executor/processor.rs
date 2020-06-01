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
    sleeping: AtomicBool,

    current_machine: AtomicPtr<Machine>,
    current_task: AtomicPtr<Task>,

    global: Injector<Task>,
    local: SimpleLock<Queue>,
    stealers: [Stealer<Task>; 2],

    parker: SimpleLock<Parker>,
    unparker: Unparker,
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
            sleeping: AtomicBool::new(false),

            current_machine: AtomicPtr::new(ptr::null_mut()),
            current_task: AtomicPtr::new(ptr::null_mut()),

            global: Injector::new(),
            local: SimpleLock::new(local),
            stealers,

            parker: SimpleLock::new(parker),
            unparker,
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
        self.current_task.store(ptr::null_mut(), Ordering::Relaxed);
        self.last_seen.store(u64::MAX, Ordering::Relaxed);
        self.sleeping.store(false, Ordering::Relaxed);

        #[cfg(feature = "tracing")]
        trace!("{:?} is now running on {:?} ", self, machine);

        let mut wake_up_other = true;

        let backoff = if self.index == 0 {
            Some(Backoff::new())
        } else {
            None
        };

        loop {
            qlock.flush_slot();
            if let Some(task) = machine.system.pop_into(&qlock.worker, self) {
                qlock = check!(self.run_task(machine, qlock, &mut wake_up_other, task));
            }

            for _ in 0..37 {
                macro_rules! run_task {
                    ($task:expr) => {
                        qlock = check!(self.run_task(machine, qlock, &mut wake_up_other, $task));
                        continue;
                    };
                }

                if let Some(task) = qlock.pop() {
                    run_task!(task);
                }

                // when local queue is empty:

                // 1. get from global queue
                if let Some(task) = machine.system.pop_into(&qlock.worker, self) {
                    run_task!(task);
                }

                // 2. steal from others
                if let Some(task) = machine.system.steal_into(&qlock.worker, self) {
                    run_task!(task);
                }

                // 3. no more task for now, just sleep
                {
                    wake_up_other = true;

                    if let Some(backoff) = backoff.as_ref() {
                        if !backoff.is_completed() {
                            backoff.snooze();
                            continue;
                        }
                    }

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

                    if let Some(backoff) = backoff.as_ref() {
                        backoff.reset();
                    }
                }
            }
        }
    }

    #[inline(always)]
    fn run_task<'a>(
        &'a self,
        machine: &Machine,
        mut qlock: SimpleLockGuard<'a, Queue>,
        wake_up_other: &mut bool,
        task: Task,
    ) -> Option<SimpleLockGuard<'a, Queue>> {
        #[cfg(feature = "tracing")]
        let task_info = format!("{:?}", task.tag());

        #[cfg(feature = "tracing")]
        trace!("{} is running on {:?} on {:?}", task_info, self, machine);

        self.last_seen
            .store(machine.system.now(), Ordering::Relaxed);

        task.tag().set_index_hint(self.index);

        self.current_task
            .store(task.tag() as *const _ as *mut _, Ordering::Relaxed);

        // we are about to run a task that might be blocking
        // wake other to help
        if *wake_up_other && (!machine.system.is_empty()) {
            let processors = &machine.system.processors;
            let mut other_index = self.index + 1;
            if other_index == processors.len() {
                other_index = 0;
            }
            unsafe { processors.get_unchecked(other_index) }.wake_up();
            *wake_up_other = false;
        }

        qlock = match self.without_qlock(machine, qlock, || {
            task.run();
        }) {
            Some(qlock) => qlock,
            None => {
                #[cfg(feature = "tracing")]
                trace!("{} is done running on {:?}", task_info, machine);
                return None;
            }
        };

        self.current_task.store(ptr::null_mut(), Ordering::Relaxed);

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
    pub fn push_local(&self, machine: &Machine, task: Task) -> Result<(), Task> {
        match self.try_acquire_qlock(machine) {
            None => Err(task),
            Some(qlock) => {
                // do not push into slot when currently running task is rescheduled (yielding)
                let into_slot = !ptr::eq(
                    self.current_task.load(Ordering::Relaxed),
                    task.tag() as *const _ as *mut _,
                );

                #[cfg(feature = "tracing")]
                {
                    if into_slot {
                        trace!("{:?} is pushed to {:?}'s local queue", task.tag(), self);
                    } else {
                        trace!(
                            "{:?} is pushed to {:?}'s local queue (yielding)",
                            task.tag(),
                            self
                        );
                    }
                }

                qlock.push(task, into_slot);
                Ok(())
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
    slot: Worker<Task>,
    worker: Worker<Task>,
}

impl Queue {
    fn new() -> Queue {
        Queue {
            slot: Worker::new_lifo(),
            worker: Worker::new_fifo(),
        }
    }

    #[inline(always)]
    fn flush_slot(&self) {
        let slot_stealer = self.slot.stealer();
        loop {
            if let Steal::Empty = slot_stealer.steal_batch(&self.worker) {
                break;
            }
        }
    }

    #[inline(always)]
    fn pop(&self) -> Option<Task> {
        self.slot.pop().or_else(|| self.worker.pop())
    }

    #[inline(always)]
    fn push(&self, task: Task, into_slot: bool) {
        if into_slot {
            self.slot.push(task);
        } else {
            self.worker.push(task);
        }
    }
}
