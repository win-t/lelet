use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};

use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use crossbeam_utils::Backoff;

#[cfg(feature = "tracing")]
use std::sync::atomic::AtomicUsize;

#[cfg(feature = "tracing")]
use log::trace;

use lelet_utils::{SimpleLock, SimpleLockGuard};

use super::machine::Machine;
use super::system::System;
use super::Task;

/// Processor is the one who run the task
pub struct Processor {
    #[cfg(feature = "tracing")]
    pub id: usize,

    system: Option<&'static System>,
    others: Vec<&'static Processor>,

    last_seen: AtomicU64,

    current_machine: AtomicPtr<Machine>,
    current_task: AtomicPtr<Task>,

    global: Injector<Task>,
    local: SimpleLock<Queue>,
    stealers: [Stealer<Task>; 2],
}

impl Processor {
    pub fn new() -> Processor {
        #[cfg(feature = "tracing")]
        static PROCESSOR_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

        let local = Queue::new();
        let stealers = [local.worker.stealer(), local.slot.stealer()];

        #[allow(clippy::let_and_return)]
        let processor = Processor {
            #[cfg(feature = "tracing")]
            id: PROCESSOR_ID_COUNTER.fetch_add(1, Ordering::Relaxed),

            system: None,
            others: vec![],

            last_seen: AtomicU64::new(0),

            current_machine: AtomicPtr::new(ptr::null_mut()),
            current_task: AtomicPtr::new(ptr::null_mut()),

            global: Injector::new(),
            local: SimpleLock::new(local),
            stealers,
        };

        #[cfg(feature = "tracing")]
        trace!("{:?} is created", processor);

        processor
    }

    #[inline(always)]
    pub fn set_system(&mut self, system: &'static System, others: Vec<&'static Processor>) {
        let old = self.system.replace(system);

        // can only be set once
        assert!(old.is_none());

        self.others = others;
    }

    #[inline(always)]
    pub fn run_on(&'static self, machine: &Machine) {
        macro_rules! check {
            ($qlock:expr) => {
                match $qlock {
                    Some(qlock) => qlock,
                    None => return,
                }
            };
        }

        // steal this processor from old machine
        self.current_machine
            .store(machine as *const _ as *mut _, Ordering::Relaxed);

        let mut qlock = check!(self.try_acquire_qlock(machine));

        // reset
        self.current_task.store(ptr::null_mut(), Ordering::Relaxed);
        self.last_seen.store(u64::MAX, Ordering::Relaxed);

        #[cfg(feature = "tracing")]
        trace!("{:?} is now running on {:?} ", self, machine);

        let system = self.system.unwrap();
        let mut check_for_help = true;

        macro_rules! self_run_task {
            ($task:expr) => {
                // we are going to run task that might be blocking
                // wake others processor in case we need help
                if check_for_help {
                    if !qlock.worker.is_empty()
                        || !qlock.slot.is_empty()
                        || !system.global_is_empty()
                    {
                        check_for_help = false;
                        system.processors_send_notif();
                    }
                }

                qlock = check!(self.run_task(machine, qlock, system.now(), $task));
            };
        }

        loop {
            qlock.flush_slot();
            if let Some(task) = self.pop_global(&qlock.worker) {
                self_run_task!(task);
            }

            for _ in 0..61 {
                macro_rules! run_task {
                    ($task:expr) => {
                        self_run_task!($task);
                        continue;
                    };
                }

                if let Some(task) = qlock.pop() {
                    run_task!(task);
                }

                // when local queue is empty:

                // 1. get from global queue
                if let Some(task) = self.pop_global(&qlock.worker) {
                    run_task!(task);
                }

                // 2. steal from others
                if let Some(task) = self.steal_others(&qlock.worker) {
                    run_task!(task);
                }

                // 3. no more task for now, just sleep
                {
                    #[cfg(feature = "tracing")]
                    trace!("{:?} entering sleep", self);

                    system.processors_wait_notif();
                    check_for_help = true;

                    #[cfg(feature = "tracing")]
                    trace!("{:?} exiting sleep", self);
                }

                break;
            }
        }
    }

    #[inline(always)]
    fn run_task<'a>(
        &'static self,
        machine: &Machine,
        mut qlock: SimpleLockGuard<'a, Queue>,
        now: u64,
        task: Task,
    ) -> Option<SimpleLockGuard<'a, Queue>> {
        #[cfg(feature = "tracing")]
        let task_info = format!("{:?}", task.tag());

        #[cfg(feature = "tracing")]
        trace!("{} is running on {:?} on {:?}", task_info, self, machine);

        self.last_seen.store(now, Ordering::Relaxed);

        self.current_task
            .store(task.tag() as *const _ as *mut _, Ordering::Relaxed);

        qlock = match self.without_qlock(machine, qlock, || {
            task.tag().set_processor_hint(self);
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
    pub fn push_local(&self, machine: &Machine, task: Task) -> Result<(), Task> {
        match self.try_acquire_qlock(machine) {
            None => Err(task),
            Some(qlock) => {
                // if currently running task is rescheduled, it mean yielding
                if ptr::eq(
                    self.current_task.load(Ordering::Relaxed),
                    task.tag() as *const _ as *mut _,
                ) {
                    #[cfg(feature = "tracing")]
                    trace!(
                        "{:?} is pushed to {:?}'s local queue (yielding)",
                        task.tag(),
                        self
                    );

                    qlock.push_into_worker(task);
                } else {
                    #[cfg(feature = "tracing")]
                    trace!("{:?} is pushed to {:?}'s local queue", task.tag(), self);

                    qlock.push_into_slot(task);
                }

                Ok(())
            }
        }
    }

    #[inline(always)]
    fn steal(&self, worker: &Worker<Task>) -> Steal<Task> {
        match self.stealers[0].steal_batch_and_pop(worker) {
            Steal::Success(task) => Steal::Success(task),
            Steal::Empty => self.stealers[1].steal_batch_and_pop(worker),
            Steal::Retry => Steal::Retry,
        }
    }

    #[inline(always)]
    fn steal_others(&self, worker: &Worker<Task>) -> Option<Task> {
        loop {
            let mut retry = false;

            for p in &self.others {
                match p.steal(worker) {
                    Steal::Success(task) => return Some(task),
                    Steal::Empty => {}
                    Steal::Retry => retry = true,
                }
            }

            if !retry {
                return None;
            }
        }
    }

    #[inline(always)]
    pub fn push_global(&self, task: Task) {
        #[cfg(feature = "tracing")]
        trace!("{:?} is pushed to {:?}'s global queue", task.tag(), self);

        self.global.push(task);
    }

    #[inline(always)]
    fn pop_global(&self, worker: &Worker<Task>) -> Option<Task> {
        loop {
            let mut retry = false;

            // check dedicated global queue first
            match self.global.steal_batch_and_pop(worker) {
                Steal::Success(task) => return Some(task),
                Steal::Empty => {}
                Steal::Retry => retry = true,
            }

            // then steal from others global queue
            for p in &self.others {
                match p.global.steal_batch_and_pop(worker) {
                    Steal::Success(task) => return Some(task),
                    Steal::Empty => {}
                    Steal::Retry => retry = true,
                }
            }

            if !retry {
                return None;
            }
        }
    }

    #[inline(always)]
    pub fn global_is_empty(&self) -> bool {
        self.global.is_empty()
    }

    #[inline(always)]
    pub fn local_is_empty(&self) -> bool {
        self.stealers[0].is_empty() && self.stealers[1].is_empty()
    }
}

#[cfg(feature = "tracing")]
impl std::fmt::Debug for Processor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(&format!("Processor({})", self.id))
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
    fn push_into_slot(&self, task: Task) {
        self.slot.push(task);
    }

    #[inline(always)]
    fn push_into_worker(&self, task: Task) {
        self.worker.push(task);
    }
}
