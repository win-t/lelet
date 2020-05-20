use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering};

use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use crossbeam_utils::Backoff;

#[cfg(feature = "tracing")]
use log::trace;

use lelet_utils::{Parker, SimpleLock, SimpleLockGuard};

use super::machine::Machine;
use super::task::TaskTag;
use super::Task;

/// Processor is the one who run the task
pub struct Processor {
    pub index: usize,

    last_seen: AtomicU64,

    current_machine: AtomicPtr<Machine>,
    current_task: AtomicPtr<TaskTag>,

    global: Injector<Task>,
    local: SimpleLock<Queue>,
    stealers: [Stealer<Task>; 2],

    parker: Parker,
    sleeping: AtomicBool,
}

impl Processor {
    pub fn new(index: usize) -> Processor {
        let local = Queue::new();
        let stealers = [local.slot.stealer(), local.worker.stealer()];

        #[allow(clippy::let_and_return)]
        let processor = Processor {
            index,

            last_seen: AtomicU64::new(0),

            current_machine: AtomicPtr::new(ptr::null_mut()),
            current_task: AtomicPtr::new(ptr::null_mut()),

            global: Injector::new(),
            local: SimpleLock::new(local),
            stealers,

            parker: Parker::default(),
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

        // steal this processor from its machine
        self.current_machine
            .store(machine as *const _ as *mut _, Ordering::Relaxed);

        // in case it is sleeping
        self.wake_up();

        let mut qlock = check!(self.try_acquire_qlock(machine));

        // reset
        self.last_seen.store(u64::MAX, Ordering::Relaxed);
        self.sleeping.store(false, Ordering::Relaxed);
        self.current_task.store(ptr::null_mut(), Ordering::Relaxed);

        #[cfg(feature = "tracing")]
        crate::thread_pool::THREAD_ID.with(|tid| {
            trace!("{:?} is now running on {:?} on {:?}", self, machine, tid);
        });

        loop {
            for _ in 0..61 {
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

                // 3.a. no more task for now, just sleep
                self.sleeping.store(true, Ordering::Relaxed);
                qlock = check!(self.without_qlock(machine, qlock, || self.parker.park()));
                self.sleeping.store(false, Ordering::Relaxed);
                machine.system.sysmon_wake_up();

                // 3.b. after sleep, get from global queue
                if let Some(task) = machine.system.pop(&qlock.worker, self) {
                    run_task!(task);
                }
            }

            // flush queue slot and check global queue occasionally
            // for fair scheduling and preventing starvation
            qlock.flush_slot();
            if let Some(task) = machine.system.pop(&qlock.worker, self) {
                qlock = check!(self.run_task(machine, qlock, task));
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
        trace!("{} is running on {:?}", task_info, self);

        self.current_task
            .store(task.tag() as *const _ as *mut _, Ordering::Relaxed);

        self.last_seen
            .store(machine.system.now(), Ordering::Relaxed);

        task.tag().set_index_hint(self.index);

        qlock = match self.without_qlock(machine, qlock, || task.run()) {
            Some(qlock) => qlock,
            None => {
                #[cfg(feature = "tracing")]
                trace!("{} is done running on {:?}", task_info, machine);
                return None;
            }
        };

        self.last_seen.store(u64::MAX, Ordering::Relaxed);

        self.current_task.store(ptr::null_mut(), Ordering::Relaxed);

        #[cfg(feature = "tracing")]
        trace!("{} is done running on {:?}", task_info, self);

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
    fn without_qlock<T>(
        &self,
        machine: &Machine,
        qlock: SimpleLockGuard<Queue>,
        f: impl FnOnce() -> T,
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
        self.parker.unpark();
    }

    #[inline(always)]
    pub fn push_local(&self, machine: &Machine, task: Task) -> Result<(), Task> {
        match self.try_acquire_qlock(machine) {
            None => Err(task),
            Some(mut qlock) => {
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

                qlock.push(prioritized, task);

                Ok(())
            }
        }
    }

    #[inline(always)]
    pub fn steal_local(&self, worker: &Worker<Task>) -> Option<Task> {
        retry_steal(|| self.stealers[0].steal_batch_and_pop(worker))
            .or_else(|| retry_steal(|| self.stealers[1].steal_batch_and_pop(worker)))
    }

    #[inline(always)]
    pub fn push_global(&self, task: Task) {
        #[cfg(feature = "tracing")]
        trace!("{:?} is pushed to {:?}'s global queue", task.tag(), self);

        self.global.push(task);
    }

    #[inline(always)]
    pub fn pop_global(&self, worker: &Worker<Task>) -> Option<Task> {
        retry_steal(|| self.global.steal_batch_and_pop(worker))
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
    fn pop(&mut self) -> Option<Task> {
        self.slot.pop().or_else(|| self.worker.pop())
    }

    #[inline(always)]
    fn push(&mut self, prioritized: bool, task: Task) {
        if prioritized {
            self.slot.push(task);
        } else {
            self.worker.push(task);
        }
    }
}

#[inline(always)]
fn retry_steal(f: impl Fn() -> Steal<Task>) -> Option<Task> {
    loop {
        match f() {
            Steal::Success(task) => return Some(task),
            Steal::Empty => return None,
            Steal::Retry => {}
        }
    }
}
