use std::collections::VecDeque;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};
use std::sync::Arc;

use crossbeam_channel::{bounded, Receiver, Sender};
use crossbeam_deque::{Injector, Steal, Worker};
use crossbeam_utils::Backoff;

#[cfg(feature = "tracing")]
use log::trace;

use crate::utils::Spinlock;

use super::machine::Machine;
use super::system::System;
use super::Task;

/// Processor is the one who run the task
pub struct Processor {
    pub index: usize,

    /// for blocking detection
    /// usize::MAX mean the processor is sleeping
    last_seen: AtomicU64,

    /// current machine that holding the processor
    current_machine: AtomicPtr<Machine>,

    /// machines that holding the processor in the past, but still alive
    zombie_machines: Spinlock<VecDeque<Arc<Machine>>>,

    /// global queue dedicated to this processor
    injector: Injector<Task>,

    wake_up_notif_sender: Sender<()>,
    wake_up_notif_receiver: Receiver<()>,
}

// just to make sure
impl Drop for Processor {
    fn drop(&mut self) {
        eprintln!("Processor should not be dropped once created");
        std::process::abort();
    }
}

impl Processor {
    pub fn new(index: usize) -> Processor {
        // channel with buffer size 1 to not miss a notification
        let (wake_up_notif_sender, wake_up_notif_receiver) = bounded(1);

        #[allow(clippy::let_and_return)]
        let processor = Processor {
            index,

            last_seen: AtomicU64::new(0),

            current_machine: AtomicPtr::new(ptr::null_mut()),
            zombie_machines: Spinlock::new(VecDeque::with_capacity(1)),

            injector: Injector::new(),

            wake_up_notif_sender,
            wake_up_notif_receiver,
        };

        #[cfg(feature = "tracing")]
        trace!("{:?} is created", processor);

        processor
    }

    pub fn run_on(&self, system: &System, machine: Arc<Machine>, worker: &Worker<Task>) {
        // steal this processor from old machine and add old machine to zombie list
        if let Some(old_machine) = self.swap_machine(machine) {
            self.zombie_machines.lock().push_back(old_machine);
        }

        self.last_seen.store(system.now(), Ordering::Relaxed);

        let machine = self.get_current_machine().unwrap();

        #[cfg(feature = "tracing")]
        crate::thread_pool::THREAD_ID.with(|tid| {
            trace!("{:?} is now running on {:?} on {:?}", self, machine, tid);
        });

        // fill initial task from zombies machine
        self.inherit_zombies(worker);

        // Number of runs in a row before the global queue is inspected.
        const MAX_RUNS: usize = 16;
        let mut run_counter = 0;

        let sleep_backoff = Backoff::new();

        while self.still_on_machine(machine) {
            macro_rules! run_task {
                ($task:ident) => {
                    #[cfg(feature = "tracing")]
                    trace!("{:?} is going to run on {:?}", $task.tag(), self);

                    $task.tag().set_schedule_index_hint(self.index);
                    $task.run();
                    run_counter += 1;

                    continue;
                };
            }

            macro_rules! run_global_task {
                () => {
                    run_counter = 0;

                    // also check zombie, in case zombie still pushing
                    // task directly to its worker, see also ack_zombie
                    self.inherit_zombies(worker);

                    if let Some(task) = system.pop(self.index, worker) {
                        run_task!(task);
                    }

                    // in case we got nothing from global queue, but something
                    // from zombies
                    if let Some(task) = worker.pop() {
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
            if let Some(task) = worker.pop() {
                run_task!(task);
            }

            // at this point, the worker is empty

            // 1. pop from global queue
            run_global_task!();

            // 2. steal from others
            if let Some(task) = system.steal(&worker) {
                run_task!(task);
            }

            // 3.a. no more task for now, just sleep
            self.sleep(system, &sleep_backoff);

            // 3.b. after sleep, pop from global queue
            run_global_task!();
        }
    }

    fn swap_machine(&self, machine: Arc<Machine>) -> Option<Arc<Machine>> {
        let mut machine = Arc::into_raw(machine) as *mut _;
        machine = self.current_machine.swap(machine, Ordering::Relaxed);
        if machine.is_null() {
            None
        } else {
            Some(unsafe { Arc::from_raw(machine) })
        }
    }

    #[inline(always)]
    pub fn get_current_machine(&self) -> Option<&Machine> {
        let current_machine = self.current_machine.load(Ordering::Relaxed);
        if current_machine.is_null() {
            None
        } else {
            Some(unsafe { &*current_machine })
        }
    }

    #[inline(always)]
    pub fn still_on_machine(&self, machine: &Machine) -> bool {
        self.get_current_machine()
            .map(|current_machine| ptr::eq(current_machine, machine))
            .unwrap_or(false)
    }

    fn inherit_zombies(&self, worker: &Worker<Task>) {
        self.zombie_machines
            .lock()
            .iter()
            .for_each(|zombie| zombie.steal_all(worker));
    }

    /// this is a promise that machine will not push
    /// to its worker anymore
    /// so we can safely remove machine from zombie list,
    /// and no task is stalled on machine worker
    pub fn ack_zombie(&self, machine: &Machine) {
        self.zombie_machines
            .lock()
            .retain(|zombie| !ptr::eq(zombie as &Machine, machine));
    }

    fn sleep(&self, system: &System, backoff: &Backoff) {
        if backoff.is_completed() {
            self.last_seen.store(u64::MAX, Ordering::Relaxed);
            self.wake_up_notif_receiver.recv().unwrap();
            self.last_seen.store(system.now(), Ordering::Relaxed);

            // wake the sysmon in case the sysmon is also sleeping
            system.sysmon_wake_up();

            backoff.reset();
        } else {
            backoff.snooze();
        }
    }

    /// will return usize::MAX when processor is sleeping (always seen in the future)
    #[inline(always)]
    pub fn get_last_seen(&self) -> u64 {
        self.last_seen.load(Ordering::Relaxed)
    }

    pub fn wake_up(&self) -> bool {
        self.wake_up_notif_sender.try_send(()).is_ok()
    }

    pub fn push(&self, task: Task) {
        #[cfg(feature = "tracing")]
        trace!("{:?} pushed to {:?}", task.tag(), self);

        self.injector.push(task);
    }

    pub fn pop(&self, worker: &Worker<Task>) -> Option<Task> {
        // repeat until success or empty
        std::iter::repeat_with(|| self.injector.steal_batch_and_pop(worker))
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
