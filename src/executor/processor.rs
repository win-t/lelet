use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
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
    current_machine: Option<Arc<Machine>>,

    /// machines that holding the processor in the past, but still alive
    zombie_machines: Spinlock<VecDeque<Arc<Machine>>>,

    /// global queue dedicated to this processor
    injector: Injector<Task>,
    injector_notif: Sender<()>,
    injector_notif_recv: Receiver<()>,
}

impl Processor {
    pub fn new(index: usize) -> Processor {
        // channel with buffer size 1 to not miss a notification
        let (injector_notif, injector_notif_recv) = bounded(1);

        #[allow(clippy::let_and_return)]
        let processor = Processor {
            index,

            last_seen: AtomicU64::new(0),

            current_machine: None,
            zombie_machines: Spinlock::new(VecDeque::with_capacity(1)),

            injector: Injector::new(),
            injector_notif,
            injector_notif_recv,
        };

        #[cfg(feature = "tracing")]
        trace!("{:?} is created", processor);

        processor
    }

    fn swap_machine(&self, new: Arc<Machine>) -> Option<Arc<Machine>> {
        use std::mem::transmute;
        use std::sync::atomic::AtomicPtr;

        let output = Some(new);

        // force atomic swap self.current_machine,
        // this is safe because Option<Arc<Machine>> have same size
        // with AtomicPtr<()>, because they ARE pointer
        unsafe {
            if false {
                // just to make sure at compile time
                // https://internals.rust-lang.org/t/compile-time-assert/6751/2
                // do not run this code, this code will surely crash the program
                transmute::<AtomicPtr<()>, Option<Arc<Machine>>>(AtomicPtr::default());
            }

            let backoff = Backoff::new();
            loop {
                let current = &self.current_machine;
                let current = &*(current as *const Option<Arc<Machine>> as *const AtomicPtr<()>);
                let output = &*(&output as *const Option<Arc<Machine>> as *const AtomicPtr<()>);
                let tmp = current.load(Ordering::Relaxed);
                if current.compare_and_swap(tmp, output.load(Ordering::Relaxed), Ordering::Relaxed)
                    == tmp
                {
                    output.store(tmp, Ordering::Relaxed);
                    break;
                }
                backoff.snooze();
            }
        }

        output
    }

    pub fn run_on(&self, system: &System, machine: Arc<Machine>, worker: &Worker<Task>) {
        // steal this processor from old machine and add old machine to zombie list
        if let Some(old_machine) = self.swap_machine(machine) {
            self.zombie_machines.lock().push_back(old_machine);
        }

        self.last_seen.store(system.now(), Ordering::Relaxed);

        let machine = &self.current_machine.as_ref().unwrap() as &Machine;

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

        'main: loop {
            // mark this processor on every iteration
            self.last_seen.store(system.now(), Ordering::Relaxed);

            macro_rules! run_task {
                ($task:ident) => {
                    #[cfg(feature = "tracing")]
                    trace!("{:?} is going to run on {:?}", $task.tag(), self);

                    // also mark this processor before running task
                    self.last_seen.store(system.now(), Ordering::Relaxed);

                    // update the tag, so this task will be push to this processor again
                    $task.tag().set_schedule_index_hint(self.index);
                    $task.run();

                    if !self.still_on_machine(machine) {
                        // we now running on thread on machine without processor (stolen)
                        // that mean the task was blocking, MUST exit now.
                        // but, there is possibility of race condition (new machine take over after
                        // this check, but before next run), but that is okay,
                        // eventually old machine thread will go here

                        // remove old machine from zombie list
                        self.zombie_machines
                            .lock()
                            .retain(|old_machine| !old_machine.eq(machine));

                        return;
                    }

                    run_counter += 1;
                    continue 'main;
                };
            }

            macro_rules! run_global_task {
                () => {
                    // also check zombie, in case zombie still pushing
                    // task directly to its worker
                    self.inherit_zombies(worker);
                    if let Some(task) = worker.pop() {
                        run_task!(task);
                    }

                    run_counter = 0;
                    if let Some(task) = system.pop(self.index, worker) {
                        run_task!(task);
                    }
                };
            }

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

    #[inline(always)]
    pub fn still_on_machine(&self, machine: &Machine) -> bool {
        self.current_machine
            .as_ref()
            .map(|current_machine| current_machine.eq(machine))
            .unwrap_or(false)
    }

    #[inline(always)]
    pub fn get_machine(&self) -> &Option<Arc<Machine>> {
        &self.current_machine
    }

    #[inline(always)]
    fn inherit_zombies(&self, worker: &Worker<Task>) {
        self.zombie_machines
            .lock()
            .iter()
            .for_each(|zombie| zombie.steal_all(worker));
    }

    fn sleep(&self, system: &System, backoff: &Backoff) {
        if backoff.is_completed() {
            self.last_seen.store(u64::MAX, Ordering::Relaxed);
            self.injector_notif_recv.recv().unwrap();
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

    #[inline(always)]
    pub fn wake_up(&self) {
        let _ = self.injector_notif.try_send(());
    }

    pub fn push(&self, task: Task) {
        #[cfg(feature = "tracing")]
        trace!("{:?} pushed to {:?}", task.tag(), self);

        self.injector.push(task);
    }

    pub fn pop(&self, worker: &Worker<Task>) -> Option<Task> {
        // flush the notification channel
        let _ = self.injector_notif_recv.try_recv();

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
