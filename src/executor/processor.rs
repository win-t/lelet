use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crossbeam_channel::{bounded, Receiver, Sender};
use crossbeam_deque::{Injector, Steal, Worker};
use crossbeam_utils::Backoff;

#[cfg(feature = "tracing")]
use log::trace;

use super::machine::Machine;
use super::system::System;
use super::Task;

/// Processor is the one who run the task
pub struct Processor {
    pub index: usize,

    /// current machine that hold the processor
    machine_id: AtomicUsize,

    /// for blocking detection
    /// usize::MAX mean the processor is sleeping
    last_seen: AtomicU64,

    // global queue dedicated to this processor
    injector: Injector<Task>,
    injector_notif: Sender<()>,
    injector_notif_recv: Receiver<()>,
}

pub struct RunContext<'a> {
    pub system: &'a System,
    pub machine: &'a Machine,
    pub worker: &'a Worker<Task>,
}

impl Processor {
    pub fn new(index: usize) -> Processor {
        // channel with buffer size 1 to not miss a notification
        let (injector_notif, injector_notif_recv) = bounded(1);

        #[allow(clippy::let_and_return)]
        let processor = Processor {
            index,

            last_seen: AtomicU64::new(u64::MAX),

            injector: Injector::new(),
            injector_notif,
            injector_notif_recv,

            machine_id: AtomicUsize::new(usize::MAX), // no machine
        };

        #[cfg(feature = "tracing")]
        trace!("{:?} is created", processor);

        processor
    }

    pub fn run(&self, ctx: &RunContext) {
        let RunContext {
            system,
            machine,
            worker,
            ..
        } = ctx;

        // steal self from old machine
        self.machine_id.store(machine.id, Ordering::Relaxed);
        self.last_seen.store(system.now(), Ordering::Relaxed);

        #[cfg(feature = "tracing")]
        crate::thread_pool::THREAD_ID.with(|tid| {
            trace!("{:?} is now running on {:?} on {:?}", self, machine, tid);
        });

        // Number of runs in a row before the global queue is inspected.
        const MAX_RUNS: usize = 16;
        let mut run_counter = 0;

        let sleep_backoff = Backoff::new();

        'main: loop {
            // mark this processor still healthy
            self.last_seen.store(system.now(), Ordering::Relaxed);

            macro_rules! run_task {
                ($task:ident) => {
                    #[cfg(feature = "tracing")]
                    let last_task = format!("{:?}", $task.tag());

                    if self.still_on_machine(machine) {
                        #[cfg(feature = "tracing")]
                        trace!("{:?} is running on {:?}", $task.tag(), self);

                        // update the tag, so this task will be push to this processor again
                        $task.tag().set_schedule_index_hint(self.index);
                        $task.run();
                    } else {
                        // there is possibility that (*) is skipped because of race condition,
                        // put it back in global queue
                        system.push($task);
                    }

                    #[cfg(feature = "tracing")]
                    if self.still_on_machine(machine) {
                        trace!("{} is done running on {:?}", last_task, self);
                    }

                    if !self.still_on_machine(machine) {
                        // (*) we now running on thread on machine without processor (stealed)
                        // that mean the task was blocking, MUST exit now

                        #[cfg(feature = "tracing")]
                        crate::thread_pool::THREAD_ID.with(|tid| {
                            trace!("{} was blocking on {:?} on {:?}", last_task, machine, tid);
                        });

                        return;
                    }

                    run_counter += 1;
                    continue 'main;
                };
            }

            macro_rules! get_tasks {
                () => {
                    run_counter = 0;
                    let _ = self.injector_notif_recv.try_recv(); // flush the notification channel
                    if let Some(task) = system.pop(self.index, worker) {
                        run_task!(task);
                    }
                };
            }

            if run_counter >= MAX_RUNS {
                get_tasks!();
            }

            // run all task in the worker
            if let Some(task) = worker.pop() {
                run_task!(task);
            }

            // at this point, the worker is empty

            // 1. pop from global queue
            get_tasks!();

            // 2. steal from others
            if let Some(task) = system.steal(&worker) {
                run_task!(task);
            }

            // 3.a. no more task for now, just sleep
            self.sleep(ctx, &sleep_backoff);

            // 3.b. after sleep, pop from global queue
            get_tasks!();
        }
    }

    fn sleep(&self, ctx: &RunContext, backoff: &Backoff) {
        let RunContext { system, .. } = ctx;
        if backoff.is_completed() {
            #[cfg(feature = "tracing")]
            trace!("{:?} entering sleep", self);

            #[cfg(feature = "tracing")]
            defer! {
              trace!("{:?} leaving sleep", self);
            }

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

    #[inline(always)]
    pub fn still_on_machine(&self, machine: &Machine) -> bool {
        self.machine_id.load(Ordering::Relaxed) == machine.id
    }

    /// will return usize::MAX when processor is sleeping (always seen in the future)
    #[inline(always)]
    pub fn get_last_seen(&self) -> u64 {
        self.last_seen.load(Ordering::Relaxed)
    }

    /// return true if wake up signal is delivered
    pub fn wake_up(&self) -> bool {
        self.injector_notif.try_send(()).is_ok()
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
