use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use crossbeam_deque::{Steal, Stealer, Worker};

#[cfg(feature = "tracing")]
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(feature = "tracing")]
use log::trace;

use lelet_utils::abort_on_panic;
use lelet_utils::defer;

use crate::thread_pool;

use super::processor::Processor;
use super::system::System;
use super::Task;

/// Machine is the one who have OS thread
pub struct Machine {
    #[cfg(feature = "tracing")]
    id: usize,

    /// stealer for the machine, Worker part is not here,
    /// it is stored on thread tls, because Worker is !Sync
    stealer: Stealer<Task>,
}

struct Current {
    active: Option<(&'static System, Arc<Machine>, &'static Processor)>,
    worker: Rc<Worker<Task>>,
}

impl Current {
    fn new() -> Current {
        Current {
            active: None,
            worker: Rc::new(Worker::new_fifo()),
        }
    }

    fn init(
        &mut self,
        processor: &'static Processor,
    ) -> (&'static System, Arc<Machine>, Rc<Worker<Task>>) {
        self.clean_up();

        let system = System::get();
        let machine = Arc::new(Machine::new(&self.worker));
        let worker = self.worker.clone();

        self.active.replace((system, machine.clone(), processor));

        (system, machine, worker)
    }

    fn clean_up(&mut self) {
        self.active.take();
    }

    fn push(&mut self, task: Task) -> Result<(), Task> {
        if let Some((system, machine, processor)) = self.active.as_ref() {
            if processor.still_on_machine(machine) {
                #[cfg(feature = "tracing")]
                trace!(
                    "{:?} directly pushed to {:?}'s machine",
                    task.tag(),
                    processor
                );

                self.worker.push(task);
                system.processors_wake_up(processor.index);

                Ok(())
            } else {
                self.clean_up();
                Err(task)
            }
        } else {
            Err(task)
        }
    }

    fn respawn(&mut self) {
        if let Some((_, _machine, processor)) = self.active.as_ref() {
            #[cfg(feature = "tracing")]
            trace!("{:?} giving up on {:?}", _machine, processor);

            Machine::spawn(processor);
        }

        self.clean_up();
    }
}

thread_local! {
    static CACHE: RefCell<Option<Current>> = RefCell::new(None);
}

impl Machine {
    fn new(worker: &Worker<Task>) -> Machine {
        #[cfg(feature = "tracing")]
        static MACHINE_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

        #[allow(clippy::let_and_return)]
        let machine = Machine {
            #[cfg(feature = "tracing")]
            id: MACHINE_ID_COUNTER.fetch_add(1, Ordering::Relaxed),

            stealer: worker.stealer(),
        };

        #[cfg(feature = "tracing")]
        trace!("{:?} is created", machine);

        machine
    }

    pub fn spawn(processor: &'static Processor) {
        thread_pool::spawn_box(Box::new(move || {
            abort_on_panic(move || {
                CACHE.with(|cache| {
                    // ensure cache
                    if cache.borrow().is_none() {
                        cache.borrow_mut().replace(Current::new());
                    }

                    let (system, machine, worker) =
                        cache.borrow_mut().as_mut().unwrap().init(processor);

                    defer! {
                        cache.borrow_mut().as_mut().unwrap().clean_up();
                    }

                    processor.run_on(system, machine, &worker);
                });
            })
        }));
    }

    pub fn direct_push(task: Task) -> Result<(), Task> {
        CACHE.with(|cache| match cache.borrow_mut().as_mut() {
            Some(current) => current.push(task),
            None => Err(task),
        })
    }

    pub fn respawn() {
        CACHE.with(|cache| {
            if let Some(current) = cache.borrow_mut().as_mut() {
                current.respawn();
            }
        });
    }

    pub fn steal(&self, worker: &Worker<Task>) -> Option<Task> {
        // repeat until success or empty
        std::iter::repeat_with(|| self.stealer.steal_batch_and_pop(worker))
            .find(|s| !s.is_retry())
            .map(|s| match s {
                Steal::Success(task) => Some(task),
                Steal::Empty => None,
                Steal::Retry => unreachable!(), // already filtered
            })
            .flatten()
    }

    pub fn steal_all(&self, worker: &Worker<Task>) {
        // repeat until empty
        std::iter::repeat_with(|| self.stealer.steal_batch(worker)).find(|s| s.is_empty());
    }
}

#[cfg(feature = "tracing")]
impl std::fmt::Debug for Machine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!("Machine({})", self.id))
    }
}

#[cfg(feature = "tracing")]
impl Drop for Machine {
    fn drop(&mut self) {
        trace!("{:?} is destroyed", self);
    }
}
