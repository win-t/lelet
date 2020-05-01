use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use crossbeam_deque::{Steal, Stealer, Worker};

#[cfg(feature = "tracing")]
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(feature = "tracing")]
use log::trace;

use crate::thread_pool;
use crate::utils::abort_on_panic;

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
    live: Option<(&'static System, Arc<Machine>, &'static Processor)>,
    worker: Rc<Worker<Task>>,
}

thread_local! {
  static CURRENT: RefCell<Option<Current>> = RefCell::new(None);
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
                CURRENT.with(|current| {
                    if current.borrow().is_none() {
                        current.borrow_mut().replace(Current {
                            worker: Rc::new(Worker::new_fifo()),
                            live: None,
                        });
                    }

                    let system = System::get();
                    let worker = current.borrow().as_ref().unwrap().worker.clone();
                    let machine = Arc::new(Machine::new(&worker));

                    // set current live machine
                    current.borrow_mut().as_mut().unwrap().live.replace((
                        system,
                        machine.clone(),
                        processor,
                    ));

                    defer! {
                        // take out the current live machine
                        current
                            .borrow_mut()
                            .as_mut()
                            .unwrap()
                            .live
                            .take();
                    }

                    defer! {
                        // clean up all task in worker to make sure
                        // there is no task is stalled
                        while let Some(task) = worker.pop() {
                            task.tag().set_schedule_index_hint(processor.index);
                            system.push(task);
                        }
                    }

                    processor.run_on(system, machine, &worker);
                });
            })
        }));
    }

    pub fn direct_push(task: Task) -> Result<(), Task> {
        CURRENT.with(|current| match current.borrow().as_ref() {
            Some(Current {
                live: Some((system, machine, processor)),
                worker,
            }) if processor.still_on_machine(machine) => {
                #[cfg(feature = "tracing")]
                trace!(
                    "{:?} pushed directly to {:?}'s machine",
                    task.tag(),
                    processor,
                );

                worker.push(task);
                system.processors_wake_up();

                Ok(())
            }
            _ => Err(task),
        })
    }

    #[inline(always)]
    pub fn eq(&self, other: &Machine) -> bool {
        std::ptr::eq(self, other)
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
