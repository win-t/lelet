use std::cell::RefCell;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crossbeam_deque::{Steal, Stealer, Worker};

#[cfg(feature = "tracing")]
use log::trace;

use crate::thread_pool;
use crate::utils::abort_on_panic;

use super::processor::{Processor, RunContext};
use super::system::System;
use super::Task;

/// Machine is the one who have OS thread
pub struct Machine {
    pub id: usize,

    /// stealer for the machine, Worker part is not here,
    /// and moved via closure, because Worker is !Sync
    stealer: Stealer<Task>,
}

static MACHINE_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

struct Current {
    processor: &'static Processor,
    machine: Arc<Machine>,
    worker: Rc<Worker<Task>>,
}

thread_local! {
  static CURRENT_TLS: RefCell<Option<Current>> = RefCell::new(None);
}

impl Machine {
    fn new() -> (Arc<Machine>, Worker<Task>) {
        let worker = Worker::new_fifo();
        let stealer = worker.stealer();
        let machine = Machine {
            id: MACHINE_ID_COUNTER.fetch_add(1, Ordering::Relaxed),
            stealer,
        };

        #[cfg(feature = "tracing")]
        trace!("{:?} is created", machine);

        (Arc::new(machine), worker)
    }

    pub fn replace(processor: &'static Processor, current: Option<Arc<Machine>>) -> Arc<Machine> {
        // just to make sure that current is current processor's machine
        if let Some(current) = current.as_ref() {
            assert!(processor.still_on_machine(current));
        }

        let (new, worker) = Machine::new();
        {
            let new = new.clone();
            thread_pool::spawn_box(Box::new(move || {
                abort_on_panic(move || {
                    let worker = Rc::new(worker);

                    CURRENT_TLS.with(|tls| {
                        tls.borrow_mut().replace(Current {
                            processor,
                            machine: new.clone(),
                            worker: worker.clone(),
                        })
                    });
                    defer! {
                      CURRENT_TLS.with(|tls| drop(tls.borrow_mut().take()));
                    }

                    // run processor with new machine
                    processor.run(&RunContext {
                        system: System::get(),
                        machine: &new,
                        worker: &worker,
                        inherit_tasks: current.map(|m| m.stealer.clone()),
                    });
                })
            }));
        }

        new
    }

    pub fn direct_push(task: Task) -> Result<(), Task> {
        CURRENT_TLS.with(|current| match current.borrow().as_ref() {
            Some(Current {
                processor,
                machine,
                worker,
            }) if processor.still_on_machine(machine) => {
                #[cfg(feature = "tracing")]
                trace!(
                    "{:?} pushed directly to {:?}'s machine",
                    task.tag(),
                    processor,
                );

                worker.push(task);
                Ok(())
            }
            _ => Err(task),
        })
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
