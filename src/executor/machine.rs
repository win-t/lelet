use std::cell::RefCell;
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
    active: Option<(&'static System, Arc<Machine>, &'static Processor)>,
    worker: Worker<Task>,
}

impl Current {
    fn new() -> Current {
        Current {
            active: None,
            worker: Worker::new_fifo(),
        }
    }

    fn init(&mut self, processor: &'static Processor) {
        self.clean_up();
        self.active.replace((
            System::get(),
            Arc::new(Machine::new(&self.worker)),
            processor,
        ));
    }

    fn clean_up(&mut self) {
        if let Some((system, machine, processor)) = self.active.take() {
            // clean up all task in worker
            while let Some(task) = self.worker.pop() {
                task.tag().set_schedule_index_hint(processor.index);
                system.push(task);
            }

            processor.ack_zombie(&machine);
        }
    }

    fn still_valid(&self) -> bool {
        self.active
            .as_ref()
            .map(|(_, machine, processor)| processor.still_on_machine(machine))
            .unwrap_or(false)
    }

    fn push(&self, task: Task) {
        if let Some((system, _, _)) = self.active.as_ref() {
            self.worker.push(task);
            system.processors_wake_up();
        }
    }

    fn run(&self) {
        if let Some((system, machine, processor)) = self.active.as_ref() {
            processor.run_on(system, machine.clone(), &self.worker);
        }
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
                    if cache.borrow().is_none() {
                        cache.borrow_mut().replace(Current::new());
                    }

                    cache.borrow_mut().as_mut().unwrap().init(processor);
                    defer! {
                        cache.borrow_mut().as_mut().unwrap().clean_up();
                    }

                    cache.borrow().as_ref().unwrap().run();
                });
            })
        }));
    }

    pub fn direct_push(task: Task) -> Result<(), Task> {
        CACHE.with(|cache| match cache.borrow_mut().as_mut() {
            Some(current) => {
                if current.still_valid() {
                    current.push(task);
                    Ok(())
                } else {
                    current.clean_up();
                    Err(task)
                }
            }
            None => Err(task),
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
