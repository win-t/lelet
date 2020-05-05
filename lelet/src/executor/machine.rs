use std::cell::RefCell;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(feature = "tracing")]
use log::trace;

use lelet_utils::abort_on_panic;
use lelet_utils::defer;

use crate::thread_pool;

use super::system::System;
use super::Task;

/// Machine is the one who have OS thread
pub struct Machine {
    pub id: usize,
    pub system: &'static System,
    pub processor_index: usize,
}

thread_local! {
    static CURRENT: RefCell<Option<Rc<Machine>>> = RefCell::new(None);
}

impl Machine {
    fn new(system: &'static System, processor_index: usize) -> Machine {
        static MACHINE_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

        #[allow(clippy::let_and_return)]
        let machine = Machine {
            id: MACHINE_ID_COUNTER.fetch_add(1, Ordering::Relaxed),
            system,
            processor_index,
        };

        #[cfg(feature = "tracing")]
        trace!("{:?} is created", machine);

        machine
    }

    pub fn spawn(system: &'static System, processor_index: usize) {
        thread_pool::spawn_box(Box::new(move || {
            abort_on_panic(move || {
                CURRENT.with(|current| {
                    current
                        .borrow_mut()
                        .replace(Rc::new(Machine::new(system, processor_index)));

                    defer! {
                        current.borrow_mut().take();
                    }

                    system.processors[processor_index].run();
                });
            })
        }));
    }

    pub fn direct_push(task: Task) -> Result<(), Task> {
        CURRENT.with(|current| {
            let mut current = current.borrow_mut();
            match current.as_ref() {
                None => Err(task),
                Some(m) => m.system.processors[m.processor_index]
                    .push(task)
                    .map_err(|err| {
                        // current no longer hold it processor, take it out
                        current.take();
                        err
                    }),
            }
        })
    }

    pub fn respawn() {
        CURRENT.with(|current| {
            if let Some(m) = current.borrow_mut().take() {
                #[cfg(feature = "tracing")]
                trace!(
                    "{:?} giving up on {:?}",
                    m,
                    m.system.processors[m.processor_index]
                );

                Machine::spawn(m.system, m.processor_index)
            }
        })
    }

    pub fn get() -> Option<Rc<Machine>> {
        CURRENT.with(|current| current.borrow().as_ref().cloned())
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
