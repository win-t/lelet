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
    fn new(system: &'static System, processor_index: usize) -> Rc<Machine> {
        static MACHINE_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

        let machine = Machine {
            id: MACHINE_ID_COUNTER.fetch_add(1, Ordering::Relaxed),
            system,
            processor_index,
        };

        #[cfg(feature = "tracing")]
        trace!("{:?} is created", machine);

        Rc::new(machine)
    }

    fn run(self: Rc<Machine>) {
        CURRENT.with(|m| m.borrow_mut().replace(self.clone()));
        defer! { CURRENT.with(|m| { m.borrow_mut().take() }); }

        self.system.processors[self.processor_index].run_on(&self);
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

pub fn spawn(system: &'static System, processor_index: usize) {
    thread_pool::spawn_box(Box::new(move || {
        abort_on_panic(move || {
            Machine::new(system, processor_index).run();
        })
    }));
}

pub fn direct_push(task: Task) -> Result<(), Task> {
    CURRENT.with(|current| match current.borrow().as_ref() {
        Some(m) => m.system.processors[m.processor_index].push(m, task),
        None => Err(task),
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

            spawn(m.system, m.processor_index)
        }
    })
}
