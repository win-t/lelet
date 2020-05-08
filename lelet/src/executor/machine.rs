use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;

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
    pub id: usize,

    pub system: &'static System,
    pub processor: &'static Processor,

    // !Send + !Sync
    _marker: PhantomData<*mut ()>,
}

thread_local! {
    static CURRENT: RefCell<Option<Rc<Machine>>> = RefCell::new(None);
}

impl Machine {
    fn new(system: &'static System, processor: &'static Processor) -> Rc<Machine> {
        #[cfg(feature = "tracing")]
        static MACHINE_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

        let machine = Machine {
            #[cfg(feature = "tracing")]
            id: MACHINE_ID_COUNTER.fetch_add(1, Ordering::Relaxed),

            system,
            processor,
            _marker: PhantomData,
        };

        #[cfg(feature = "tracing")]
        trace!("{:?} is created", machine);

        Rc::new(machine)
    }

    fn run(self: &Rc<Machine>) {
        CURRENT.with(|current| current.borrow_mut().replace(self.clone()));
        defer! { CURRENT.with(|current| { current.borrow_mut().take() }); }

        self.processor.run_on(self);
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

pub fn spawn(system: &'static System, processor: &'static Processor) {
    thread_pool::spawn_box(Box::new(move || {
        abort_on_panic(move || {
            Machine::new(system, processor).run();
        })
    }));
}

pub fn direct_push(task: Task) -> Result<(), Task> {
    CURRENT.with(|current| {
        let mut current = current.borrow_mut();
        match current.as_ref() {
            None => Err(task),
            Some(m) => m.processor.check_machine_and_push(m, task).map_err(|err| {
                current.take();
                err
            }),
        }
    })
}

/// spawn new machine for current processor.
///
/// this is useful if you know that you are going to do blocking that longer
/// than blocking threshold.
pub fn mark_blocking() {
    CURRENT.with(|current| {
        if let Some(m) = current.borrow_mut().take() {
            #[cfg(feature = "tracing")]
            trace!("{:?} giving up on {:?}", m, m.processor);

            spawn(m.system, m.processor)
        }
    })
}
