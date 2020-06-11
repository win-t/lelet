use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;

#[cfg(feature = "tracing")]
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(feature = "tracing")]
use log::trace;

use lelet_utils::abort_on_panic;

use crate::thread_pool;

use super::processor::Processor;
use super::Task;

/// Machine is the one who have OS thread
pub struct Machine {
    #[cfg(feature = "tracing")]
    pub id: usize,

    processor: &'static Processor,

    // !Send + !Sync
    _marker: PhantomData<*mut ()>,
}

thread_local! {
    static CURRENT: RefCell<Option<Rc<Machine>>> = RefCell::new(None);
}

impl Machine {
    #[inline(always)]
    fn new(processor: &'static Processor) -> Rc<Machine> {
        #[cfg(feature = "tracing")]
        static MACHINE_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

        let machine = Machine {
            #[cfg(feature = "tracing")]
            id: MACHINE_ID_COUNTER.fetch_add(1, Ordering::Relaxed),

            processor,

            _marker: PhantomData,
        };

        #[cfg(feature = "tracing")]
        trace!("{:?} is created", machine);

        Rc::new(machine)
    }

    #[inline(always)]
    fn run(self: &Rc<Machine>) {
        CURRENT.with(|current| {
            let old = current.borrow_mut().replace(self.clone());

            // just to make sure that the machine is not cached in thread pool
            assert!(old.is_none());
        });

        #[cfg(feature = "tracing")]
        crate::thread_pool::THREAD_ID.with(|tid| {
            trace!("{:?} is running on {:?}", self, tid);
        });

        self.processor.run_on(self);

        CURRENT.with(|current| current.borrow_mut().take());
    }
}

#[cfg(feature = "tracing")]
impl std::fmt::Debug for Machine {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(&format!("Machine({})", self.id))
    }
}

#[cfg(feature = "tracing")]
impl Drop for Machine {
    fn drop(&mut self) {
        trace!("{:?} is destroyed", self);
    }
}

#[inline(always)]
pub fn spawn(processor: &'static Processor) {
    thread_pool::spawn_box(Box::new(move || {
        abort_on_panic(move || {
            Machine::new(processor).run();
        })
    }));
}

#[inline(always)]
pub fn direct_push(task: Task) -> Result<(), Task> {
    CURRENT.with(|current| {
        let mut current = current.borrow_mut();
        match current.as_ref() {
            None => Err(task),
            Some(m) => match m.processor.push_local(m, task) {
                Ok(()) => Ok(()),
                Err(err) => {
                    current.take();
                    Err(err)
                }
            },
        }
    })
}

#[inline(always)]
pub fn respawn() {
    CURRENT.with(|current| {
        if let Some(m) = current.borrow_mut().take() {
            #[cfg(feature = "tracing")]
            trace!(
                "{:?} is giving up on {:?}, spawn new machine",
                m,
                m.processor
            );

            spawn(m.processor)
        }
    })
}
