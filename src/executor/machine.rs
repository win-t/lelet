use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex;

use crossbeam_deque::{Steal, Stealer, Worker};
use once_cell::sync::Lazy;

#[cfg(feature = "tracing")]
use log::trace;

use crate::thread_pool;
use crate::utils::abort_on_panic;

use super::processor::{Processor, RunContext};
use super::system::SYSTEM;
use super::Task;

/// Machine is the one who have thread
/// every machine have thier own local Worker queue
pub struct Machine {
    pub id: usize,

    /// stealer for the machine, worker part is moved via closure,
    /// because Worker is !Send+!Sync
    stealer: Stealer<Task>,
}

static MACHINE_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

struct Current {
    processor: &'static Processor,
    machine: Arc<Machine>,
    worker: Rc<WorkerWrapper>,
}

thread_local! {
  static CURRENT: RefCell<Option<Current>> = RefCell::new(None);
}

impl Machine {
    fn new() -> (Arc<Machine>, WorkerWrapper) {
        let worker = WorkerWrapper::new();
        let stealer = worker.stealer();
        let machine = Machine {
            id: MACHINE_ID_COUNTER.fetch_add(1, Ordering::Relaxed),
            stealer,
        };

        #[cfg(feature = "tracing")]
        trace!("{:?} is created", machine);

        (Arc::new(machine), worker)
    }

    pub fn replace(
        processor: &'static Processor,
        old_machine: Option<Arc<Machine>>,
    ) -> Arc<Machine> {
        // just to make sure that the old machine is current processor's machine
        if let Some(old_machine) = old_machine.as_ref() {
            assert!(processor.still_on_machine(old_machine));
        }

        let (machine, worker) = Machine::new();
        {
            let machine = machine.clone();
            thread_pool::spawn_box(Box::new(move || {
                abort_on_panic(move || {
                    // initialize/steal task from old machine
                    if let Some(old_machine) = old_machine {
                        loop {
                            if let Steal::Empty = old_machine.stealer.steal_batch(&worker) {
                                break;
                            }
                        }
                        drop(old_machine);
                    }

                    let worker = Rc::new(worker);

                    CURRENT.with(|current| {
                        current.borrow_mut().replace(Current {
                            processor,
                            machine: machine.clone(),
                            worker: worker.clone(),
                        })
                    });
                    defer! {
                      CURRENT.with(|current| drop(current.borrow_mut().take()));
                    }

                    processor.run(&RunContext {
                        system: SYSTEM.get(),
                        machine: &machine,
                        worker: &worker,
                    });
                })
            }));
        }

        machine
    }

    pub fn direct_push(task: Task) -> Result<(), Task> {
        CURRENT.with(|current| match current.borrow().as_ref() {
            Some(Current {
                processor,
                machine,
                worker,
            }) if processor.still_on_machine(machine) => {
                worker.push(task);
                Ok(())
            }
            _ => Err(task),
        })
    }

    pub fn steal(&self, dest: &Worker<Task>) -> Option<Task> {
        // retry until success or empty
        std::iter::repeat_with(|| self.stealer.steal_batch_and_pop(dest))
            .filter(|s| !matches!(s, Steal::Retry))
            .map(|s| match s {
                Steal::Success(task) => Some(task),
                Steal::Empty => None,
                Steal::Retry => unreachable!(), // already filtered
            })
            .next()
            .unwrap()
    }
}

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

/// this wrapper to make sure that no task is discarded
/// when a machine done with a worker
/// and cache the unused worker if necessary
/// because creating worker will allocate some memory
struct WorkerWrapper(Option<Worker<Task>>);

static WORKER_POOL: Lazy<Mutex<VecDeque<Worker<Task>>>> = Lazy::new(|| Mutex::new(VecDeque::new()));

impl WorkerWrapper {
    fn new() -> WorkerWrapper {
        WorkerWrapper(Some(
            WORKER_POOL
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(Worker::new_fifo),
        ))
    }
}

impl std::ops::Deref for WorkerWrapper {
    type Target = Worker<Task>;
    fn deref(&self) -> &Worker<Task> {
        self.0.as_ref().unwrap()
    }
}

impl Drop for WorkerWrapper {
    fn drop(&mut self) {
        let system = SYSTEM.get();
        while let Some(task) = self.pop() {
            system.push(task);
        }
        WORKER_POOL
            .lock()
            .unwrap()
            .push_back(self.0.take().unwrap());
    }
}
