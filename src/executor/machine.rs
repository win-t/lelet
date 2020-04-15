use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crossbeam_deque::{Steal, Stealer, Worker};

#[cfg(feature = "tracing")]
use log::trace;

use crate::thread_pool;
use crate::utils::abort_on_panic;

use super::processor::Processor;
use super::Task;

pub struct Machine {
  pub id: usize,

  // stealer for the machine, worker part is moved via closure,
  // because Worker is !Send+!Sync
  pub stealer: Stealer<Task>,
}

static MACHINE_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

impl Machine {
  fn new() -> (Machine, Worker<Task>) {
    let worker = Worker::new_fifo();
    let stealer = worker.stealer();
    let machine = Machine {
      id: MACHINE_ID_COUNTER.fetch_add(1, Ordering::Relaxed),
      stealer: stealer,
    };

    #[cfg(feature = "tracing")]
    trace!("{:?} is created", machine);

    (machine, worker)
  }

  pub fn new_and_take_over_processor(
    p: &'static Processor,
    old_stealer: Option<Stealer<Task>>,
  ) -> Arc<Machine> {
    let (machine, worker) = Machine::new();
    let machine = Arc::new(machine);
    {
      let machine = machine.clone();

      // set processor's machine now, so old machine don't hold this processor anymore
      p.set_machine(&machine);

      // drain old machine worker
      if let Some(old_stealer) = old_stealer {
        loop {
          match old_stealer.steal_batch(&worker) {
            Steal::Empty => break,
            _ => (),
          }
        }
      }

      // spawn machine thread
      thread_pool::spawn_box(Box::new(move || {
        abort_on_panic(move || machine.execute_processor(p, worker))
      }));
    }

    machine
  }

  fn execute_processor(&self, p: &'static Processor, worker: Worker<Task>) {
    p.run_on_machine(&self, worker);
  }
}

impl std::fmt::Debug for Machine {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.write_str(&format!("M({})", self.id))
  }
}

#[cfg(feature = "tracing")]
impl Drop for Machine {
  fn drop(&mut self) {
    trace!("{:?} is destroyed", self);
  }
}
