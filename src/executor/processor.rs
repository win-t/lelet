use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crossbeam_channel::{bounded, Receiver, Sender};
use crossbeam_deque::{Injector, Steal, Worker};
use crossbeam_utils::Backoff;

#[cfg(feature = "tracing")]
use log::trace;

use crate::utils::monotonic_ms;

use super::machine::Machine;
use super::system::SYSTEM;
use super::Task;

pub struct Processor {
  pub index: usize,

  // current machine that hold the processor
  machine_id: AtomicUsize,

  // for blocking detection
  last_seen: AtomicU64,

  // global queue dedicated to this processor
  injector: Injector<Task>,

  // to wakeup sleeping processor
  wake_up: Sender<()>,
  wake_up_notif: Receiver<()>,
}

impl Processor {
  pub fn new(index: usize) -> Processor {
    // channel with buffer size 1 is enough to give notification
    // when new task is arrive
    let (wake_up, wake_up_notif) = bounded(1);

    let processor = Processor {
      index,
      machine_id: AtomicUsize::new(usize::MAX),
      last_seen: AtomicU64::new(u64::MAX),
      injector: Injector::new(),
      wake_up,
      wake_up_notif,
    };

    #[cfg(feature = "tracing")]
    trace!("{:?} is created", processor);

    processor
  }

  pub fn run_on_machine(&'static self, machine: &Machine, worker: Worker<Task>) {
    #[cfg(feature = "tracing")]
    trace!("{:?} is running on {:?}", self, machine);

    // Number of runs in a row before the global queue is inspected.
    const MAX_RUNS: u64 = 64;

    let mut run_counter = 0;

    'main: loop {
      macro_rules! run_task {
        ($task:ident) => {{
          // update the tag, so this task will be push to this processor again
          $task.tag().set_schedule_index_hint(self.index);

          #[cfg(feature = "tracing")]
          let task_rep = format!("{:?}", $task.tag());

          #[cfg(feature = "tracing")]
          trace!("{} is running on {:?}", task_rep, self);

          // help sysmon before doing real task
          SYSTEM.assist();

          // always assume the task is blocking
          self.mark_blocking();
          {
            $task.run();

            // if the processor is assigned to another machine, just exit
            if !self.still_on_machine(machine) {
              #[cfg(feature = "tracing")]
              trace!(
                "{:?} is no longer on {:?}, it was blocking on {}",
                self,
                machine,
                task_rep,
              );
              return;
            }
          }
          self.mark_nonblocking();

          run_counter += 1;
          continue 'main;
        }};
      }

      macro_rules! get_tasks {
        () => {{
          run_counter = 0;
          match SYSTEM.pop(self.index, &worker) {
            Some(task) => run_task!(task),
            None => {}
          }
        }};
      }

      if run_counter > MAX_RUNS {
        get_tasks!();
      }

      // run all task in the worker
      if let Some(task) = worker.pop() {
        run_task!(task);
      }

      // at this point, the worker is empty

      // 1. pop from global queue
      get_tasks!();

      // 2. steal from others
      match SYSTEM.steal(&worker) {
        Some(task) => run_task!(task),
        None => {}
      }

      // 3.a. no more task for now, just sleep until waked up
      self.sleep();

      // 3.b. just waked up, pop from global queue
      get_tasks!();
    }
  }

  pub fn send_wake_up(&'static self) -> bool {
    match self.wake_up.try_send(()) {
      Ok(_) => true,
      Err(_) => false,
    }
  }

  fn sleep(&'static self) {
    let backoff = Backoff::new();
    loop {
      match self.wake_up_notif.try_recv() {
        Ok(()) => return,
        Err(_) => {
          if backoff.is_completed() {
            #[cfg(feature = "tracing")]
            trace!("{:?} entering sleep", self);

            #[cfg(feature = "tracing")]
            defer! {
              trace!("{:?} leaving sleep", self);
            }

            self.wake_up_notif.recv().unwrap();
            return;
          } else {
            backoff.snooze();
          }
        }
      }
    }
  }

  fn mark_blocking(&'static self) {
    self.last_seen.store(monotonic_ms(), Ordering::Relaxed);
  }

  fn mark_nonblocking(&'static self) {
    self.last_seen.store(u64::MAX, Ordering::Relaxed);
  }

  pub fn get_last_seen(&'static self) -> u64 {
    self.last_seen.load(Ordering::Relaxed)
  }

  pub fn push(&'static self, t: Task) -> bool {
    self.injector.push(t);
    self.send_wake_up()
  }

  pub fn pop(&'static self, dest: &Worker<Task>) -> Option<Task> {
    // steal until success or empty
    std::iter::repeat_with(|| self.injector.steal_batch_and_pop(dest))
      .filter(|s| !matches!(s, Steal::Retry))
      .map(|s| match s {
        Steal::Success(task) => Some(task),
        Steal::Empty => None,
        Steal::Retry => None,
      })
      .nth(0)
      .unwrap()
  }

  pub fn set_machine(&'static self, machine: &Machine) {
    self.machine_id.store(machine.id, Ordering::Relaxed);
    self.mark_nonblocking();
  }

  pub fn still_on_machine(&'static self, machine: &Machine) -> bool {
    self.machine_id.load(Ordering::Relaxed) == machine.id
  }
}

impl std::fmt::Debug for Processor {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.write_str(&format!("P({})", self.index))
  }
}
