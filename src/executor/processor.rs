use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

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

  pub fn run_on_machine(
    &'static self,
    current_machine: &Machine,
    worker: Worker<Task>,
    old_machine: Option<Arc<Machine>>,
  ) {
    #[cfg(feature = "tracing")]
    crate::thread_pool::THREAD_ID.with(|tid| {
      let prev = match old_machine.as_ref() {
        None => "None".into(),
        Some(machine) => format!("{:?}", machine),
      };
      trace!(
        "{:?} is now running on {:?} (prev is {}) on {:?}",
        self,
        current_machine,
        prev,
        tid,
      );
    });

    // Number of runs in a row before the global queue is inspected.
    const MAX_RUNS: u64 = 64;

    let mut run_counter = 0;

    let backoff = Backoff::new();
    'main: loop {
      macro_rules! run_task {
        ($task:ident) => {{
          // update the tag, so this task will be push to this processor again
          $task.tag().set_schedule_index_hint(self.index);

          #[cfg(feature = "tracing")]
          let task_rep = format!("{:?}", $task.tag());

          // help sysmon before doing real task
          SYSTEM.sysmon_assist();

          // always assume the task is blocking
          self.mark_blocking(current_machine);
          {
            // there is possibility that (*) is skipped because of race condition
            if self.still_on_machine(current_machine) {
              #[cfg(feature = "tracing")]
              trace!("{} is running on {:?}", task_rep, self);
              $task.run();
              #[cfg(feature = "tracing")]
              trace!("{} is done running on {:?}", task_rep, self);
            } else {
              // push the thak back, it will be stealed later
              worker.push($task);
            }

            // (*) if the processor is assigned to another machine, just exit
            if !self.still_on_machine(current_machine) {
              #[cfg(feature = "tracing")]
              trace!(
                "{} was blocking, so {:?} is no longer on {:?}",
                task_rep,
                self,
                current_machine,
              );
              return;
            }
          }
          self.mark_nonblocking(current_machine);

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

      // 1. steal from old machine
      if let Some(old_machine) = old_machine.as_ref() {
        'inner: loop {
          match old_machine.stealer.steal_batch_and_pop(&worker) {
            Steal::Success(task) => run_task!(task),
            Steal::Empty => break 'inner,
            Steal::Retry => {}
          }
        }
      }

      // 2. pop from global queue
      get_tasks!();

      // 3. steal from others
      match SYSTEM.steal(&worker) {
        Some(task) => run_task!(task),
        None => {}
      }

      // 4.a. no more task for now, just sleep until waked up
      {
        match self.wake_up_notif.try_recv() {
          Ok(()) => continue 'main,
          Err(_) => {
            if backoff.is_completed() {
              #[cfg(feature = "tracing")]
              trace!("{:?} entering sleep", self);

              #[cfg(feature = "tracing")]
              defer! {
                trace!("{:?} leaving sleep", self);
              }

              self.wake_up_notif.recv().unwrap();
              backoff.reset();
              continue 'main;
            } else {
              backoff.snooze();
            }
          }
        }
      }

      // 4.b. just waked up, pop from global queue
      get_tasks!();
    }
  }

  fn mark_blocking(&'static self, machine: &Machine) {
    // only alive machine can alter last_seen value
    if !self.still_on_machine(machine) {
      return;
    }
    self.last_seen.store(monotonic_ms(), Ordering::Relaxed);
  }

  fn mark_nonblocking(&'static self, machine: &Machine) {
    // only alive machine can alter last_seen value
    if !self.still_on_machine(machine) {
      return;
    }
    self.last_seen.store(u64::MAX, Ordering::Relaxed);
  }

  pub fn get_last_seen(&'static self) -> u64 {
    self.last_seen.load(Ordering::Relaxed)
  }

  pub fn send_wake_up(&'static self) -> bool {
    match self.wake_up.try_send(()) {
      Ok(_) => true,
      Err(_) => false,
    }
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

    // mark non blocking on fresh machine
    self.mark_nonblocking(machine);
  }

  pub fn still_on_machine(&'static self, machine: &Machine) -> bool {
    self.machine_id.load(Ordering::Relaxed) == machine.id
  }
}

impl std::fmt::Debug for Processor {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.write_str(&format!("Processor({})", self.index))
  }
}
