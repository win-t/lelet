use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crossbeam_channel::{bounded, Receiver, Sender};
use crossbeam_deque::{Injector, Steal, Worker};
use crossbeam_utils::{Backoff, CachePadded};

#[cfg(feature = "tracing")]
use log::trace;

use crate::utils::monotonic_ms;

use super::machine::Machine;
use super::system::SYSTEM;
use super::Task;

// Processor is the one who run the task
pub struct Processor {
  pub index: usize,

  // current machine that hold the processor
  machine_id: CachePadded<AtomicUsize>,

  // for blocking detection
  last_seen: AtomicU64,

  // global queue dedicated to this processor
  injector: Injector<Task>,
  injector_notif: Sender<()>,
  injector_notif_recv: Receiver<()>,
}

impl Processor {
  pub fn new(index: usize) -> Processor {
    // channel with buffer size 1 to not miss a notification
    let (injector_notif, injector_notif_recv) = bounded(1);

    #[allow(clippy::let_and_return)]
    let processor = Processor {
      index,

      last_seen: AtomicU64::new(u64::MAX),

      injector: Injector::new(),
      injector_notif,
      injector_notif_recv,

      machine_id: CachePadded::new(AtomicUsize::new(usize::MAX)), // to be initialized later
    };

    #[cfg(feature = "tracing")]
    trace!("{:?} is created", processor);

    processor
  }

  #[inline]
  #[allow(clippy::cognitive_complexity)]
  pub fn run_on_machine(&self, machine: &Machine, worker: &Worker<Task>) {
    #[cfg(feature = "tracing")]
    crate::thread_pool::THREAD_ID.with(|tid| {
      trace!("{:?} is now running on {:?} on {:?}", self, machine, tid);
    });

    // Number of runs in a row before the global queue is inspected.
    const MAX_RUNS: u64 = 64;
    let mut run_counter = 0;

    let backoff = Backoff::new();

    #[cfg(feature = "tracing")]
    let mut last_task_rep = String::new();

    'main: loop {
      macro_rules! run_task {
        ($task:ident) => {{
          self.mark_blocking(machine);
          {
            // there is possibility that (*) is skipped because of race condition
            if self.still_on_machine(machine) {
              #[cfg(feature = "tracing")]
              {
                last_task_rep = format!("{:?}", $task.tag());
                trace!("{} is running on {:?}", last_task_rep, self);
              }

              // update the tag, so this task will be push to this processor again
              $task.tag().set_schedule_index_hint(self.index);
              $task.run();

              #[cfg(feature = "tracing")]
              {
                trace!("{} is done running on {:?}", last_task_rep, self);
              }
            } else {
              // put it back in global queue
              SYSTEM.push($task);
            }

            // (*) if the processor is running in another machine, just exit
            if !self.still_on_machine(machine) {
              #[cfg(feature = "tracing")]
              trace!(
                "{} was blocking, so {:?} is no longer on {:?}",
                last_task_rep,
                self,
                machine,
              );
              return;
            }
          }
          self.mark_nonblocking(machine);

          run_counter += 1;
          continue 'main;
        }};
      }

      macro_rules! get_tasks {
        () => {{
          run_counter = 0;
          let _ = self.injector_notif_recv.try_recv(); // flush the notification channel
          if let Some(task) = SYSTEM.pop(self.index, worker) {
            run_task!(task);
          }
        }};
      }

      if run_counter >= MAX_RUNS {
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
      if let Some(task) = SYSTEM.steal(&worker) {
        run_task!(task);
      }

      // 3.a. no more task for now, just sleep
      self.sleep(&backoff);

      // 3.b. after sleep, pop from global queue
      get_tasks!();
    }
  }

  #[inline]
  pub fn still_on_machine(&self, machine: &Machine) -> bool {
    self.machine_id.load(Ordering::Relaxed) == machine.id
  }

  #[inline]
  fn mark_blocking(&self, machine: &Machine) {
    // only alive machine can alter last_seen value
    if !self.still_on_machine(machine) {
      return;
    }
    self.last_seen.store(monotonic_ms(), Ordering::Relaxed);
    SYSTEM.sysmon_wake_up();
  }

  #[inline]
  fn mark_nonblocking(&self, machine: &Machine) {
    // only alive machine can alter last_seen value
    if !self.still_on_machine(machine) {
      return;
    }
    self.last_seen.store(u64::MAX, Ordering::Relaxed);
  }

  #[inline]
  pub fn set_machine(&self, machine: &Machine) {
    self.machine_id.store(machine.id, Ordering::Relaxed);

    // mark non blocking on fresh machine
    self.mark_nonblocking(machine);
  }

  #[inline]
  pub fn get_last_seen(&self) -> u64 {
    self.last_seen.load(Ordering::Relaxed)
  }

  #[inline]
  fn sleep(&self, backoff: &Backoff) {
    if backoff.is_completed() {
      #[cfg(feature = "tracing")]
      trace!("{:?} entering sleep", self);

      #[cfg(feature = "tracing")]
      defer! {
        trace!("{:?} leaving sleep", self);
      }

      self.injector_notif_recv.recv().unwrap();
      backoff.reset();
    } else {
      backoff.snooze();
    }
  }

  #[inline]
  pub fn wake_up(&self) -> bool {
    self.injector_notif.try_send(()).is_ok()
  }

  #[inline]
  pub fn push_then_wake_up(&self, t: Task) -> bool {
    self.injector.push(t);
    self.wake_up()
  }

  #[inline]
  pub fn pop(&self, dest: &Worker<Task>) -> Option<Task> {
    // steal until success or empty
    std::iter::repeat_with(|| self.injector.steal_batch_and_pop(dest))
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

impl std::fmt::Debug for Processor {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.write_str(&format!("Processor({})", self.index))
  }
}
