// extra doc:
// Inspired by golang runtime, see https://golang.org/src/runtime/proc.go
// so understand some terminology like machine and processor will help you
// understand this code.

use std::future::Future;
use std::hint::unreachable_unchecked;
use std::mem::transmute;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crossbeam_channel::{bounded, Receiver, Sender};
use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use crossbeam_utils::Backoff;
use once_cell::sync::Lazy;

#[cfg(feature = "tracing")]
use log::trace;

use crate::thread_pool;
use crate::utils::abort_on_panic;
use crate::utils::monotonic_ms;

// how long a processor considered to be blocking
const BLOCKING_THRESHOLD: Duration = Duration::from_millis(10);

// interval of sysmon check, it is okay to be higher than BLOCKING_THRESHOLD
// because idle processor will assist the sysmon
const SYSMON_CHECK_INTERVAL: Duration = Duration::from_millis(100);

struct TaskTag {
  #[cfg(feature = "tracing")]
  id: usize,

  schedule_hint: AtomicUsize,
}

type Task = async_task::Task<TaskTag>;

// singleton: EXECUTOR
struct Executor {
  // all processors
  processors: Vec<Processor>,

  // used to select which processor got the task
  processor_push_index_hint: AtomicUsize,

  // machine[i] is currently running processor[i]
  machines: Vec<Arc<Machine>>,

  // used to select which machine to be stealed first
  machine_steal_index_hint: AtomicUsize,

  // to wakeup sleeping processor
  wake_up: Sender<()>,
  wake_up_notif: Receiver<()>,

  // for sysmon assist
  check_running: AtomicBool,
  check_next: AtomicU64,
}

struct Processor {
  id: usize,

  // current machine that hold the processor
  machine_id: AtomicUsize,

  // for blocking detection
  last_seen: AtomicU64,

  // global queue dedicated to this processor
  injector: Injector<Task>,
}

struct Machine {
  id: usize,

  // stealer for the machine
  stealer: Stealer<Task>,

  // we inherit this from old machine when we replace them
  inherit: Stealer<Task>,
}

static EXECUTOR: Lazy<Executor> = Lazy::new(|| {
  // the number is processor is fix
  let num_cpus = std::cmp::max(1, num_cpus::get());

  let mut processors = Vec::with_capacity(num_cpus);
  for id in 0..num_cpus {
    let p = Processor {
      id,
      machine_id: AtomicUsize::new(0),
      last_seen: AtomicU64::new(0),
      injector: Injector::new(),
    };

    #[cfg(feature = "tracing")]
    trace!("{:?} is created", p);

    processors.push(p);
  }

  let empty_worker = Worker::new_fifo();
  let mut machines = Vec::with_capacity(num_cpus);
  for index in 0..num_cpus {
    machines.push(Machine::move_processor_to_new_machine(
      &processors[index],
      empty_worker.stealer(),
    ));
  }

  // just to make sure,
  // that index between processor and machine is consistent
  for index in 0..processors.len() {
    let p = &processors[index];
    assert_eq!(index, p.id);
    assert_eq!(p.machine_id.load(Ordering::Relaxed), machines[index].id,);
  }

  thread::spawn(move || abort_on_panic(move || EXECUTOR.sysmon_main()));

  // channel with buffer size 1 is enough to give notification
  // when new task is arrive
  let (wake_up, wake_up_notif) = bounded(1);

  Executor {
    processors,
    processor_push_index_hint: AtomicUsize::new(0),

    machines,
    machine_steal_index_hint: AtomicUsize::new(0),

    wake_up,
    wake_up_notif,

    check_running: AtomicBool::new(false),
    check_next: AtomicU64::new(0),
  }
});

#[cfg(feature = "tracing")]
static TASK_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

static MACHINE_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

impl TaskTag {
  fn new() -> TaskTag {
    let tag = TaskTag {
      #[cfg(feature = "tracing")]
      id: TASK_ID_COUNTER.fetch_add(1, Ordering::Relaxed),

      schedule_hint: AtomicUsize::new(usize::MAX),
    };

    #[cfg(feature = "tracing")]
    trace!("{} is created", TaskTag::string_rep(tag.id));

    tag
  }

  #[cfg(feature = "tracing")]
  fn string_rep(id: usize) -> String {
    format!("T({})", id)
  }
}

#[cfg(feature = "tracing")]
impl Drop for TaskTag {
  fn drop(&mut self) {
    trace!("{} is destroyed", TaskTag::string_rep(self.id));
  }
}

impl Executor {
  fn sysmon_check(&self) {
    let monotonic_ms = monotonic_ms();

    if monotonic_ms < self.check_next.load(Ordering::Relaxed) {
      // it is not time yet
      return;
    }

    if self
      .check_running
      .compare_and_swap(false, true, Ordering::Relaxed)
      == true
    {
      // check already running on other thread
      // only one check allowed at a time
      return;
    }

    defer! {
      self.check_running.store(false, Ordering::Relaxed)
    }

    if monotonic_ms < BLOCKING_THRESHOLD.as_millis() as u64 {
      return;
    }

    let must_seen_at = monotonic_ms - BLOCKING_THRESHOLD.as_millis() as u64;

    for index in 0..self.processors.len() {
      let p = &self.processors[index];

      if must_seen_at <= p.get_last_seen() {
        continue;
      }

      let current: &Arc<Machine> = &self.machines[index];
      let new: &Arc<Machine> = &Machine::move_processor_to_new_machine(p, current.stealer.clone());

      #[cfg(feature = "tracing")]
      trace!(
        "{:?} is blocking while running on {:?}, replacing with {:?}",
        p,
        current,
        new
      );

      // force swap on immutable list, atomic update the Arc/pointer in the list
      // this is safe because:
      // 1) Arc have same size with *mut ()
      // 2) Arc counter is not touched when swaping
      // 3) only one thread is doing this (guarded by self.check_running)
      unsafe {
        // #1
        if false {
          // do not run this code, this is for compile time checking only
          // transmute null_mut() to Arc will surely crashing the program
          //
          // https://internals.rust-lang.org/t/compile-time-assert/6751/2
          transmute::<*mut (), Arc<Machine>>(std::ptr::null_mut());
        }

        // #2
        let current = transmute::<&Arc<Machine>, &AtomicPtr<()>>(current);
        let new = transmute::<&Arc<Machine>, &AtomicPtr<()>>(&new);
        let old = current.swap(new.load(Ordering::Relaxed), Ordering::Relaxed);
        new.store(old, Ordering::Relaxed);
      }
    }

    self.check_next.store(
      self
        .processors
        .iter()
        .map(|p| p.get_last_seen())
        .chain(std::iter::once(monotonic_ms))
        .min()
        .unwrap()
        + BLOCKING_THRESHOLD.as_millis() as u64,
      Ordering::Relaxed,
    );
  }

  fn sysmon_main(&self) {
    loop {
      thread::sleep(SYSMON_CHECK_INTERVAL);
      self.sysmon_check();
    }
  }

  fn sysmon_assist(&self) {
    self.sysmon_check();
  }

  fn push(&self, t: Task) {
    let mut index = t.tag().schedule_hint.load(Ordering::Relaxed);

    // if the task does not have prefered processor, we pick one
    if index > self.processors.len() {
      index = self.processor_push_index_hint.load(Ordering::Relaxed);

      // rotate the index, for fair load
      self
        .processor_push_index_hint
        .store((index + 1) % self.processors.len(), Ordering::Relaxed);
    }

    self.processors[index].push(t);
  }

  fn pop(&self, index: usize, dest: &Worker<Task>) -> Option<Task> {
    // pop from global queue that dedicated to processor[index],
    // if None, proceed to another global queue
    let (l, r) = self.processors.split_at(index);
    r.iter()
      .chain(l.iter())
      .map(|p| p.pop(dest))
      .filter(|s| matches!(s, Some(_)))
      .nth(0)
      .flatten()
  }

  fn steal(&self, dest: &Worker<Task>) -> Option<Task> {
    let m = self.machine_steal_index_hint.load(Ordering::Relaxed);
    let (l, r) = self.machines.split_at(m);
    (1..)
      .zip(r.iter().chain(l.iter()))
      .map(|(hint_add, m)| {
        (
          hint_add,
          // steal until success or empty
          std::iter::repeat_with(|| m.stealer.steal_batch_and_pop(dest))
            .filter(|s| !matches!(s, Steal::Retry)) // not Steal::Retry (*)
            .map(|s| match s {
              Steal::Success(task) => Some(task),
              Steal::Empty => None,
              Steal::Retry => unsafe { unreachable_unchecked() }, // (*)
            })
            .nth(0)
            .unwrap(),
        )
      })
      .filter(|(_, s)| matches!(s, Some(_)))
      .nth(0)
      .map(|(hint_add, s)| {
        self
          .machine_steal_index_hint
          .store((m + hint_add) % self.machines.len(), Ordering::Relaxed);
        s
      })
      .flatten()
  }
}

impl Processor {
  fn sleep(&self) {
    let backoff = Backoff::new();
    loop {
      match EXECUTOR.wake_up_notif.try_recv() {
        Ok(()) => return,
        Err(_) => {
          if backoff.is_completed() {
            #[cfg(feature = "tracing")]
            trace!("{:?} entering sleep", self);

            #[cfg(feature = "tracing")]
            defer! {
              trace!("{:?} leaving sleep", self);
            }

            EXECUTOR.wake_up_notif.recv().unwrap();
            return;
          } else {
            backoff.snooze();
          }
        }
      }
    }
  }

  fn mark_blocking(&self) {
    self.last_seen.store(monotonic_ms(), Ordering::Relaxed);
  }

  fn mark_nonblocking(&self) {
    self.last_seen.store(u64::MAX, Ordering::Relaxed);
  }

  fn get_last_seen(&self) -> u64 {
    self.last_seen.load(Ordering::Relaxed)
  }

  fn push(&self, t: Task) {
    self.injector.push(t);

    // wake up all processor,
    // in case current processor is busy,
    // others need to run (steal) it
    let _ = EXECUTOR.wake_up.try_send(());
  }

  fn pop(&self, dest: &Worker<Task>) -> Option<Task> {
    // steal until success or empty
    std::iter::repeat_with(|| self.injector.steal_batch_and_pop(dest))
      .filter(|s| !matches!(s, Steal::Retry)) // not Steal::Retry (*)
      .map(|s| match s {
        Steal::Success(task) => Some(task),
        Steal::Empty => None,
        Steal::Retry => unsafe { unreachable_unchecked() }, // (*)
      })
      .nth(0)
      .unwrap()
  }
}

impl std::fmt::Debug for Processor {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.write_str(&format!("P({})", self.id))
  }
}

impl Machine {
  fn move_processor_to_new_machine(p: &Processor, inherit: Stealer<Task>) -> Arc<Machine> {
    let id = MACHINE_ID_COUNTER.fetch_add(1, Ordering::Relaxed);

    // take over the processor
    p.machine_id.store(id, Ordering::Relaxed);
    p.mark_nonblocking();

    let worker = Worker::new_fifo();
    let stealer = worker.stealer();
    let machine = Arc::new(Machine {
      id,
      stealer: stealer,
      inherit,
    });

    #[cfg(feature = "tracing")]
    trace!("{:?} is created", machine);

    {
      let machine = machine.clone();

      // this is safe because processor is never get dropped after created,
      // it can be assume that it has static lifetime
      let processor: &'static Processor = unsafe { transmute(p) };

      thread_pool::spawn_box(Box::new(move || {
        abort_on_panic(move || machine.main(worker, processor))
      }));
    }

    machine
  }

  fn main(&self, worker: Worker<Task>, processor: &Processor) {
    #[cfg(feature = "tracing")]
    trace!("{:?} is running on {:?}", processor, self);

    // initial task from old machine
    loop {
      match self.inherit.steal_batch(&worker) {
        Steal::Retry => continue,
        _ => break,
      }
    }

    // Number of runs in a row before the global queue is inspected.
    const MAX_RUNS: u64 = 64;

    let mut run_counter = 0;

    'main: loop {
      macro_rules! run_task {
        ($task:ident) => {{
          // update the tag, so this task will be push to this processor again
          $task
            .tag()
            .schedule_hint
            .store(processor.id, Ordering::Relaxed);

          #[cfg(feature = "tracing")]
          let task_id = $task.tag().id;

          #[cfg(feature = "tracing")]
          trace!(
            "{} is running on {:?}",
            TaskTag::string_rep(task_id),
            processor
          );

          // help sysmon before doing real task
          EXECUTOR.sysmon_assist();

          // always assume the task is blocking
          processor.mark_blocking();
          {
            $task.run();

            // it is very crucial that we must exit this machine now when other machine holding
            // the processor, so we don't mess up with the processor state
            if processor.machine_id.load(Ordering::Relaxed) != self.id {
              #[cfg(feature = "tracing")]
              trace!(
                "{:?} is no longer holding {:?}, blocking when executing {}",
                self,
                processor,
                TaskTag::string_rep(task_id),
              );
              return;
            }
          }
          processor.mark_nonblocking();

          run_counter += 1;
          continue 'main;
        }};
      }

      macro_rules! get_tasks {
        () => {{
          run_counter = 0;
          match EXECUTOR.pop(processor.id, &worker) {
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

      // 1. steal from old machine (in case some one accidentally push to it)
      match self.inherit.steal_batch_and_pop(&worker) {
        Steal::Success(task) => run_task!(task),
        _ => {}
      }

      // 2. pop from global queue
      get_tasks!();

      // 3. steal from others
      match EXECUTOR.steal(&worker) {
        Some(task) => run_task!(task),
        None => {}
      }

      // 4.a. no more task for now, just sleep until waked up
      processor.sleep();

      // 4.b. just waked up, pop from global queue
      get_tasks!();
    }
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

/// Run the task.
///
/// It's okay to do blocking operation in the task, the executor will detect
/// this and scale the pool.
pub fn spawn<F: Future<Output = ()> + Send + 'static>(f: F) {
  let (task, _) = async_task::spawn(f, |t| EXECUTOR.push(t), TaskTag::new());
  task.schedule();
}
