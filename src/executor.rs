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
use log::trace;
use once_cell::sync::Lazy;

use crate::thread_pool;
use crate::utils::abort_on_panic;
use crate::utils::monotonic_ms as now_ms;

// how long a processor considered to be blocking
const BLOCKING_THRESHOLD: Duration = Duration::from_millis(100);

const INVALID_ID: usize = usize::MAX;

struct TaskTag {
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
}

struct Processor {
  id: usize,

  // current machine that hold the processor
  machine_id: AtomicUsize,

  // for blocking detection
  last_seen: AtomicU64,
  sleeping: AtomicBool,

  // global queue dedicated to this processor
  injector: Injector<Task>,
  wake_up: Sender<()>,
  wake_up_notif: Receiver<()>,
}

struct Machine {
  id: usize,

  // stealer for the machine
  stealer: Stealer<Task>,

  // we inherit this from old machine when we replace them
  old_machine_stealer: Stealer<Task>,
}

static EXECUTOR: Lazy<Executor> = Lazy::new(|| {
  // the number is processor is fix
  let num_cpus = std::cmp::max(1, num_cpus::get());

  let mut processors = Vec::with_capacity(num_cpus);
  for id in 0..num_cpus {
    // channel with buffer size 1 is enough to give notification
    // when new task is arrive
    let (wake_up, wake_up_notif) = bounded(1);

    processors.push(Processor {
      id,
      machine_id: AtomicUsize::new(INVALID_ID),
      last_seen: AtomicU64::new(0),
      sleeping: AtomicBool::new(true),
      injector: Injector::new(),
      wake_up,
      wake_up_notif,
    });
  }

  let empty_worker = Worker::new_fifo();

  let mut machines = Vec::with_capacity(num_cpus);
  for index in 0..num_cpus {
    machines.push(Machine::create_with_processor(
      &processors[index],
      empty_worker.stealer(),
    ));
  }

  thread::spawn(move || abort_on_panic(move || EXECUTOR.sysmon_main()));

  Executor {
    processors,
    processor_push_index_hint: AtomicUsize::new(0),

    machines,
    machine_steal_index_hint: AtomicUsize::new(0),
  }
});

static TASK_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

static MACHINE_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

impl TaskTag {
  fn new() -> TaskTag {
    TaskTag {
      id: TASK_ID_COUNTER.fetch_add(1, Ordering::Relaxed),
      schedule_hint: AtomicUsize::new(INVALID_ID),
    }
  }
}

impl std::fmt::Debug for TaskTag {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.write_str(&format!("T({})", self.id))
  }
}

impl Executor {
  fn sysmon_main(&self) {
    for id in 0..self.processors.len() {
      let p = &self.processors[id];
      assert_eq!(id, p.id);
    }

    loop {
      thread::sleep(BLOCKING_THRESHOLD);

      let must_seen_at = now_ms() - BLOCKING_THRESHOLD.as_millis() as u64;

      for index in 0..self.processors.len() {
        let p = &self.processors[index];

        if p.is_sleeping() || must_seen_at <= p.get_last_seen() {
          continue;
        }

        let current: &Arc<Machine> = &self.machines[index];
        let new: &Arc<Machine> = &Machine::create_with_processor(p, current.stealer.clone());

        trace!(
          "{:?} is not responding while on {:?}, replacing with {:?}",
          p,
          current,
          new
        );

        // force swap on immutable list, atomic update the pointer in the list
        // this is safe because:
        // 1) Arc have same size with *mut ()
        // 2) Arc counter is not touched in the process, no drop and use-after-free
        unsafe {
          // #1
          if false {
            // do not run this code, this is for compile time checking
            // https://internals.rust-lang.org/t/compile-time-assert/6751/2
            transmute::<*mut (), Arc<Machine>>(std::ptr::null_mut());
          }

          // #2
          let current = transmute::<&Arc<Machine>, &AtomicPtr<()>>(current);
          let new = transmute::<&Arc<Machine>, &AtomicPtr<()>>(&new);
          let old = current.swap(new.load(Ordering::SeqCst), Ordering::SeqCst);
          new.store(old, Ordering::SeqCst);
        }
      }
    }
  }

  fn push(&self, t: Task) {
    let mut id = t.tag().schedule_hint.load(Ordering::Relaxed);

    // if the task is not have prefered processor, we pick one
    if id > self.processors.len() {
      id = self.processor_push_index_hint.load(Ordering::Relaxed);

      // rotate the index, for fair load
      self
        .processor_push_index_hint
        .store((id + 1) % self.processors.len(), Ordering::Relaxed);
    }

    self.processors[id].push(t);
  }

  fn pop(&self, index: usize, dest: &Worker<Task>) -> Option<Task> {
    // pop from global queue that dedicated to processor[index],
    // if no task found, proceed to another global queue
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
      .map(|(i, m)| {
        (
          i,
          // steal until success or empty
          std::iter::repeat_with(|| m.stealer.steal_batch_and_pop(dest))
            .filter(|s| !matches!(s, Steal::Retry)) // (*)
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
      .map(|(i, s)| {
        self
          .machine_steal_index_hint
          .store((m + i) % self.machines.len(), Ordering::Relaxed);
        s
      })
      .flatten()
  }
}

impl Processor {
  fn is_sleeping(&self) -> bool {
    self.sleeping.load(Ordering::Relaxed)
  }

  fn sleep(&self) {
    trace!("{:?} entering sleep", self);
    self.sleeping.store(true, Ordering::Relaxed);
    defer! {
      trace!("{:?} leaving sleep", self);
      self.sleeping.store(false, Ordering::Relaxed);
    }

    self.wake_up_notif.recv().unwrap();
  }

  fn get_last_seen(&self) -> u64 {
    self.last_seen.load(Ordering::Relaxed)
  }

  fn tick(&self) {
    self.last_seen.store(now_ms(), Ordering::Relaxed);
  }

  fn push(&self, t: Task) {
    self.injector.push(t);
    let _ = self.wake_up.try_send(());
  }

  fn pop(&self, dest: &Worker<Task>) -> Option<Task> {
    // steal until success or empty
    std::iter::repeat_with(|| self.injector.steal_batch_and_pop(dest))
      .filter(|s| !matches!(s, Steal::Retry)) // (*)
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
  fn create_with_processor(p: &Processor, old_machine_stealer: Stealer<Task>) -> Arc<Machine> {
    let id = MACHINE_ID_COUNTER.fetch_add(1, Ordering::Relaxed);

    // take over the processor
    p.machine_id.store(id, Ordering::Relaxed);

    let worker = Worker::new_fifo();
    let stealer = worker.stealer();
    let machine = Arc::new(Machine {
      id,
      stealer: stealer,
      old_machine_stealer,
    });
    {
      let machine = machine.clone();

      // this is safe because processor is never get dropped after created
      let processor = unsafe { transmute(p) };

      thread_pool::spawn_box(Box::new(move || {
        abort_on_panic(move || machine.main(worker, processor))
      }));
    }

    machine
  }

  fn main(&self, worker: Worker<Task>, processor: &Processor) {
    trace!("{:?} now is running on {:?}", processor, self);

    // push remaining task to global queue before leaving
    defer! {
      while let Some(task) = worker.pop() {
        processor.push(task);
      }
    }

    // Number of runs in a row before the global queue is inspected.
    const MAX_RUNS: u64 = 64;

    // Number of yields before sleeping
    const YIELDS: u64 = 2;

    let mut run_counter = 0;

    // try to inherit initial task from old machine
    let _ = self.old_machine_stealer.steal_batch(&worker);

    'main: loop {
      macro_rules! run_task {
        ($task:ident) => {{
          // update the tag, so this task will be push to this processor again
          $task
            .tag()
            .schedule_hint
            .store(processor.id, Ordering::Relaxed);

          trace!("{:?} is running on {:?}", $task.tag(), processor);
          $task.run();

          // if this machine don't hold the processor anymore
          // sysmon detect this machine was blocking and already replaced it with another machine
          if processor.machine_id.load(Ordering::Relaxed) != self.id {
            trace!("{:?} is no longer holding {:?}", self, processor);
            return;
          }

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

      processor.tick();

      if run_counter > MAX_RUNS {
        get_tasks!();
      }

      if let Some(task) = worker.pop() {
        run_task!(task);
      }

      match self.old_machine_stealer.steal_batch_and_pop(&worker) {
        Steal::Success(task) => run_task!(task),
        _ => {}
      }

      for i in 0..YIELDS + 1 {
        if i > 0 {
          thread::yield_now();
        }

        get_tasks!();

        match EXECUTOR.steal(&worker) {
          Some(task) => run_task!(task),
          None => {}
        }
      }

      processor.sleep();
      get_tasks!();
    }
  }
}

impl std::fmt::Debug for Machine {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.write_str(&format!("M({})", self.id))
  }
}

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
  let tag = TaskTag::new();
  trace!("{:?} is created", tag);
  let (task, _) = async_task::spawn(f, |t| EXECUTOR.push(t), tag);
  task.schedule();
}
