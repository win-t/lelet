use std::cell::RefCell;
use std::hint::unreachable_unchecked;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

use crossbeam_channel::{bounded, Receiver, Sender};
use crossbeam_deque::{Steal, Stealer, Worker};

use crate::thread_pool;
use crate::utils::abort_on_panic;
use crate::utils::monotonic_ms;

use super::sysmon::SYSMON;
use super::Task;

pub struct Processor {
  id: u64,
  valid: AtomicBool,
  is_parking: &'static AtomicBool,
  last_seen: &'static AtomicU64,
  stealer: Stealer<Task>,
  steal_all_sender: Sender<Stealer<Task>>,
  steal_all_receiver: Receiver<Stealer<Task>>,
}

impl std::fmt::Debug for Processor {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.write_str(&format!("<Processor({})>", self.id))
  }
}

impl Drop for Processor {
  fn drop(&mut self) {
    trace!("{:?} is destroyed", self);
  }
}

struct Queue(RefCell<Option<Task>>, Worker<Task>);

impl Queue {
  fn push(&self, task: Task) {
    match self.0.borrow_mut().replace(task) {
      None => {}
      Some(task) => self.1.push(task),
    }
  }

  fn pop(&self) -> Option<Task> {
    match self.0.borrow_mut().take() {
      Some(task) => Some(task),
      None => self.1.pop(),
    }
  }
}

impl std::ops::Deref for Queue {
  type Target = Worker<Task>;
  fn deref(&self) -> &Worker<Task> {
    &self.1
  }
}

thread_local! {
   static QUEUE: RefCell<Option<Rc<Queue>>> = RefCell::new(None);
}

static COUNTER: AtomicU64 = AtomicU64::new(0);

impl Processor {
  pub fn new(is_parking: &'static AtomicBool, last_seen: &'static AtomicU64) -> Arc<Processor> {
    let worker = Worker::new_fifo();
    let (s, r) = bounded(1);
    let processor = Arc::new(Processor {
      id: COUNTER.fetch_add(1, Ordering::Relaxed),
      valid: AtomicBool::new(true),
      is_parking,
      last_seen,
      stealer: worker.stealer(),
      steal_all_sender: s,
      steal_all_receiver: r,
    });
    {
      let processor = processor.clone();
      thread_pool::spawn_box(Box::new(move || abort_on_panic(|| processor.main(worker))));
    }

    trace!("{:?} is created", processor);

    processor
  }

  fn main(&self, worker: Worker<Task>) {
    let queue = Rc::new(Queue(RefCell::new(None), worker));

    // push remaining task to global queue before leaving
    {
      let queue = queue.clone();
      defer! {
        while let Some(task) = queue.pop() {
          SYSMON.push_task(task);
        }
      }
    }

    // set current thread worker queue, and unset it before leaving
    QUEUE.with(|queue_tls| queue_tls.borrow_mut().replace(queue.clone()));
    defer! {
      QUEUE.with(|queue_tls| { queue_tls.borrow_mut().take(); } );
    }

    // Number of runs in a row before the global queue is inspected.
    const MAX_RUNS: u64 = 64;

    // Number of yields before parking the current thread
    const YIELDS: u64 = 2;

    let mut counter: u64 = 0;
    'main: loop {
      macro_rules! run_task {
        ($task:ident) => {{
          $task.run();
          counter += 1;
          if !self.valid.load(Ordering::Relaxed) {
            return;
          }
          continue 'main;
        }};
      }

      macro_rules! pop_global {
        () => {{
          counter = 0;
          match SYSMON.pop_task(&queue) {
            Some(task) => run_task!(task),
            None => {}
          }
        }};
      }

      self.tick();

      if counter >= MAX_RUNS {
        pop_global!();
      }

      if let Some(task) = queue.pop() {
        run_task!(task);
      }

      for i in 0..YIELDS + 1 {
        if i > 0 {
          thread::yield_now();
        }

        pop_global!();

        match SYSMON.steal_task(&queue) {
          Some(task) => run_task!(task),
          None => {}
        }

        match self.steal_all_receiver.try_recv() {
          Ok(stealer) => {
            let steal = stealer.steal_batch_and_pop(&queue);
            match steal {
              Steal::Empty => {} // (*)
              _ => {
                // we are not done with this one yet
                self.steal_all_sender.send(stealer).unwrap();

                match steal {
                  Steal::Success(task) => run_task!(task),
                  Steal::Empty => {}
                  Steal::Retry => unsafe { unreachable_unchecked() }, // (*)
                }
              }
            }
          }
          _ => {}
        }
      }

      self.park();

      pop_global!();
    }
  }

  fn tick(&self) {
    self.last_seen.store(monotonic_ms(), Ordering::Relaxed);
  }

  fn park(&self) {
    self.is_parking.store(true, Ordering::Relaxed);
    defer! {
      self.is_parking.store(false, Ordering::Relaxed);
    }

    SYSMON.park();
  }

  pub fn mark_invalid(&self) {
    self.valid.store(false, Ordering::Relaxed)
  }

  pub fn get_stealer(&self) -> &Stealer<Task> {
    &self.stealer
  }

  pub fn steal_all(&self, s: Stealer<Task>) {
    self.steal_all_sender.send(s).unwrap();
  }
}

pub fn schedule_task(task: Task) {
  QUEUE.with(|queue| match &*queue.borrow() {
    // current thread is a processor
    Some(queue) => queue.push(task),

    // current thread is not a processor
    None => SYSMON.push_task(task),
  });
}
