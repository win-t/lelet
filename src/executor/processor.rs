use std::hint::unreachable_unchecked;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

use crossbeam_channel::{unbounded, Receiver, Sender};
use crossbeam_deque::{Steal, Stealer, Worker};

use crate::thread_pool;
use crate::utils::abort_on_panic;
use crate::utils::monotonic_ms;

use super::Task;
use super::SYSMON;
use super::WORKER;

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
    f.write_str(&format!("Processor({})", self.id))
  }
}

impl Drop for Processor {
  fn drop(&mut self) {
    trace!("{:?} is destroyed", self);
  }
}

static COUNTER: AtomicU64 = AtomicU64::new(0);

impl Processor {
  pub fn new(is_parking: &'static AtomicBool, last_seen: &'static AtomicU64) -> Arc<Processor> {
    let worker = Worker::new_fifo();
    let (s, r) = unbounded();
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
    let worker = Rc::new(worker);

    // push remaining task to global queue before leaving
    defer! {
      while let Some(task) = worker.pop() {
        SYSMON.push_task(task);
      }
    }

    // set current thread worker queue, and unset it before leaving
    WORKER.with(|worker_tls| worker_tls.borrow_mut().replace(worker.clone()));
    defer! {
      WORKER.with(|worker_tls| { worker_tls.borrow_mut().take(); } );
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
          match SYSMON.pop_task(&worker) {
            Some(task) => run_task!(task),
            None => {}
          }
        }};
      }

      macro_rules! steal_others {
        () => {{
          match SYSMON.steal_task(&worker) {
            Some(task) => run_task!(task),
            None => {}
          }
        }};
      }

      self.tick();

      if counter >= MAX_RUNS {
        pop_global!();
      }

      if let Some(task) = worker.pop() {
        run_task!(task);
      }

      pop_global!();
      steal_others!();

      match self.steal_all_receiver.try_recv() {
        Ok(stealer) => {
          let steal = stealer.steal_batch_and_pop(&worker);
          match steal {
            Steal::Empty => {} // (*)
            _ => {
              // we are not done with this stealer yet
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

      for _ in 0..YIELDS {
        thread::yield_now();
        pop_global!();
        steal_others!();
      }

      self.park();
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
