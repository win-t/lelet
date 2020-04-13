use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use crossbeam_channel::{unbounded, Receiver, Sender};
use crossbeam_deque::{Steal, Stealer, Worker};

use crate::thread_pool;
use crate::utils::abort_on_panic;
use crate::utils::monotonic_ms;

use super::Task;
use super::SYSMON;
use super::WORKER;

pub struct Processor {
  valid: AtomicBool,
  parked: AtomicBool,
  last_seen: AtomicU64,
  stealer: Stealer<Task>,
  steal_all_sender: Sender<Stealer<Task>>,
  steal_all_receiver: Receiver<Stealer<Task>>,
}

macro_rules! run_task {
  ($self:ident, $task:ident, $counter:ident) => {{
    $task.run();
    $counter += 1;
    if !$self.still_valid() {
      return;
    }
  }};
}

impl Processor {
  pub fn new() -> Arc<Processor> {
    let worker = Worker::new_fifo();
    let (s, r) = unbounded();
    let processor = Arc::new(Processor {
      valid: AtomicBool::new(true),
      parked: AtomicBool::new(false),
      last_seen: AtomicU64::new(monotonic_ms()),
      stealer: worker.stealer(),
      steal_all_sender: s,
      steal_all_receiver: r,
    });
    {
      let processor = processor.clone();
      thread_pool::spawn_box(Box::new(move || abort_on_panic(|| processor.main(worker))));
    }
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
    let mut counter: u64 = 0;
    loop {
      self.tick();

      if counter > MAX_RUNS {
        counter = 0;
        match SYSMON.pop_task(&worker) {
          Some(task) => run_task!(self, task, counter),
          None => {}
        }
      }

      if let Some(task) = worker.pop() {
        run_task!(self, task, counter);
        continue;
      }

      match SYSMON.pop_task(&worker) {
        Some(task) => {
          run_task!(self, task, counter);
          continue;
        }
        None => {}
      }

      match self.steal_all_receiver.try_recv() {
        Ok(stealer) => {
          match stealer.steal_batch(&worker) {
            Steal::Empty => {}
            _ => self.steal_all_sender.send(stealer).unwrap(),
          }
          continue;
        }
        _ => {}
      }

      match SYSMON.steal_task(&worker) {
        Some(task) => {
          run_task!(self, task, counter);
          continue;
        }
        None => {}
      }

      self.parked.store(true, Ordering::Relaxed);
      SYSMON.park();
      self.parked.store(false, Ordering::Relaxed);
    }
  }

  fn tick(&self) {
    self.last_seen.store(monotonic_ms(), Ordering::Relaxed);
  }

  pub fn is_parking(&self) -> bool {
    self.parked.load(Ordering::Relaxed)
  }

  pub fn get_last_seen(&self) -> u64 {
    self.last_seen.load(Ordering::Relaxed)
  }

  fn still_valid(&self) -> bool {
    self.valid.load(Ordering::Relaxed)
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
