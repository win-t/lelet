use std::hint::unreachable_unchecked;
use std::mem::transmute;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crossbeam_channel::{bounded, Receiver, Sender};
use crossbeam_deque::{Injector, Steal, Worker};
use once_cell::sync::Lazy;

use crate::utils::abort_on_panic;
use crate::utils::monotonic_ms;

use super::processor::Processor;
use super::Task;
use super::BLOCKING_THRESHOLD;

pub struct Runtime {
  // the global queue
  injector: Injector<Task>,

  // this hint will used to wakeup idle processor
  injector_hint_sender: Sender<()>,
  injector_hint_reciever: Receiver<()>,

  // list of valid processor
  processors: Vec<Arc<Processor>>,

  // this hint will used to determine which processor
  // to be stealed
  steal_index_hint: AtomicUsize,
}

pub static RUNTIME: Lazy<Runtime> = Lazy::new(|| {
  let num_cpus = std::cmp::max(num_cpus::get(), 1);
  let (s, r) = bounded(num_cpus);

  let runtime = Runtime {
    injector: Injector::new(),
    injector_hint_sender: s,
    injector_hint_reciever: r,

    processors: std::iter::repeat_with(|| Processor::new())
      .take(num_cpus)
      .collect(),

    steal_index_hint: AtomicUsize::new(0),
  };

  thread::spawn(|| abort_on_panic(|| RUNTIME.main()));

  runtime
});

impl Runtime {
  fn main(&self) {
    let mut delay = BLOCKING_THRESHOLD;
    loop {
      thread::sleep(delay);

      let now = monotonic_ms();
      let must_seen_at = now - BLOCKING_THRESHOLD.as_millis() as u64;

      // find blocking processors, a processor is considered as blocking
      // when they failed to update last_seen at least once per BLOCKING_THRESHOLD period
      let mut replacement: Vec<_> = self
        .processors
        .iter()
        .filter(|p| p.get_last_seen() < must_seen_at)
        .map(|p| p.clone())
        .zip(std::iter::repeat_with(|| Processor::new()))
        .collect();

      // update processors list
      for current in self.processors.iter().rev() {
        if replacement.is_empty() {
          break;
        }

        let (old, _) = &replacement[replacement.len() - 1];
        if Arc::ptr_eq(current, &old) {
          let (old, new) = replacement.pop().unwrap();
          old.mark_invalid();
          new.steal_all(old.get_stealer().clone());

          // force swap on immutable processors list, atomic update the processor pointer
          // inside the list, others will see some inconsistency in the list, but that is okay
          unsafe {
            let current = transmute::<&Arc<Processor>, &AtomicPtr<*mut Processor>>(current);
            let new = transmute::<&Arc<Processor>, &AtomicPtr<*mut Processor>>(&new);
            let old = current.swap(new.load(Ordering::SeqCst), Ordering::SeqCst);
            new.store(old, Ordering::SeqCst);
          }
        }
      }

      delay = Duration::from_millis(
        self
          .processors
          .iter()
          .map(|p| p.get_last_seen())
          .chain(std::iter::once(now))
          .min()
          .unwrap()
          + BLOCKING_THRESHOLD.as_millis() as u64
          - now,
      );
    }
  }

  pub fn push_task(&self, t: Task) {
    self.injector.push(t);
    let _ = self.injector_hint_sender.try_send(());
  }

  pub fn pop_task(&self, dest: &Worker<Task>) -> Option<Task> {
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

  pub fn steal_task(&self, dest: &Worker<Task>) -> Option<Task> {
    let mid = self.steal_index_hint.load(Ordering::Relaxed);
    let (l, r) = self.processors.split_at(mid);
    (1..)
      .zip(r.iter().chain(l.iter()))
      .map(|(i, p)| (i, p.get_stealer().steal_batch_and_pop(dest)))
      .filter(|(_, s)| matches!(s, Steal::Success(_))) // (*)
      .nth(0)
      .map(|(i, s)| {
        self
          .steal_index_hint
          .store((mid + i) % self.processors.len(), Ordering::Relaxed);
        match s {
          Steal::Success(task) => task,
          _ => unsafe { unreachable_unchecked() }, // (*)
        }
      })
  }

  pub fn park_timeout(&self, d: Duration) {
    let _ = self.injector_hint_reciever.recv_timeout(d);
  }
}
