use std::hint::unreachable_unchecked;
use std::mem::transmute;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering};
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

// how long a processor considered to be blocking
const BLOCKING_THRESHOLD: Duration = Duration::from_millis(100);

pub struct SysMon {
  // the global queue
  injector: Injector<Task>,

  // this hint will used to wakeup parked processor
  injector_hint_sender: Sender<()>,
  injector_hint_reciever: Receiver<()>,

  // list of valid processor
  processors: Vec<Arc<Processor>>,

  // "is_parking" and "last_seen" is stored in this vec,
  // so it is packed and more CPU cache friendly
  processors_status_storage: Vec<(AtomicBool, AtomicU64)>,

  // this hint will used to determine which processor
  // to be stealed
  steal_index_hint: AtomicUsize,
}

pub static SYSMON: Lazy<SysMon> = Lazy::new(|| {
  let num_cpus = std::cmp::max(num_cpus::get(), 1);
  let (s, r) = bounded(num_cpus);

  let mut processors_status_storage = Vec::new();
  for _ in 0..num_cpus {
    processors_status_storage.push((AtomicBool::new(false), AtomicU64::new(monotonic_ms())));
  }

  let mut processors = Vec::new();
  for i in 0..num_cpus {
    let (ref is_parking, ref last_seen) = processors_status_storage[i];

    // this is safe, because processors_status_storage is
    // intialized once and never get droped (basically it is static)
    let is_parking = unsafe { transmute::<&AtomicBool, &'static AtomicBool>(is_parking) };
    let last_seen = unsafe { transmute::<&AtomicU64, &'static AtomicU64>(last_seen) };

    processors.push(Processor::new(is_parking, last_seen));
  }

  let sysmon = SysMon {
    injector: Injector::new(),
    injector_hint_sender: s,
    injector_hint_reciever: r,

    processors,
    processors_status_storage,

    steal_index_hint: AtomicUsize::new(0),
  };

  thread::spawn(|| abort_on_panic(|| SYSMON.main()));

  sysmon
});

impl SysMon {
  fn main(&self) {
    loop {
      thread::sleep(BLOCKING_THRESHOLD);

      let now = monotonic_ms();
      let must_seen_at = now - BLOCKING_THRESHOLD.as_millis() as u64;

      for i in 0..self.processors_status_storage.len() {
        let (ref is_parking, ref last_seen) = self.processors_status_storage[i];
        if is_parking.load(Ordering::Relaxed) || must_seen_at <= last_seen.load(Ordering::Relaxed) {
          continue;
        }

        let current: &Arc<Processor> = &self.processors[i];
        current.mark_invalid();

        // this is safe as long:
        // 1. see SYSMON initialization
        // 2. processor never access it after mark_invalid() is called
        let is_parking = unsafe { transmute::<&AtomicBool, &'static AtomicBool>(is_parking) };
        let last_seen = unsafe { transmute::<&AtomicU64, &'static AtomicU64>(last_seen) };

        let new: &Arc<Processor> = &Processor::new(is_parking, last_seen);
        new.steal_all(current.get_stealer().clone());

        trace!("{:?} is blocking, replacing with {:?}", current, new);

        // force swap on immutable processors list, atomic update the processor pointer
        // inside the list, others will see some inconsistency in the list, but that is okay
        unsafe {
          let current = transmute::<&Arc<Processor>, &AtomicPtr<Arc<Processor>>>(current);
          let new = transmute::<&Arc<Processor>, &AtomicPtr<Arc<Processor>>>(&new);
          let old = current.swap(new.load(Ordering::SeqCst), Ordering::SeqCst);
          new.store(old, Ordering::SeqCst);
        }
      }
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

  pub fn park(&self) {
    let _ = self.injector_hint_reciever.recv();
  }
}
