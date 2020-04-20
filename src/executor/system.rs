use std::mem::transmute;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crossbeam_channel::{bounded, Receiver, Sender};
use crossbeam_deque::Worker;
use once_cell::sync::Lazy;

#[cfg(feature = "tracing")]
use log::trace;

use crate::utils::{abort_on_panic, monotonic_ms};

use super::machine::Machine;
use super::processor::Processor;
use super::Task;

// how long a processor considered to be blocking
const BLOCKING_THRESHOLD: Duration = Duration::from_millis(10);

// singleton: SYSTEM
pub struct System {
  num_cpus: usize,

  // all processors
  processors: Vec<Processor>,

  // used to select which processor got the task
  processor_push_index_hint: AtomicUsize,

  // machine[i] is currently running processor[i]
  machines: Vec<Arc<Machine>>,

  // used to select which machine to be stealed first
  machine_steal_index_hint: AtomicUsize,

  // for sysmon assist
  sysmon_notif: Sender<()>,
  sysmon_notif_recv: Receiver<()>,
}

// just to make sure
impl Drop for System {
  fn drop(&mut self) {
    panic!("System should not be dropped once created")
  }
}

pub static SYSTEM: Lazy<&'static System> = Lazy::new(|| {
  #[cfg(feature = "tracing")]
  trace!("Creating system");

  let num_cpus = std::cmp::max(1, num_cpus::get());

  // channel with buffer size 1 to not miss a notification
  let (sysmon_notif, sysmon_notif_recv) = bounded(1);

  let system = Box::into_raw(Box::new(System {
    num_cpus,

    processor_push_index_hint: AtomicUsize::new(0),
    machine_steal_index_hint: AtomicUsize::new(0),

    sysmon_notif,
    sysmon_notif_recv,

    processors: Vec::with_capacity(num_cpus), // to be initialized later
    machines: Vec::with_capacity(num_cpus),   // to be initialized later
  }));

  fn init(self_: &'static mut System) {
    for index in 0..self_.num_cpus {
      self_.processors.push(Processor::new(index));
    }

    for index in 0..self_.num_cpus {
      self_
        .machines
        .push(Machine::replace_processor_machine_with_new_one(
          &self_.processors[index],
          None,
        ));
    }

    // just to make sure that index between processor and machine is consistent
    for index in 0..self_.num_cpus {
      let p = &self_.processors[index];
      assert_eq!(index, p.index);
      assert!(p.still_on_machine(&self_.machines[index]));
    }

    // spawn dedicated sysmon thread
    thread::spawn(move || abort_on_panic(move || SYSTEM.sysmon_main()));
  }

  unsafe {
    init(&mut *system);
    &*system
  }
});

impl System {
  #[inline]
  fn sysmon_main(&'static self) {
    'main: loop {
      {
        let min_last_seen = self
          .processors
          .iter()
          .map(|p| p.get_last_seen())
          .min()
          .unwrap();

        let now = monotonic_ms();
        if min_last_seen > now {
          self.sysmon_notif_recv.recv().unwrap();
          continue 'main;
        } else {
          let next = min_last_seen + BLOCKING_THRESHOLD.as_millis() as u64;
          if next > now {
            thread::sleep(Duration::from_millis(next - now));
          } else {
            thread::sleep(Duration::from_millis(1));
          }
        }
      }

      {
        let must_seen_at = monotonic_ms() - BLOCKING_THRESHOLD.as_millis() as u64;
        'check: for index in 0..self.num_cpus {
          let p = &self.processors[index];

          if must_seen_at <= p.get_last_seen() {
            continue 'check;
          }

          let current = &self.machines[index];
          let new = &Machine::replace_processor_machine_with_new_one(p, Some(current.clone()));

          #[cfg(feature = "tracing")]
          trace!(
            "{:?} is blocking while running on {:?}, replacing with {:?}",
            p,
            current,
            new
          );

          // force swap on immutable list, atomic update the Arc/pointer in the list
          // this is safe because:
          // 1) Arc have same size with AtomicPtr
          // 2) Arc counter is not touched when swaping, no clone, no drop
          // 3) only one thread is doing this (guarded by self.check_running)
          unsafe {
            // #1
            if false {
              // do not run this code, this is for compile time checking only
              // transmute null_mut() to Arc will surely crashing the program
              //
              // https://internals.rust-lang.org/t/compile-time-assert/6751/2
              transmute::<AtomicPtr<()>, Arc<Machine>>(AtomicPtr::new(std::ptr::null_mut()));
            }

            // #2
            let current = transmute::<&Arc<Machine>, &AtomicPtr<()>>(current);
            let new = transmute::<&Arc<Machine>, &AtomicPtr<()>>(&new);
            let tmp = current.swap(new.load(Ordering::Relaxed), Ordering::Relaxed);
            new.store(tmp, Ordering::Relaxed);
          }
        }
      }
    }
  }

  #[inline]
  pub fn sysmon_wake_up(&self) {
    drop(self.sysmon_notif.try_send(()));
  }

  #[inline]
  pub fn push(&self, t: Task) {
    match Machine::direct_push(t) {
      // direct push to machine worker succeeded
      Ok(()) => {}

      // Fail, push to processor instead
      Err(t) => {
        let mut index = t.tag().get_schedule_index_hint();

        // if the task does not have prefered processor, we pick one
        if index > self.num_cpus {
          index = self.processor_push_index_hint.load(Ordering::Relaxed);

          // rotate the index, for fair load
          self.processor_push_index_hint.compare_and_swap(
            index,
            (index + 1) % self.num_cpus,
            Ordering::Relaxed,
          );
        }

        if !self.processors[index].push_then_wake_up(t) {
          // cannot send wake up signal to processors[index] (processor is busy),
          // wake up others
          let (l, r) = self.processors.split_at((index + 1) % self.num_cpus);
          r.iter()
            .chain(l.iter())
            .map(|p| p.wake_up())
            .filter(|r| *r)
            .nth(0);
        }
      }
    }
  }

  #[inline]
  pub fn pop(&self, index: usize, dest: &Worker<Task>) -> Option<Task> {
    // pop from global queue that dedicated to processor[index],
    // if None, pop from others
    let (l, r) = self.processors.split_at(index);
    r.iter()
      .chain(l.iter())
      .map(|p| p.pop(dest))
      .filter(|s| matches!(s, Some(_)))
      .nth(0)
      .flatten()
  }

  #[inline]
  pub fn steal(&self, dest: &Worker<Task>) -> Option<Task> {
    let m = self.machine_steal_index_hint.load(Ordering::Relaxed);
    let (l, r) = self.machines.split_at(m);
    (1..)
      .zip(r.iter().chain(l.iter()))
      .map(|(hint_add, m)| (hint_add, m.steal(dest)))
      .filter(|(_, s)| matches!(s, Some(_)))
      .nth(0)
      .map(|(hint_add, s)| {
        // rotate the index
        self.machine_steal_index_hint.compare_and_swap(
          m,
          (m + hint_add) % self.num_cpus,
          Ordering::Relaxed,
        );
        s
      })
      .flatten()
  }
}
