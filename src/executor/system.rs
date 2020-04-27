use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crossbeam_channel::{bounded, Receiver, Sender};
use crossbeam_deque::Worker;
use once_cell::sync::Lazy;

#[cfg(feature = "tracing")]
use log::trace;

use crate::utils::abort_on_panic;

use super::machine::Machine;
use super::processor::Processor;
use super::Task;

/// how long a processor considered to be blocking
const BLOCKING_THRESHOLD: Duration = Duration::from_millis(10);

/// singleton: SYSTEM
pub struct System {
    num_cpus: usize,

    /// all processors
    processors: Vec<Processor>,

    /// used to select which processor got the task
    processor_push_index_hint: AtomicUsize,

    /// machine[i] is currently running processor[i]
    machines: Vec<Arc<Machine>>,

    /// used to select which machine to be stealed first
    machine_steal_index_hint: AtomicUsize,

    /// for blocking detection
    /// 1 tick = BLOCKING_THRESHOLD/2
    tick: AtomicUsize,

    // for sysmon wake up notification
    sysmon_notif: Sender<()>,
    sysmon_notif_recv: Receiver<()>,
}

// just to make sure
impl Drop for System {
    fn drop(&mut self) {
        eprintln!("System should not be dropped once created");
        std::process::abort();
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

        tick: AtomicUsize::new(0),

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
                .push(Machine::replace(&self_.processors[index], None));
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
    pub fn get(&'static self) -> &'static System {
        self
    }

    fn sysmon_main(&'static self) {
        // spinloop until initial machines run the processors
        loop {
            let mut ready = 0;
            for index in 0..self.num_cpus {
                let processor = &self.processors[index];
                assert_eq!(processor.index, index);
                if processor.still_on_machine(&self.machines[index]) {
                    ready += 1;
                }
            }
            if ready == self.num_cpus {
                break;
            }
        }

        loop {
            thread::sleep(BLOCKING_THRESHOLD / 2);
            self.tick.fetch_add(1, Ordering::Relaxed);

            let must_seen_at = self.now() - 1;
            'check: for index in 0..self.num_cpus {
                let p = &self.processors[index];

                if must_seen_at <= p.get_last_seen() {
                    continue 'check;
                }

                let current = &self.machines[index];
                let new = &Machine::replace(p, Some(current.clone()));

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
                unsafe {
                    // #1
                    if false {
                        // do not run this code, this is for compile time checking only
                        // transmute null_mut() to Arc will surely crashing the program
                        //
                        // https://internals.rust-lang.org/t/compile-time-assert/6751/2
                        std::mem::transmute::<AtomicPtr<()>, Arc<Machine>>(AtomicPtr::new(
                            std::ptr::null_mut(),
                        ));
                    }

                    // #2
                    let current = &*(current as *const Arc<Machine> as *const AtomicPtr<()>);
                    let new = &*(new as *const Arc<Machine> as *const AtomicPtr<()>);
                    let tmp = current.swap(new.load(Ordering::Relaxed), Ordering::Relaxed);
                    new.store(tmp, Ordering::Relaxed);
                }
            }

            if self
                .processors
                .iter()
                .map(|p| p.get_last_seen())
                .all(|last_seen| last_seen == usize::MAX)
            {
                #[cfg(feature = "tracing")]
                trace!("sysmon entering sleep");

                #[cfg(feature = "tracing")]
                defer! {
                  trace!("sysmon leaving sleep");
                }

                // all processor is sleeping, also go to sleep
                self.sysmon_notif_recv.recv().unwrap();
            }
        }
    }

    pub fn sysmon_wake_up(&self) {
        let _ = self.sysmon_notif.try_send(());
    }

    pub fn push(&self, t: Task) {
        match Machine::direct_push(t) {
            // direct push to machine worker succeeded
            Ok(()) => {}

            // Fail, push to processor instead
            Err(t) => {
                let mut index = t.tag().get_schedule_index_hint();

                // if the task does not have prefered processor, we pick one
                if index >= self.num_cpus {
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
                    r.iter().chain(l.iter()).map(|p| p.wake_up()).find(|r| *r);
                }
            }
        }
    }

    pub fn pop(&self, index: usize, dest: &Worker<Task>) -> Option<Task> {
        // pop from global queue that dedicated to processor[index],
        // if None, pop from others
        let (l, r) = self.processors.split_at(index);
        r.iter()
            .chain(l.iter())
            .map(|p| p.pop(dest))
            .find(|s| matches!(s, Some(_)))
            .flatten()
    }

    pub fn steal(&self, dest: &Worker<Task>) -> Option<Task> {
        let m = self.machine_steal_index_hint.load(Ordering::Relaxed);
        let (l, r) = self.machines.split_at(m);
        (1..)
            .zip(r.iter().chain(l.iter()))
            .map(|(hint_add, m)| (hint_add, m.steal(dest)))
            .find(|(_, s)| matches!(s, Some(_)))
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

    #[inline(always)]
    pub fn now(&self) -> usize {
        self.tick.load(Ordering::Relaxed)
    }
}
