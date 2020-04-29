use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
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

// singleton: SYSTEM
pub struct System {
    num_cpus: usize,

    /// all processors
    processors: Vec<Processor>,

    /// used to select which processor got the task
    processor_push_index_hint: AtomicUsize,

    /// machine[i] is currently running processor[i]
    machines: Vec<Arc<Machine>>,

    /// used to select which machine to be stole first
    machine_steal_index_hint: AtomicUsize,

    /// for blocking detection
    /// 1 tick = BLOCKING_THRESHOLD/2
    tick: AtomicU64,

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

static SYSTEM: Lazy<&'static System> = Lazy::new(|| {
    #[cfg(feature = "tracing")]
    trace!("Creating system");

    let num_cpus = std::cmp::max(1, num_cpus::get());

    // channel with buffer size 1 to not miss a notification
    let (sysmon_notif, sysmon_notif_recv) = bounded(1);

    let system = Box::into_raw(Box::new(System {
        num_cpus,

        processor_push_index_hint: AtomicUsize::new(0),
        machine_steal_index_hint: AtomicUsize::new(0),

        tick: AtomicU64::new(0),

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
    pub fn get() -> &'static System {
        &SYSTEM
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
                trace!("{:?} was blocked, replacing the machine", p);

                unsafe {
                    use std::sync::atomic::AtomicPtr;

                    // force swap on immutable list, atomic update the Arc/pointer in the list
                    // this is safe because:
                    // (1) Arc have same size with AtomicPtr
                    // (2) Arc counter is not touched when swaping, no clone, no drop

                    // to make sure (1) at compile time
                    // https://internals.rust-lang.org/t/compile-time-assert/6751/2
                    if false {
                        // do not run this code, transmute invalid AtomicPtr to Arc
                        // will surely crashing the program
                        std::mem::transmute::<AtomicPtr<()>, Arc<Machine>>(AtomicPtr::default());
                    }

                    // (2) is just AtomicPtr swap
                    let current = &*(current as *const Arc<Machine> as *const AtomicPtr<()>);
                    let new = &*(new as *const Arc<Machine> as *const AtomicPtr<()>);
                    let tmp = current.swap(new.load(Ordering::Relaxed), Ordering::Relaxed);
                    new.store(tmp, Ordering::Relaxed);
                }
            }

            if self
                .processors
                .iter()
                .all(|p| p.get_last_seen() == u64::MAX)
            {
                // all processor is sleeping, also go to sleep
                self.sysmon_notif_recv.recv().unwrap();
            }
        }
    }

    pub fn sysmon_wake_up(&self) {
        let _ = self.sysmon_notif.try_send(());
    }

    pub fn push(&self, task: Task) {
        if let Err(task) = Machine::direct_push(task) {
            let mut index = task.tag().get_schedule_index_hint();

            // if the task does not have prefered processor, we pick one
            if index >= self.num_cpus {
                index = self.processor_push_index_hint.load(Ordering::Relaxed);
                self.processor_push_index_hint
                    .store((index + 1) % self.num_cpus, Ordering::Relaxed);
            }

            let processor = &self.processors[index];
            processor.push(task);

            if !processor.wake_up() {
                // cannot send wake up signal (processor is busy), wake up others
                let (l, r) = self.processors.split_at(index + 1);
                r.iter().chain(l.iter()).find(|p| p.wake_up());
            }
        }
    }

    pub fn pop(&self, index: usize, worker: &Worker<Task>) -> Option<Task> {
        // pop from global queue that dedicated to processor[index],
        // if None, pop from others
        let (l, r) = self.processors.split_at(index);
        r.iter()
            .chain(l.iter())
            .map(|p| p.pop(worker))
            .find(|s| s.is_some())
            .flatten()
    }

    pub fn steal(&self, worker: &Worker<Task>) -> Option<Task> {
        let m = self.machine_steal_index_hint.load(Ordering::Relaxed);
        let (l, r) = self.machines.split_at(m);
        (1..)
            .zip(r.iter().chain(l.iter()))
            .map(|(hint_add, m)| (hint_add, m.steal(worker)))
            .find(|(_, s)| s.is_some())
            .map(|(hint_add, s)| {
                self.machine_steal_index_hint
                    .store((m + hint_add) % self.num_cpus, Ordering::Relaxed);
                s
            })
            .flatten()
    }

    #[inline(always)]
    pub fn now(&self) -> u64 {
        self.tick.load(Ordering::Relaxed)
    }
}
