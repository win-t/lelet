use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
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

    processor_push_index_hint: AtomicUsize,
    steal_index_hint: AtomicUsize,

    /// for blocking detection
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

impl System {
    pub fn get() -> &'static System {
        static SYSTEM: Lazy<&'static System> = Lazy::new(|| {
            #[cfg(feature = "tracing")]
            trace!("Creating system");

            let num_cpus = std::cmp::max(1, num_cpus::get());

            // channel with buffer size 1 to not miss a notification
            let (sysmon_notif, sysmon_notif_recv) = bounded(1);

            let mut processors = Vec::new();
            for index in 0..num_cpus {
                processors.push(Processor::new(index));
            }

            // just to make sure that processor index is consistent
            (0..num_cpus)
                .zip(processors.iter())
                .for_each(|(index, processor)| {
                    assert_eq!(processor.index, index);
                });

            let system = System {
                num_cpus,
                processors,

                processor_push_index_hint: AtomicUsize::new(0),
                steal_index_hint: AtomicUsize::new(0),

                tick: AtomicU64::new(0),

                sysmon_notif,
                sysmon_notif_recv,
            };

            thread::spawn(move || abort_on_panic(move || System::get().sysmon_main()));

            // alocate on heap and leak it to make sure it has 'static lifetime
            unsafe { &*Box::into_raw(Box::new(system)) }
        });

        &SYSTEM
    }

    fn sysmon_main(&'static self) {
        // spawn machine for every processor
        self.processors.iter().for_each(Machine::spawn);

        loop {
            let check_tick = self.tick.fetch_add(1, Ordering::Relaxed) + 1;

            thread::sleep(BLOCKING_THRESHOLD);

            let processors = self
                .processors
                .iter()
                .filter(|p| p.get_last_seen() < check_tick);

            #[cfg(feature = "tracing")]
            let processors = processors.map(|p| {
                trace!("{:?} was blocked, replacing its machine", p);
                p
            });

            processors.for_each(Machine::spawn);

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
                self.processor_push_index_hint.compare_and_swap(
                    index,
                    (index + 1) % self.num_cpus,
                    Ordering::Relaxed,
                );
            }

            self.processors[index].push(task);
            self.processors_wake_up();
        }
    }

    pub fn processors_wake_up(&self) {
        self.processors.iter().for_each(|p| p.wake_up());
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
        let m = self.steal_index_hint.load(Ordering::Relaxed);
        let (l, r) = self.processors.split_at(m);
        (1..)
            .zip(r.iter().chain(l.iter()).map(|p| p.get_machine().as_ref()))
            .filter(|(_, m)| m.is_some())
            .map(|(hint_add, m)| (hint_add, m.unwrap().steal(worker)))
            .find(|(_, s)| s.is_some())
            .map(|(hint_add, s)| {
                self.steal_index_hint.compare_and_swap(
                    m,
                    (m + hint_add) % self.num_cpus,
                    Ordering::Relaxed,
                );
                s
            })
            .flatten()
    }

    #[inline(always)]
    pub fn now(&self) -> u64 {
        self.tick.load(Ordering::Relaxed)
    }
}
