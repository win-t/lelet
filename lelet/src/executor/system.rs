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

pub struct System {
    num_cpus: usize,

    /// all processors
    processors: Vec<Processor>,

    processor_push_index_hint: AtomicUsize,
    steal_index_hint: AtomicUsize,

    /// for blocking detection
    tick: AtomicU64,

    wake_up_notif_sender: Sender<()>,
    wake_up_notif_receiver: Receiver<()>,
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
        static SYSTEM: Lazy<System> = Lazy::new(|| {
            thread::spawn(move || abort_on_panic(move || System::get().sysmon_main()));

            let num_cpus = std::cmp::max(1, num_cpus::get());

            // channel with buffer size 1 to not miss a notification
            let (wake_up_notif_sender, wake_up_notif_receiver) = bounded(1);

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

            System {
                num_cpus,
                processors,

                processor_push_index_hint: AtomicUsize::new(0),
                steal_index_hint: AtomicUsize::new(0),

                tick: AtomicU64::new(0),

                wake_up_notif_sender,
                wake_up_notif_receiver,
            }
        });

        &SYSTEM
    }

    fn sysmon_main(&'static self) {
        #[cfg(feature = "tracing")]
        trace!("Sysmon is running");

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
                self.wake_up_notif_receiver.recv().unwrap();
            }
        }
    }

    pub fn sysmon_wake_up(&self) {
        let _ = self.wake_up_notif_sender.try_send(());
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
            self.processors_wake_up(index);
        }
    }

    pub fn processors_wake_up(&self, index: usize) {
        // wake up processors[index]
        self.processors[index].wake_up();

        // plus another one, in case processors[index] need help
        let (l, r) = self.processors.split_at(index + 1);
        r.iter().chain(l.iter()).find(|p| p.wake_up());
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
            .zip(r.iter().chain(l.iter()).map(|p| p.get_current_machine()))
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
