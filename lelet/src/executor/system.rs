use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

use crossbeam_deque::{Injector, Steal, Worker};
use once_cell::sync::Lazy;

#[cfg(feature = "tracing")]
use log::trace;

use lelet_utils::abort_on_panic;

use crate::utils::Sleeper;

use super::machine;
use super::processor::Processor;
use super::Task;

/// how long a processor considered to be blocking
const BLOCKING_THRESHOLD: Duration = Duration::from_millis(10);

pub struct System {
    /// all processors
    pub processors: Vec<Processor>,

    /// global queue
    injector: Injector<Task>,

    steal_index_hint: AtomicUsize,

    /// for blocking detection
    tick: AtomicU64,

    sleeper: Sleeper,
}

// just to make sure
impl Drop for System {
    fn drop(&mut self) {
        eprintln!("System should not be dropped once created");
        std::process::abort();
    }
}

impl System {
    fn new(num_cpus: usize) -> System {
        let mut processors = Vec::with_capacity(num_cpus);
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
            processors,
            injector: Injector::new(),
            steal_index_hint: AtomicUsize::new(0),
            tick: AtomicU64::new(0),
            sleeper: Sleeper::new(),
        }
    }

    fn sysmon_run(&'static self) {
        #[cfg(feature = "tracing")]
        trace!("Sysmon is running");

        // spawn machine for every processor
        self.processors
            .iter()
            .for_each(|p| machine::spawn(self, p.index));

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

            processors.for_each(|p| machine::spawn(self, p.index));

            if self.processors.iter().all(|p| p.is_sleeping()) {
                // all processor is sleeping, also go to sleep
                self.sleeper.sleep();
            }
        }
    }

    pub fn sysmon_wake_up(&self) {
        self.sleeper.wake_up();
    }

    pub fn push(&self, task: Task) {
        if let Err(task) = machine::direct_push(task) {
            #[cfg(feature = "tracing")]
            trace!("{:?} is pushed to global queue", task.tag());

            self.injector.push(task);
            self.processors_wake_up();
        }
    }

    pub fn processors_wake_up(&self) {
        // wake up processor that unlikely to be stolen near future
        let mut index = self.steal_index_hint.load(Ordering::Relaxed) + self.processors.len() - 1;
        while index >= self.processors.len() {
            index -= self.processors.len();
        }

        let p = &self.processors[index];
        let is_sleeping = p.is_sleeping();

        p.wake_up();

        // wake up another one, in case p need help
        if !is_sleeping {
            let (l, r) = self.processors.split_at(index + 1);
            r.iter().chain(l.iter()).find(|p| p.wake_up());
        }
    }

    pub fn pop(&self, worker: &Worker<Task>) -> Option<Task> {
        // repeat until success or empty
        std::iter::repeat_with(|| self.injector.steal_batch_and_pop(worker))
            .find(|s| !s.is_retry())
            .map(|s| match s {
                Steal::Success(task) => Some(task),
                Steal::Empty => None,
                Steal::Retry => unreachable!(), // already filtered
            })
            .flatten()
    }

    pub fn steal(&self, worker: &Worker<Task>) -> Option<Task> {
        let hint = self.steal_index_hint.load(Ordering::Relaxed);
        let (l, r) = self.processors.split_at(hint);
        (1..)
            .zip(r.iter().chain(l.iter()))
            .map(|(hint_add, p)| (hint_add, p.steal(worker)))
            .find(|(_, s)| s.is_some())
            .map(|(hint_add, s)| {
                let mut new_hint = hint + hint_add;
                while new_hint >= self.processors.len() {
                    new_hint -= self.processors.len();
                }

                self.steal_index_hint
                    .compare_and_swap(hint, new_hint, Ordering::Relaxed);

                s
            })
            .flatten()
    }

    #[inline(always)]
    pub fn now(&self) -> u64 {
        self.tick.load(Ordering::Relaxed)
    }
}

pub fn get() -> &'static System {
    static SYSTEM: Lazy<System> = Lazy::new(|| {
        thread::spawn(move || abort_on_panic(move || get().sysmon_run()));
        System::new(lock_num_cpus())
    });

    &SYSTEM
}

static NUM_CPUS: Lazy<Mutex<(bool, usize)>> = Lazy::new(|| Mutex::new((false, 0)));

pub fn set_num_cpus(size: usize) -> Result<usize, &'static str> {
    let mut num_cpus = NUM_CPUS.lock().unwrap();
    if num_cpus.0 {
        return Err("cannot change num_cpus");
    }
    let old = num_cpus.1;
    num_cpus.1 = size;
    Ok(old)
}

pub fn get_num_cpus() -> usize {
    NUM_CPUS.lock().unwrap().1
}

fn lock_num_cpus() -> usize {
    let mut num_cpus = NUM_CPUS.lock().unwrap();
    if num_cpus.1 == 0 {
        num_cpus.1 = std::cmp::max(1, num_cpus::get());
    }
    num_cpus.0 = true;
    num_cpus.1
}
