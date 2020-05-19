use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
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
        for (i, p) in processors.iter().enumerate() {
            assert_eq!(i, p.index);
        }

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
        self.processors.iter().for_each(|p| machine::spawn(self, p));

        loop {
            let check_tick = self.tick.fetch_add(1, Ordering::Relaxed) + 1;

            thread::sleep(BLOCKING_THRESHOLD);

            self.processors
                .iter()
                .filter(|p| p.get_last_seen() < check_tick)
                .for_each(|p| {
                    #[cfg(feature = "tracing")]
                    trace!("{:?} is blocked, spawn new machine for it", p);

                    machine::spawn(self, p);
                });

            if self.processors.iter().all(|p| p.is_sleeping()) {
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
        if index >= self.processors.len() {
            index -= self.processors.len();
        }

        let p = &self.processors[index];
        let already_running = !p.is_sleeping();

        p.wake_up();

        // wake up another one, in case p need help
        if already_running {
            let (l, r) = self.processors.split_at(index);
            r.iter().chain(l.iter()).rev().find(|p| p.wake_up());
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

    #[inline(always)]
    fn recalc_steal_index_hint(&self, add: usize) -> usize {
        let old_hint = self.steal_index_hint.fetch_add(add, Ordering::Relaxed);
        if old_hint < self.processors.len() {
            return old_hint;
        }

        let hint = old_hint % self.processors.len();
        self.steal_index_hint
            .compare_and_swap(old_hint + add, hint + add, Ordering::Relaxed);

        hint
    }

    pub fn steal(&self, worker: &Worker<Task>) -> Option<Task> {
        let (l, r) = self.processors.split_at(self.recalc_steal_index_hint(1));
        (r.iter().chain(l.iter()).enumerate())
            .map(|(i, p)| (i, p.steal(worker)))
            .find(|(_, s)| s.is_some())
            .map(|(i, s)| {
                if i != 0 {
                    self.recalc_steal_index_hint(i);
                }
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

static NUM_CPUS: AtomicUsize = AtomicUsize::new(0);

/// set the number of executor thread
///
/// analogous to GOMAXPROCS in golang,
/// can only be set once and before executor is running,
/// if not set before executor running, it will be the number of available cpu in the host
pub fn set_num_cpus(size: usize) -> Result<(), String> {
    let old_value = NUM_CPUS.compare_and_swap(0, size, Ordering::Relaxed);
    if old_value == 0 {
        Ok(())
    } else {
        Err(format!("num_cpus already set to {}", old_value))
    }
}

/// get the number of executor thread,
/// 0 if executor is not run yet
pub fn get_num_cpus() -> usize {
    NUM_CPUS.load(Ordering::Relaxed)
}

fn lock_num_cpus() -> usize {
    let num_cpus = &NUM_CPUS;
    if num_cpus.load(Ordering::Relaxed) == 0 {
        num_cpus.compare_and_swap(0, std::cmp::max(1, num_cpus::get()), Ordering::Relaxed);
    }
    num_cpus.load(Ordering::Relaxed)
}
