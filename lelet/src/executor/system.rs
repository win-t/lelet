use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use crossbeam_deque::Worker;
use crossbeam_utils::CachePadded;
use once_cell::sync::Lazy;

#[cfg(feature = "tracing")]
use log::trace;

use lelet_utils::abort_on_panic;

use crate::utils::{atomic_usize_add_mod, coprime, Sleeper};

use super::machine;
use super::processor::Processor;
use super::Task;

/// how long a processor considered to be blocking
const BLOCKING_THRESHOLD: Duration = Duration::from_millis(10);

pub struct System {
    /// all processors
    pub processors: Vec<Processor>,

    /// for blocking detection
    tick: AtomicU64,

    push_hint: AtomicUsize,
    steal_hint: Vec<(CachePadded<AtomicUsize>, Vec<usize>)>,

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
        let processors: Vec<Processor> = (0..num_cpus).map(Processor::new).collect();

        // just to make sure that processor index is valid
        for (i, p) in processors.iter().enumerate() {
            assert_eq!(i, p.index);
        }

        let steal_hint: Vec<(CachePadded<AtomicUsize>, Vec<usize>)> = (3..)
            .step_by(2)
            .filter(|i| coprime(*i, num_cpus))
            .take(num_cpus)
            .enumerate()
            .map(|(i, c)| {
                (0..num_cpus)
                    .map(move |j| ((c * j) + i) % num_cpus)
                    .filter(move |s| *s != i)
                    .collect()
            })
            .map(|o| (CachePadded::default(), o))
            .collect();

        // just to make sure that steal_hint are valid
        let sum: usize = (0..num_cpus).sum();
        for (i, s) in steal_hint.iter().enumerate() {
            assert!(s.1.iter().all(|&x| x != i));
            assert_eq!(sum, i + s.1.iter().sum::<usize>());
        }

        System {
            processors,
            steal_hint,

            tick: AtomicU64::new(0),
            push_hint: AtomicUsize::new(0),
            sleeper: Sleeper::new(),
        }
    }

    #[inline(always)]
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

    #[inline(always)]
    pub fn sysmon_wake_up(&self) {
        self.sleeper.wake_up();
    }

    #[inline(always)]
    pub fn push(&self, task: Task) {
        match machine::direct_push(task) {
            Ok(mut index) => {
                if index == self.processors.len() - 1 {
                    index = 0;
                } else {
                    index += 1;
                }
                self.processors[index].wake_up();
            }
            Err(task) => {
                let mut index = task.tag().get_index_hint();
                if index >= self.processors.len() {
                    index = atomic_usize_add_mod(&self.push_hint, 1, self.processors.len());
                }
                let p = &self.processors[index];
                p.push(task);
                p.wake_up();
            }
        };
    }

    #[inline(always)]
    pub fn pop(&self, worker: &Worker<Task>, index: usize) -> Option<Task> {
        let (l, r) = self.processors.split_at(index);
        for p in r.iter().chain(l.iter()) {
            if let Some(task) = p.pop(worker) {
                return Some(task);
            }
        }
        None
    }

    #[inline(always)]
    pub fn steal(&self, worker: &Worker<Task>, index: usize) -> Option<Task> {
        let (steal_index, steal_order) = &self.steal_hint[index];
        let (l, r) = steal_order.split_at(atomic_usize_add_mod(steal_index, 1, steal_order.len()));
        for (add, &index) in r.iter().chain(l.iter()).enumerate() {
            if let Some(task) = self.processors[index].steal(worker) {
                atomic_usize_add_mod(steal_index, add, steal_order.len());
                return Some(task);
            }
        }
        None
    }

    #[inline(always)]
    pub fn now(&self) -> u64 {
        self.tick.load(Ordering::Relaxed)
    }
}

#[inline(always)]
pub fn get() -> &'static System {
    static SYSTEM: Lazy<System> = Lazy::new(|| {
        thread::spawn(move || abort_on_panic(move || get().sysmon_run()));
        System::new(lock_num_cpus())
    });

    &SYSTEM
}

/// spawn new machine for current processor.
///
/// this is useful if you know that you are going to do blocking that longer
/// than blocking threshold.
pub fn mark_blocking() {
    machine::respawn();
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
