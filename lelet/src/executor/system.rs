use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use crossbeam_deque::{Steal, Worker};
use crossbeam_utils::sync::{Parker, Unparker};
use once_cell::sync::Lazy;

#[cfg(feature = "tracing")]
use log::trace;

use lelet_utils::{abort_on_panic, SimpleLock};

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
    steal_orders: Vec<Vec<usize>>,

    parker: SimpleLock<Parker>,
    unparker: Unparker,
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
        #[cfg(feature = "tracing")]
        trace!("Creating system");

        let processors: Vec<Processor> = (0..num_cpus).map(Processor::new).collect();

        let steal_orders: Vec<Vec<usize>> = (3..)
            .step_by(2)
            .filter(|&i| coprime(i, num_cpus))
            .take(num_cpus)
            .enumerate()
            .map(|(i, c)| {
                (0..num_cpus)
                    .map(move |j| ((c * j) + i) % num_cpus)
                    .filter(move |&s| s != i)
                    .collect()
            })
            .collect();

        let parker = Parker::new();
        let unparker = parker.unparker().clone();

        // do some validity check
        // this will justify the usage of unsafe get_unchecked
        assert_eq!(processors.len(), steal_orders.len());
        for (i, p) in processors.iter().enumerate() {
            assert_eq!(i, p.index);
        }
        for (i, s) in steal_orders.iter().enumerate() {
            assert!((0..num_cpus).eq(std::iter::once(i)
                .chain(s.clone().into_iter())
                .collect::<std::collections::BinaryHeap<usize>>()
                .into_sorted_vec()));
        }

        System {
            processors,
            steal_orders,

            tick: AtomicU64::new(0),
            push_hint: AtomicUsize::new(0),

            parker: SimpleLock::new(parker),
            unparker,
        }
    }

    #[inline(always)]
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

            if self.processors.iter().any(|p| !p.is_empty()) {
                let mut sleeping_processors = self.processors.iter().filter(|p| p.is_sleeping());

                for p in &self.processors {
                    if p.get_last_seen() < check_tick {
                        if let Some(other) = sleeping_processors.next() {
                            #[cfg(feature = "tracing")]
                            trace!("{:?} is blocked, waking up {:?}", p, other);

                            other.wake_up();
                        } else {
                            #[cfg(feature = "tracing")]
                            trace!("{:?} is blocked, spawn new machine for it", p);

                            machine::spawn(self, p.index);
                        }
                    }
                }
            } else if self.processors.iter().all(|p| p.is_sleeping()) {
                #[cfg(feature = "tracing")]
                trace!("Sysmon entering sleep");
                if let Some(parker) = self.parker.try_lock() {
                    parker.park();
                }
                #[cfg(feature = "tracing")]
                trace!("Sysmon exiting sleep");
            }
        }
    }

    #[inline(always)]
    fn next_push_index(&self) -> usize {
        // will always in range 0..processors.len()
        // this will justify the usage of unsafe get_unchecked
        loop {
            let index = self.push_hint.load(Ordering::Relaxed);
            if self.push_hint.compare_and_swap(
                index,
                (index + 1) % self.processors.len(),
                Ordering::Relaxed,
            ) == index
            {
                break index;
            }
        }
    }

    #[inline(always)]
    pub fn push(&self, task: Task) {
        match machine::direct_push(task) {
            Ok((index, counter)) => {
                if counter <= 1 {
                    unsafe { self.processors.get_unchecked(index) }.wake_up();
                } else {
                    let mut other_index = self.next_push_index();
                    if index == other_index {
                        other_index = self.next_push_index();
                    }
                    unsafe { self.processors.get_unchecked(other_index) }.wake_up();
                }
            }
            Err(task) => {
                let mut index = task.tag().get_index_hint();
                if index >= self.processors.len() {
                    index = self.next_push_index();
                }

                self.unparker.unpark();

                let p = unsafe { &self.processors.get_unchecked(index) };
                p.push_global(task);
                p.wake_up();
            }
        }
    }

    #[inline(always)]
    pub fn pop_into(&self, worker: &Worker<Task>, processor: &Processor) -> Option<Task> {
        let mut retry = true;
        while retry {
            retry = false;

            // check dedicated global queue first
            match unsafe { self.processors.get_unchecked(processor.index) }.pop_global(worker) {
                Steal::Success(task) => return Some(task),
                Steal::Empty => {}
                Steal::Retry => retry = true,
            }

            // then steal from others global queue
            for &index in unsafe { self.steal_orders.get_unchecked(processor.index) } {
                match unsafe { self.processors.get_unchecked(index) }.pop_global(worker) {
                    Steal::Success(task) => return Some(task),
                    Steal::Empty => {}
                    Steal::Retry => retry = true,
                }
            }
        }
        None
    }

    #[inline(always)]
    pub fn steal_into(&self, worker: &Worker<Task>, processor: &Processor) -> Option<Task> {
        let mut retry = true;
        while retry {
            retry = false;
            for &index in unsafe { self.steal_orders.get_unchecked(processor.index) } {
                match unsafe { self.processors.get_unchecked(index) }.steal_local(worker) {
                    Steal::Success(task) => return Some(task),
                    Steal::Empty => {}
                    Steal::Retry => retry = true,
                }
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

/// Detach current thread from executor pool
///
/// this is useful if you know that you are going to do blocking that longer
/// than blocking threshold.
#[inline(always)]
pub fn detach_current_thread() {
    machine::respawn();
}

static NUM_CPUS: AtomicUsize = AtomicUsize::new(0);

/// Set the number of executor thread
///
/// Analogous to `GOMAXPROCS` in golang,
/// can only be set once and before executor is running,
/// if not set before executor running, it will be the number of available cpu in the host
#[inline(always)]
pub fn set_num_cpus(size: usize) -> Result<(), String> {
    let old_value = NUM_CPUS.compare_and_swap(0, size, Ordering::Relaxed);
    if old_value == 0 {
        Ok(())
    } else {
        Err(format!("num_cpus already set to {}", old_value))
    }
}

/// Get the number of executor thread
///
/// `None` if the executor thread is not run yet
#[inline(always)]
pub fn get_num_cpus() -> Option<usize> {
    let n = NUM_CPUS.load(Ordering::Relaxed);
    if n == 0 {
        None
    } else {
        Some(n)
    }
}

#[inline(always)]
fn lock_num_cpus() -> usize {
    let num_cpus = &NUM_CPUS;
    if num_cpus.load(Ordering::Relaxed) == 0 {
        num_cpus.compare_and_swap(0, std::cmp::max(1, num_cpus::get()), Ordering::Relaxed);
    }
    num_cpus.load(Ordering::Relaxed)
}

#[inline(always)]
fn coprime(a: usize, b: usize) -> bool {
    gcd(a, b) == 1
}

#[inline(always)]
fn gcd(a: usize, b: usize) -> usize {
    let mut p = (a, b);
    while p.1 != 0 {
        p = (p.1, p.0 % p.1);
    }
    p.0
}
