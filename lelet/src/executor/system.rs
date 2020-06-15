use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, Once};
use std::thread;
use std::time::Duration;

use crossbeam_utils::sync::{Parker, Unparker};
use crossbeam_utils::CachePadded;

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
    processors: Vec<Processor>,

    /// for blocking detection
    tick: AtomicU64,

    sysmon_parker: SimpleLock<Parker>,
    sysmon_unparker: Unparker,

    processors_parker: Mutex<Parker>,
    processors_unparker: Unparker,
    notif_count: CachePadded<AtomicUsize>,
}

// just to make sure
impl Drop for System {
    fn drop(&mut self) {
        eprintln!("System must not be dropped once created");
        std::process::abort();
    }
}

impl System {
    /// create new system and leak it
    fn new(num_cpus: usize) -> &'static System {
        #[cfg(feature = "tracing")]
        trace!("Creating system");

        let processors: Vec<Processor> = (0..num_cpus).map(|_| Processor::new()).collect();

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

        // do some validity check
        assert!(!processors.is_empty());
        assert_eq!(processors.len(), steal_orders.len());
        for (i, s) in steal_orders.iter().enumerate() {
            assert!((0..num_cpus).eq(std::iter::once(i)
                .chain(s.clone().into_iter())
                .collect::<std::collections::BinaryHeap<usize>>()
                .into_sorted_vec()));
        }

        let sysmon_parker = Parker::new();
        let sysmon_unparker = sysmon_parker.unparker().clone();

        let processors_parker = Parker::new();
        let processors_unparker = processors_parker.unparker().clone();

        // we need fix memory location to pass to Processor::set_system
        // alloc in heap, and leak it
        let system_raw = Box::into_raw(Box::new(System {
            processors,

            tick: AtomicU64::new(0),

            sysmon_parker: SimpleLock::new(sysmon_parker),
            sysmon_unparker,

            processors_parker: Mutex::new(processors_parker),
            processors_unparker,
            notif_count: CachePadded::new(AtomicUsize::new(0)),
        }));

        let system: &'static System = unsafe { &*system_raw };

        let mut others: Vec<Option<(usize, Vec<&'static Processor>)>> = steal_orders
            .iter()
            .enumerate()
            .map(|(i, o)| Some((i, o.iter().map(|&i| &system.processors[i]).collect())))
            .collect();

        // although we temporary broke the reference invariant (mutable and immutable
        // are live in same time) here, this is safe because:
        // we only use mutable reference to call Processor::set_system(system, others),
        // we doesn't provide public direct/indirect access to System::processors[i]
        // from first argument, and in second argument, others[i] doesn't contain System::processors[i]
        // so code outside this block will not observe this broken invariant
        for (i, p) in unsafe { &mut *system_raw }
            .processors
            .iter_mut()
            .enumerate()
        {
            let o = others[i].take().unwrap();
            assert_eq!(i, o.0);

            p.set_system(system, o.1);
        }

        system
    }

    #[inline(always)]
    fn sysmon_run(&'static self) {
        #[cfg(feature = "tracing")]
        trace!("Sysmon is running");

        // this will also ensure that only one sysmon thread is running
        let parker = self.sysmon_parker.try_lock().unwrap();

        // spawn machine for every processor
        self.processors.iter().for_each(machine::spawn);

        loop {
            let check_tick = self.tick.fetch_add(1, Ordering::Relaxed) + 1;

            thread::sleep(BLOCKING_THRESHOLD);

            if !self.is_empty() {
                for p in &self.processors {
                    if p.get_last_seen() < check_tick {
                        #[cfg(feature = "tracing")]
                        trace!("{:?} is blocked, spawn new machine for it", p);

                        machine::spawn(p);
                    }
                }
            } else {
                #[cfg(feature = "tracing")]
                trace!("Sysmon entering sleep");

                parker.park();

                #[cfg(feature = "tracing")]
                trace!("Sysmon exiting sleep");
            }
        }
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.processors.iter().all(|p| p.is_empty())
    }

    #[inline(always)]
    pub fn processors_wait_notif(&self) {
        let mut lock: Option<MutexGuard<Parker>> = None;
        loop {
            let notif_count = self.notif_count.load(Ordering::Relaxed);
            if notif_count > 0 {
                if self.notif_count.compare_and_swap(
                    notif_count,
                    notif_count - 1,
                    Ordering::Relaxed,
                ) == notif_count
                {
                    drop(lock);
                    break;
                }
                std::sync::atomic::spin_loop_hint();
            } else {
                match lock.as_ref() {
                    Some(parker) => parker.park(),
                    None => lock = Some(self.processors_parker.lock().unwrap()),
                }
            }
        }
    }

    #[inline(always)]
    pub fn processors_send_notif(&self) {
        loop {
            let notif_count = self.notif_count.load(Ordering::Relaxed);
            if notif_count >= self.processors.len() {
                break;
            }
            if self
                .notif_count
                .compare_and_swap(notif_count, notif_count + 1, Ordering::Relaxed)
                == notif_count
            {
                self.processors_unparker.unpark();
                break;
            }
            std::sync::atomic::spin_loop_hint();
        }
    }

    #[inline(always)]
    pub fn push(&self, task: Task) {
        match machine::direct_push(task) {
            Ok(()) => {}
            Err(task) => {
                let mut p = task.tag().processor_hint();
                if p.is_null() {
                    p = unsafe { self.processors.get_unchecked(0) };
                }

                unsafe { &*p }.push_global(task);
            }
        }

        self.sysmon_unparker.unpark();
        self.processors_send_notif();
    }

    #[inline(always)]
    pub fn now(&self) -> u64 {
        self.tick.load(Ordering::Relaxed)
    }
}

#[inline(always)]
pub fn get() -> &'static System {
    static SYSTEM: (AtomicPtr<System>, Once) = (AtomicPtr::new(ptr::null_mut()), Once::new());
    SYSTEM.1.call_once(|| {
        thread::spawn(move || abort_on_panic(move || get().sysmon_run()));
        let system = System::new(lock_num_cpus()) as *const _ as *mut _;
        SYSTEM.0.store(system, Ordering::Relaxed);
    });
    unsafe { &*SYSTEM.0.load(Ordering::Relaxed) }
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
