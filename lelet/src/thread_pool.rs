use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_channel::{bounded, Receiver, Sender, TrySendError};
use once_cell::sync::Lazy;

#[cfg(feature = "tracing")]
use std::cell::Cell;

#[cfg(feature = "tracing")]
use log::trace;

const IDLE_THRESHOLD: Duration = Duration::from_secs(60);

#[cfg(feature = "tracing")]
pub struct ThreadID(Cell<usize>);

#[cfg(feature = "tracing")]
impl std::fmt::Debug for ThreadID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!("Thread({})", self.0.get()))
    }
}

#[cfg(feature = "tracing")]
thread_local! {
  pub static THREAD_ID: ThreadID = ThreadID(Cell::new(usize::MAX));
}

type Job = Box<dyn FnOnce() + Send>;

// singleton: POOL
struct Pool {
    base: Instant,
    next_exit: AtomicUsize,
    sender: Sender<Job>,
    receiver: Receiver<Job>,
}

impl Pool {
    fn new() -> Pool {
        let (sender, receiver) = bounded(0);
        Pool {
            base: Instant::now(),
            next_exit: AtomicUsize::new(0),
            sender,
            receiver,
        }
    }

    fn put_job(&'static self, job: Job) {
        self.sender.try_send(job).unwrap_or_else(|err| match err {
            TrySendError::Full(job) => {
                thread::spawn(move || self.run());
                self.sender.send(job).unwrap();
            }

            // we hold both side of the channel
            TrySendError::Disconnected(_) => unreachable!(),
        });
    }

    fn run(&self) {
        #[cfg(feature = "tracing")]
        THREAD_ID.with(|id| {
            static THREAD_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);
            id.0.set(THREAD_ID_COUNTER.fetch_add(1, Ordering::Relaxed));
            trace!("{:?} is created", id);
        });

        loop {
            match self.receiver.recv_timeout(IDLE_THRESHOLD) {
                Ok(job) => {
                    #[cfg(feature = "tracing")]
                    THREAD_ID.with(|id| {
                        trace!("{:?} is running", id);
                    });

                    job();

                    #[cfg(feature = "tracing")]
                    THREAD_ID.with(|id| {
                        trace!("{:?} is done and cached for reused", id);
                    });
                }

                _ => {
                    let now = Instant::now();
                    let next_exit = self.next_exit.load(Ordering::Relaxed);
                    if now.duration_since(self.base).as_secs() as usize >= next_exit {
                        let new_next_exit =
                            (now + IDLE_THRESHOLD).duration_since(self.base).as_secs() as usize;

                        // only 1 thread is allowed to exit per IDLE_THRESHOLD
                        // ensure it via CAS
                        if self.next_exit.compare_and_swap(
                            next_exit,
                            new_next_exit,
                            Ordering::Relaxed,
                        ) == next_exit
                        {
                            #[cfg(feature = "tracing")]
                            THREAD_ID.with(|id| {
                                trace!("{:?} is exiting", id);
                            });

                            return;
                        }
                    }
                }
            }
        }
    }
}

pub fn spawn_box(job: Job) {
    static POOL: Lazy<Pool> = Lazy::new(Pool::new);
    POOL.put_job(job);
}
