use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use crossbeam_channel::{bounded, Receiver, Sender, TrySendError};
use once_cell::sync::Lazy;

#[cfg(feature = "tracing")]
use std::cell::Cell;
#[cfg(feature = "tracing")]
use std::sync::atomic::AtomicUsize;

#[cfg(feature = "tracing")]
use log::trace;

use crate::utils::monotonic_ms;

const IDLE_THRESHOLD: Duration = Duration::from_secs(60);

#[cfg(feature = "tracing")]
static THREAD_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

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

struct Pool {
  last_exit: AtomicU64,
  sender: Sender<Job>,
  receiver: Receiver<Job>,
}

static POOL: Lazy<Pool> = Lazy::new(|| {
  let (sender, receiver) = bounded(0);
  Pool {
    last_exit: AtomicU64::new(0),
    sender,
    receiver,
  }
});

impl Pool {
  fn put_job(&self, job: Job) {
    self.sender.try_send(job).unwrap_or_else(|err| match err {
      TrySendError::Full(job) => {
        let receiver = self.receiver.clone();
        thread::spawn(move || thread_main(receiver));
        self.sender.send(job).unwrap();
      }
      TrySendError::Disconnected(_) => unreachable!(), // we hold both side of the channel
    });
  }
}

fn thread_main(receiver: Receiver<Job>) {
  #[cfg(feature = "tracing")]
  THREAD_ID.with(|id| {
    id.0.set(THREAD_ID_COUNTER.fetch_add(1, Ordering::Relaxed));
    trace!("{:?} is created", id);
  });

  loop {
    match receiver.recv_timeout(IDLE_THRESHOLD) {
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
        // only 1 thread is allowed to exit per IDLE_THRESHOLD
        let now = monotonic_ms();
        let last_exit = POOL.last_exit.load(Ordering::Relaxed);

        // make sure to check first then CAS,
        // because CAS have side effect
        #[allow(clippy::collapsible_if)]
        if now - last_exit >= (IDLE_THRESHOLD.as_millis() as u64) {
          if POOL
            .last_exit
            .compare_and_swap(last_exit, now, Ordering::Relaxed)
            == last_exit
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

pub fn spawn_box(job: Job) {
  POOL.put_job(job);
}
