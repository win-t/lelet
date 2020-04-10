use std::hint::unreachable_unchecked;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use crossbeam_channel::{bounded, Receiver, Sender, TrySendError};
use once_cell::sync::Lazy;

use crate::utils::monotonic_ms;

const IDLE_THRESHOLD: Duration = Duration::from_secs(60);

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
      // will never disconnected, because we holding reciever for cloning
      TrySendError::Disconnected(_) => unsafe { unreachable_unchecked() },
    });
  }
}

fn thread_main(receiver: Receiver<Job>) {
  loop {
    match receiver.recv_timeout(IDLE_THRESHOLD) {
      Ok(job) => job(),
      _ => {
        // only 1 thread is allowed to exit per IDLE_THRESHOLD
        let now = monotonic_ms();
        let last_exit = POOL.last_exit.load(Ordering::Relaxed);
        if now - last_exit >= (IDLE_THRESHOLD.as_millis() as u64) {
          if POOL
            .last_exit
            .compare_and_swap(last_exit, now, Ordering::Relaxed)
            == last_exit
          {
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
