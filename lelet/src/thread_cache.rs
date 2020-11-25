use std::{
    ptr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Once,
    },
    thread,
    time::{Duration, Instant},
};

use crossbeam_channel::{bounded, Receiver, RecvTimeoutError, Sender, TrySendError};

const IDLE_THRESHOLD: Duration = Duration::from_secs(10);

type Job = Box<dyn FnOnce() + Send>;

struct Cache {
    base: Instant,
    next_exit: AtomicUsize,
    sender: Sender<Job>,
    receiver: Receiver<Job>,
}

impl Cache {
    fn new() -> Cache {
        let (sender, receiver) = bounded(0);
        Cache {
            base: Instant::now(),
            next_exit: AtomicUsize::new(0),
            sender,
            receiver,
        }
    }

    fn run(&'static self, job: Job) {
        self.sender.try_send(job).unwrap_or_else(|err| match err {
            TrySendError::Full(job) => {
                let _ = thread::Builder::new().spawn(move || self.main_loop());
                self.sender.send(job).unwrap();
            }

            // we hold both side of the channel, so it will never be disconnected
            TrySendError::Disconnected(_) => unreachable!(),
        });
    }

    fn main_loop(&self) {
        loop {
            match self.receiver.recv_timeout(IDLE_THRESHOLD) {
                Ok(job) => job(),

                Err(RecvTimeoutError::Timeout) => {
                    let now = Instant::now();
                    let next_exit = self.next_exit.load(Ordering::Relaxed);
                    if now.duration_since(self.base).as_secs() as usize >= next_exit {
                        let new_next_exit =
                            (now + IDLE_THRESHOLD).duration_since(self.base).as_secs() as usize;

                        // only 1 thread is allowed to exit per IDLE_THRESHOLD
                        if next_exit
                            == self.next_exit.compare_and_swap(
                                next_exit,
                                new_next_exit,
                                Ordering::Relaxed,
                            )
                        {
                            return;
                        }
                    }
                }

                // we hold both side of the channel
                Err(RecvTimeoutError::Disconnected) => unreachable!(),
            }
        }
    }
}

impl Cache {
    fn get() -> &'static Cache {
        static ONCE: Once = Once::new();
        static mut CACHE: *const Cache = ptr::null();
        ONCE.call_once(|| unsafe { CACHE = Box::into_raw(Box::new(Cache::new())) });
        unsafe { &*CACHE }
    }
}

pub fn spawn(job: Job) {
    Cache::get().run(job)
}
