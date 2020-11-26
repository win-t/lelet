// use std::{
//     sync::{
//         atomic::{AtomicUsize, Ordering},
//         Arc, Condvar, Mutex,
//     },
//     thread,
//     time::{Duration, Instant},
// };

// const IDLE_THRESHOLD: Duration = Duration::from_secs(10);

// type Job = dyn FnOnce() + Send + 'static;

// struct Inner {
//     base: Instant,
//     next_exit: AtomicUsize,
//     job: Mutex<Option<Box<Job>>>,
//     wait_job: Condvar,
// }

// #[derive(Clone)]
// pub struct CachedThread {
//     inner: Arc<Inner>,
// }

// impl CachedThread {
//     pub fn new() -> CachedThread {
//         CachedThread {
//             inner: Arc::new(Inner {
//                 base: Instant::now(),
//                 next_exit: AtomicUsize::new(0),
//                 job: Mutex::new(None),
//                 wait_job: Condvar::new(),
//             }),
//         }
//     }

//     pub fn spawn<F>(&self, f: F)
//     where
//         F: FnOnce() + Send + 'static,
//     {
//         let job = Box::new(f);
//         self.inner.job.try
//         self.inner
//             .sender
//             .try_send(job)
//             .unwrap_or_else(|err| match err {
//                 TrySendError::Full(job) => {
//                     let self2 = self.clone();
//                     let _ = thread::Builder::new().spawn(move || self2.main_loop());
//                     self.inner.sender.send(job).unwrap();
//                 }

//                 // we hold both side of the channel, so it will never be disconnected
//                 TrySendError::Disconnected(_) => unreachable!(),
//             });
//     }

//     fn main_loop(&self) {
//         loop {
//             match self.inner.receiver.recv_timeout(IDLE_THRESHOLD) {
//                 Ok(job) => job(),

//                 Err(RecvTimeoutError::Timeout) => {
//                     let now = Instant::now();
//                     let next_exit = self.inner.next_exit.load(Ordering::Relaxed);
//                     if now.duration_since(self.inner.base).as_secs() as usize >= next_exit {
//                         let new_next_exit = (now + IDLE_THRESHOLD)
//                             .duration_since(self.inner.base)
//                             .as_secs() as usize;

//                         // only 1 thread is allowed to exit per IDLE_THRESHOLD
//                         if next_exit
//                             == self.inner.next_exit.compare_and_swap(
//                                 next_exit,
//                                 new_next_exit,
//                                 Ordering::Relaxed,
//                             )
//                         {
//                             return;
//                         }
//                     }
//                 }

//                 // we hold both side of the channel
//                 Err(RecvTimeoutError::Disconnected) => unreachable!(),
//             }
//         }
//     }
// }
