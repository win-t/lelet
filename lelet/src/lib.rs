//! Dynamic task executor.
//!
//! Inspired by golang runtime.
//!
//! It is okay to do blocking inside a task, the executor will
//! detect this, and scale the thread pool.
//!
//! But, please keep note that every time you do blocking, it will
//! create thread via [`thread::spawn`], and the number of thread you can create
//! is not unlimited, so the number of blocking task you can [`spawn`] is also not unlimited
//!
//! [`thread::spawn`]: https://doc.rust-lang.org/std/thread/fn.spawn.html
//! [`spawn`]: fn.spawn.html
//!
//! # Example
//!
//! ```rust,ignore
//! use std::thread;
//! use std::time::Duration;
//!
//! use futures_timer::Delay;
//!
//! lelet::spawn(async {
//!     for _ in 0..10 {
//!         Delay::new(Duration::from_secs(1)).await;
//!         println!("Non-blocking Hello World");
//!     }
//! });
//!
//! lelet::spawn(async {
//!     for _ in 0..10 {
//!         thread::sleep(Duration::from_secs(1));
//!         println!("Blocking Hello World");
//!     }
//! });
//!
//! thread::sleep(Duration::from_secs(11));
//! ```

#[doc(hidden)]
pub mod thread_pool;

mod executor;
pub use executor::spawn;
pub use executor::JoinHandle;

#[doc(hidden)]
pub use executor::get_num_cpus;
#[doc(hidden)]
pub use executor::set_num_cpus;

#[doc(hidden)]
pub use executor::detach_current_thread;
