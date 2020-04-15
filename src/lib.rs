//! Dynamic task executor.
//!
//! Inspired by golang runtime.
//!
//! It is okay to do blocking inside a task, the executor will
//! detect this, and scale the thread pool.
//!
//! ## Example
//!
//! ```rust,ignore
//! use std::thread;
//! use std::time::Duration;
//!
//! use futures_timer::Delay;
//!
//! fn main() {
//!     lelet::spawn(async {
//!         for _ in 0..10 {
//!             Delay::new(Duration::from_secs(1)).await;
//!             println!("Non-blocking Hello World");
//!         }
//!     });
//!
//!     lelet::spawn(async {
//!         for _ in 0..10 {
//!             thread::sleep(Duration::from_secs(1));
//!             println!("Blocking Hello World");
//!         }
//!     });
//!
//!     thread::sleep(Duration::from_secs(11));
//! }
//! ```

#[macro_use]
mod utils;

mod executor;
mod thread_pool;

pub use executor::spawn;
