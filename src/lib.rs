//! Dynamic task executor.
//!
//! Inspired by golang runtime.
//!
//! It is okay to do blocking inside a task, the executor will
//! detect this, and scale the thread pool.

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;

#[macro_use]
mod utils;

mod executor;
mod thread_pool;

pub use executor::spawn;
