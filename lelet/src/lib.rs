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

#[doc(hidden)]
pub mod thread_cache;

mod executor;
pub use executor::spawn;
pub use executor::JoinHandle;

pub use executor::get_num_cpus;
pub use executor::set_num_cpus;

pub use lelet_utils::block_on;

#[doc(hidden)]
pub use executor::detach_current_thread;

#[doc(hidden)]
#[inline(always)]
pub async fn yield_now() {
    lelet_utils::Yields(1).await;
}
