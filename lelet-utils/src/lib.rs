use std::cell::UnsafeCell;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use crossbeam_utils::Backoff;

pub fn abort_on_panic(f: impl FnOnce()) {
    struct Bomb;

    impl Drop for Bomb {
        fn drop(&mut self) {
            std::process::abort();
        }
    }

    let bomb = Bomb;
    f();
    std::mem::forget(bomb);
}

#[macro_export]
macro_rules! defer {
  ($($body:tt)*) => {
      let _guard = {
          struct Guard<F: FnOnce()>(Option<F>);

          impl<F: FnOnce()> Drop for Guard<F> {
              fn drop(&mut self) {
                  (self.0).take().map(|f| f());
              }
          }

          Guard(Some(|| {
              let _: () = { $($body)* };
          }))
      };
  };
}

pub struct Yields(usize);

impl Yields {
    pub fn new(times: usize) -> Yields {
        Yields(times)
    }
}

impl Future for Yields {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 == 0 {
            Poll::Ready(())
        } else {
            self.0 -= 1;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

pub async fn yield_now() {
    Yields(1).await;
}

/// A simple spinlock.
///
/// copied from [`crossbeam_channel`]`::utils`
/// because it is private in that crate
///
/// [`crossbeam_channel`]: https://docs.rs/crossbeam-channel
pub struct Spinlock<T> {
    flag: AtomicBool,
    value: UnsafeCell<T>,
}

unsafe impl<T> Sync for Spinlock<T> {}

impl<T> Spinlock<T> {
    /// Returns a new spinlock initialized with `value`.
    pub fn new(value: T) -> Spinlock<T> {
        Spinlock {
            flag: AtomicBool::new(false),
            value: UnsafeCell::new(value),
        }
    }

    /// Locks the spinlock.
    pub fn lock(&self) -> SpinlockGuard<'_, T> {
        let backoff = Backoff::new();
        while self.flag.swap(true, Ordering::Acquire) {
            backoff.snooze();
        }
        SpinlockGuard { parent: self }
    }
}

/// A guard holding a spinlock locked.
pub struct SpinlockGuard<'a, T: 'a> {
    parent: &'a Spinlock<T>,
}

impl<'a, T> Drop for SpinlockGuard<'a, T> {
    fn drop(&mut self) {
        self.parent.flag.store(false, Ordering::Release);
    }
}

impl<'a, T> std::ops::Deref for SpinlockGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe { &*self.parent.value.get() }
    }
}

impl<'a, T> std::ops::DerefMut for SpinlockGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.parent.value.get() }
    }
}
