use std::cell::UnsafeCell;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::thread;

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

pub struct Yields(pub usize);

impl Future for Yields {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        match self.0 {
            0 => Poll::Ready(()),
            _ => {
                self.0 -= 1;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

/// A simple spinlock.
pub struct Spinlock<T: ?Sized> {
    locked: AtomicBool,
    value: UnsafeCell<T>,
}

unsafe impl<T: ?Sized + Send> Sync for Spinlock<T> {}

impl<T> Spinlock<T> {
    /// Returns a new spinlock initialized with `value`.
    pub fn new(value: T) -> Spinlock<T> {
        Spinlock {
            locked: AtomicBool::new(false),
            value: UnsafeCell::new(value),
        }
    }

    /// Try to lock the spinlock.
    #[inline(always)]
    pub fn try_lock(&self) -> Option<SpinlockGuard<'_, T>> {
        if self.locked.swap(true, Ordering::Acquire) {
            None
        } else {
            Some(SpinlockGuard { parent: self })
        }
    }

    /// Locks the spinlock.
    #[inline(always)]
    pub fn lock(&self) -> SpinlockGuard<'_, T> {
        let backoff = Backoff::new();
        loop {
            if let Some(guard) = self.try_lock() {
                return guard;
            }

            if backoff.is_completed() {
                thread::yield_now()
            } else {
                backoff.snooze()
            }
        }
    }
}

/// A guard holding a spinlock locked.
pub struct SpinlockGuard<'a, T: 'a> {
    parent: &'a Spinlock<T>,
}

impl<'a, T> Drop for SpinlockGuard<'a, T> {
    fn drop(&mut self) {
        self.parent.locked.store(false, Ordering::Release);
    }
}

impl<'a, T> std::ops::Deref for SpinlockGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.parent.value.get() }
    }
}

impl<'a, T> std::ops::DerefMut for SpinlockGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.parent.value.get() }
    }
}
