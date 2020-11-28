#![no_std]

use core::cell::UnsafeCell;
use core::fmt;
use core::marker::PhantomData;
use core::ops::Deref;
use core::ops::DerefMut;
use core::sync::atomic::AtomicBool;
use core::sync::atomic::Ordering;

/// A simple lock.
///
/// Intentionally to not providing the `lock`, you can spin loop `try_lock` if you want.
/// You should use [`Mutex`] if you need blocking lock.
///
/// [`Mutex`]: https://doc.rust-lang.org/std/sync/struct.Mutex.html
pub struct SimpleLock<T: ?Sized> {
    locked: AtomicBool,
    value: UnsafeCell<T>,
}

unsafe impl<T: ?Sized + Send> Send for SimpleLock<T> {}
unsafe impl<T: ?Sized + Send> Sync for SimpleLock<T> {}

impl<T> SimpleLock<T> {
    /// Returns a new SimpleLock initialized with `value`.
    pub fn new(value: T) -> SimpleLock<T> {
        SimpleLock {
            locked: AtomicBool::new(false),
            value: UnsafeCell::new(value),
        }
    }
}

impl<T: ?Sized + Default> Default for SimpleLock<T> {
    fn default() -> SimpleLock<T> {
        SimpleLock::new(T::default())
    }
}

impl<T: ?Sized> SimpleLock<T> {
    /// Try to lock.
    pub fn try_lock(&self) -> Option<SimpleLockGuard<T>> {
        if self.locked.swap(true, Ordering::Acquire) {
            None
        } else {
            Some(SimpleLockGuard {
                parent: self,
                _marker: PhantomData,
            })
        }
    }

    /// Is locked ?
    pub fn is_locked(&self) -> bool {
        self.locked.load(Ordering::Relaxed)
    }
}

impl<T> From<T> for SimpleLock<T> {
    fn from(t: T) -> Self {
        SimpleLock::new(t)
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for SimpleLock<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.try_lock() {
            Some(guard) => f.debug_tuple("SimpleLock").field(&&*guard).finish(),
            None => f.write_str("SimpleLock(<locked>)"),
        }
    }
}

/// A guard holding a [`SimpleLock`].
///
/// [`SimpleLock`]: struct.SimpleLock.html
pub struct SimpleLockGuard<'a, T: 'a + ?Sized> {
    parent: &'a SimpleLock<T>,

    // !Send + !Sync
    _marker: PhantomData<*mut ()>,
}

unsafe impl<T: ?Sized + Sync> Sync for SimpleLockGuard<'_, T> {}

impl<T: ?Sized> Drop for SimpleLockGuard<'_, T> {
    fn drop(&mut self) {
        self.parent.locked.store(false, Ordering::Release);
    }
}

impl<T: ?Sized> Deref for SimpleLockGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.parent.value.get() }
    }
}

impl<T: ?Sized> DerefMut for SimpleLockGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.parent.value.get() }
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for SimpleLockGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for SimpleLockGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        (**self).fmt(f)
    }
}
