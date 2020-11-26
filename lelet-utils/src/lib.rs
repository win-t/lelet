use std::{
    cell::UnsafeCell,
    fmt,
    future::Future,
    marker::PhantomData,
    mem,
    ops::{Deref, DerefMut},
    pin::Pin,
    process::abort,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
    thread::{self, Thread},
};

/// Call [`abort`] when `f` panic
///
/// [`abort`]: https://doc.rust-lang.org/std/process/fn.abort.html
pub fn abort_on_panic<R>(f: impl FnOnce() -> R) -> R {
    struct Bomb;

    impl Drop for Bomb {
        fn drop(&mut self) {
            abort();
        }
    }

    let bomb = Bomb;

    let r = f();

    mem::forget(bomb);

    r
}

/// Defer the execution until the scope is done
#[macro_export]
macro_rules! defer {
  ($($body:tt)*) => {
      let _guard = {
          struct Guard<F: FnOnce()>(Option<F>);

          impl<F: FnOnce()> Drop for Guard<F> {
            fn drop(&mut self) {
                  self.0.take().map(|f| f());
              }
          }

          Guard(Some(|| {
              let _: () = { $($body)* };
          }))
      };
  };
}

/// Extracts the successful type of a `Poll<T>`.
///
/// This macro bakes in propagation of `Pending` signals by returning early.
#[macro_export]
macro_rules! ready {
    ($e:expr $(,)?) => {
        match $e {
            Poll::Ready(t) => t,
            Poll::Pending => return Poll::Pending,
        }
    };
}

/// Future that will yield multiple times
#[derive(Debug)]
pub struct Yields(pub usize);

impl Future for Yields {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<()> {
        if self.0 == 0 {
            Poll::Ready(())
        } else {
            self.0 -= 1;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

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

/// Block current thread until f is complete
pub fn block_on<F: Future>(f: F) -> F::Output {
    static VTABLE: RawWakerVTable = RawWakerVTable::new(
        //
        // clone: unsafe fn(*const ()) -> RawWaker
        |unparker| unsafe {
            let unparker = Arc::from_raw(unparker as *const Unparker);
            mem::forget(unparker.clone()); // to inc the Arc's ref count
            RawWaker::new(Arc::into_raw(unparker) as *const (), &VTABLE)
        },
        //
        // wake: unsafe fn(*const ())
        |unparker| unsafe {
            Arc::from_raw(unparker as *const Unparker).unpark();
        },
        //
        // wake_by_ref: unsafe fn(*const ())
        |unparker| unsafe {
            (&*(unparker as *const Unparker)).unpark();
        },
        //
        // drop: unsafe fn(*const ())
        |unparker| unsafe {
            drop(Arc::from_raw(unparker as *const Unparker));
        },
    );

    let parker = Parker::new();

    let waker = unsafe {
        Waker::from_raw(RawWaker::new(
            Arc::into_raw(parker.unparker()) as *const (),
            &VTABLE,
        ))
    };

    // pin f to the stack
    let mut f = f;
    let mut f = unsafe { Pin::new_unchecked(&mut f) };

    let mut cx = Context::from_waker(&waker);

    loop {
        match f.as_mut().poll(&mut cx) {
            Poll::Pending => parker.park(),
            Poll::Ready(val) => return val,
        }
    }
}

struct ParkerInner {
    parked: AtomicBool,
    thread: Thread,

    // !Send + !Sync
    _mark: PhantomData<*mut ()>,
}

impl ParkerInner {
    fn new() -> ParkerInner {
        ParkerInner {
            parked: AtomicBool::new(false),
            thread: thread::current(),
            _mark: PhantomData,
        }
    }

    fn park(&self) {
        // wait while the flag is set
        while !self.parked.load(Ordering::Relaxed) {
            thread::park();
        }

        // consume the flag
        self.parked.store(false, Ordering::Relaxed);
    }

    fn unpark(&self) {
        // set the flag
        self.parked.store(true, Ordering::Relaxed);

        self.thread.unpark();
    }
}

/// A thread parking primitive
pub struct Parker(Arc<ParkerInner>);

impl Parker {
    /// Create new Parker
    pub fn new() -> Parker {
        Parker(Arc::new(ParkerInner::new()))
    }

    /// Park current thread
    pub fn park(&self) {
        self.0.park();
    }

    /// Return the associated unparker
    pub fn unparker(&self) -> Arc<Unparker> {
        unsafe { mem::transmute(self.0.clone()) }
    }
}

/// Unparker for the associated parked thread
pub struct Unparker(ParkerInner);

impl Unparker {
    /// Unpark the associated parked thread
    pub fn unpark(&self) {
        self.0.unpark();
    }
}

unsafe impl Send for Unparker {}
unsafe impl Sync for Unparker {}
