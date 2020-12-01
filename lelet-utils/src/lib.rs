use std::future::Future;
use std::marker::PhantomData;
use std::mem;
use std::pin::Pin;
use std::process::abort;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::thread::{self, Thread};

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
