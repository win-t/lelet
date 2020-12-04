use std::future::Future;
use std::marker::PhantomData;
use std::mem;
use std::pin::Pin;
use std::process::abort;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::RawWaker;
use std::task::RawWakerVTable;
use std::task::Waker;
use std::thread;
use std::thread::Thread;

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
        |clone_me| {
            let clone_me = unsafe { Arc::from_raw(clone_me as *const Unparker) };
            mem::forget(clone_me.clone());
            RawWaker::new(Arc::into_raw(clone_me) as *const (), &VTABLE)
        },
        |wake_me| unsafe { Arc::from_raw(wake_me as *const Unparker) }.unpark(),
        |wake_me_by_ref| unsafe { &*(wake_me_by_ref as *const Unparker) }.unpark(),
        |drop_me| drop(unsafe { Arc::from_raw(drop_me as *const Unparker) }),
    );

    let parker = Parker::new();

    let waker = {
        let raw = RawWaker::new(Arc::into_raw(parker.unparker()) as *const (), &VTABLE);
        unsafe { Waker::from_raw(raw) }
    };

    let mut f = f;
    let mut cx = Context::from_waker(&waker);
    loop {
        match unsafe { Pin::new_unchecked(&mut f) }.poll(&mut cx) {
            Poll::Pending => parker.park(),
            Poll::Ready(val) => return val,
        }
    }
}

struct ParkerInner {
    parked: AtomicBool,
    thread: Thread,

    // !Send + !Sync
    _marker: PhantomData<*mut ()>,
}

impl ParkerInner {
    fn new() -> ParkerInner {
        ParkerInner {
            parked: AtomicBool::new(false),
            thread: thread::current(),
            _marker: PhantomData,
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
        let cloned = self.0.clone();
        unsafe { mem::transmute(cloned) }
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
