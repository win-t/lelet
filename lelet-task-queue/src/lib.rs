//! Task queue
//!
//! A global task queue for buildnig task executor
//!
//! any task in the queue can be polled from multiple thread

mod deque;
mod local;
mod poll_fn;
mod shared;

use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::Poll;

use crate::poll_fn::poll_fn;

/// Push a task
pub fn push(task: impl Future<Output = ()> + Send + 'static) {
    shared::push(task);
}

/// Push a local task
pub fn push_local(task: impl Future<Output = ()> + 'static) {
    local::push(task);
}

/// Poller
pub struct Poller<'a> {
    local: local::Poller<'a>,
    shared: shared::Poller<'a>,

    // !Send + !Sync
    _marker: PhantomData<*mut ()>,
}

/// Get [`Poller`] for polling task
///
/// [`Poller`]: struct.Poller.html
pub fn poller<'a>() -> Poller<'a> {
    Poller {
        local: local::poller(),
        shared: shared::poller(),
        _marker: PhantomData,
    }
}

impl<'a> Poller<'a> {
    /// Poll one task
    ///
    /// return false if no more task to be polled
    ///
    /// note that, when this function return false, it doesn't mean that is the queue is empty,
    /// some task maybe is in pending state
    ///
    /// use [`wait`] to check if the queue is already empty
    ///
    /// [`wait`]: struct.Poller.html#method.wait
    #[inline(always)]
    pub fn poll_one(&self) -> bool {
        if self.local.poll_one() {
            return true;
        }

        if self.shared.poll_one() {
            return true;
        }

        false
    }

    /// Wait
    ///
    /// return true if there is a task to be polled, or false if the queue becomes empty
    #[inline(always)]
    pub async fn wait(&self) -> bool {
        let mut local_wait = Some(self.local.wait());
        let mut shared_wait = Some(self.shared.wait());
        poll_fn(move |cx| {
            assert!(
                local_wait.is_none() && shared_wait.is_none(),
                "calling poll when future is already done"
            );

            if local_wait.is_some() {
                let f = local_wait.as_mut().unwrap();
                match unsafe { Pin::new_unchecked(f) }.poll(cx) {
                    Poll::Ready(ok) => {
                        local_wait.take();
                        if ok {
                            return Poll::Ready(true);
                        }
                    }
                    Poll::Pending => {}
                }
            }

            if shared_wait.is_some() {
                let f = shared_wait.as_mut().unwrap();
                match unsafe { Pin::new_unchecked(f) }.poll(cx) {
                    Poll::Ready(ok) => {
                        shared_wait.take();
                        if ok {
                            return Poll::Ready(true);
                        }
                    }
                    Poll::Pending => {}
                }
            }

            if local_wait.is_some() || shared_wait.is_some() {
                Poll::Pending
            } else {
                Poll::Ready(false)
            }
        })
        .await
    }
}
