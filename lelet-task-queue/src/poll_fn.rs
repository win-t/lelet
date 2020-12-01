// copied from nightly rust, with some modification

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};

/// Creates a future that wraps a function returning `Poll`.
///
/// Polling the future delegates to the wrapped function.
pub fn poll_fn<T, F>(f: F) -> impl Future<Output = T>
where
    F: FnMut(&mut Context<'_>) -> Poll<T>,
{
    PollFn { f }
}

struct PollFn<F> {
    f: F,
}

impl<F> Unpin for PollFn<F> {}

impl<T, F> Future for PollFn<F>
where
    F: FnMut(&mut Context<'_>) -> Poll<T>,
{
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        (&mut self.f)(cx)
    }
}
