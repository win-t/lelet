// extra doc:
// Inspired by golang runtime, see https://golang.org/s/go11sched
// so understand some terminology like machine and processor will help you
// understand this code.

mod machine;
mod processor;
mod system;
mod task;

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use self::system::System;
use self::task::TaskTag;

type Task = async_task::Task<TaskTag>;

/// Run the task in the background.
///
/// Just like goroutine in golang, there is no way to cancel a task,
/// but unlike goroutine you can `await` the task
///
/// # Panic
///
/// When a task panic, it will abort the entire program
pub fn spawn<F, R>(task: F) -> JoinHandle<R>
where
    F: Future<Output = R> + Send + 'static,
    R: Send + 'static,
{
    let system = System::get();
    let (task, handle) = async_task::spawn(task, move |t| system.push(t), TaskTag::new());
    task.schedule();
    JoinHandle(handle)
}

/// JoinHandle that you can `await` for it
pub struct JoinHandle<R>(async_task::JoinHandle<R, TaskTag>);

impl<R> Future for JoinHandle<R> {
    type Output = R;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.0).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(val)) => Poll::Ready(val),
            Poll::Ready(None) => unreachable!(), // we don't provide api to cancel the task
        }
    }
}
