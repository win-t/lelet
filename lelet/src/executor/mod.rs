// extra doc:
// Inspired by golang runtime, see https://golang.org/s/go11sched
// understanding some terminology like machine and processor will help you
// understand this code.
//
// Noticable difference with golang scheduler
// 1. new machine steal the processor from old one when it run the processor instead of
//    acquire/release from global list
// 2. machine don't live without a processor (when stolen by others),
//    it must exit as soon as possible
// 3. each processor have dedicated global queue

mod machine;
mod processor;
mod system;
mod task;

pub use system::mark_blocking;

pub use system::get_num_cpus;
pub use system::set_num_cpus;

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

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
pub fn spawn<T, R>(task: T) -> JoinHandle<R>
where
    T: Future<Output = R> + Send + 'static,
    R: Send + 'static,
{
    let system = system::get();
    let (task, handle) = async_task::spawn(task, move |task| system.push(task), TaskTag::new());
    task.schedule();
    JoinHandle(handle)
}

/// JoinHandle that you can `await` for
pub struct JoinHandle<R>(async_task::JoinHandle<R, TaskTag>);

impl<R> Future for JoinHandle<R> {
    type Output = R;

    #[inline(always)]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        match Pin::new(&mut self.0).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(val)) => Poll::Ready(val),
            Poll::Ready(None) => unreachable!(), // we don't provide api to cancel the task
        }
    }
}
