// extra doc:
// Inspired by golang runtime, see https://golang.org/src/runtime/proc.go
// so understand some terminology like machine and processor will help you
// understand this code.

mod machine;
mod processor;
mod system;
mod task;

use std::future::Future;

use self::system::SYSTEM;
use self::task::TaskTag;

type Task = async_task::Task<TaskTag>;

/// Run the task in the background.
///
/// Just like goroutine in golang, it does not return task handle,
/// if you need synchronization, you can use [`futures-channel`] or [`std channel`] or else
///
/// # Panic
///
/// When a task panic, it will abort the entire program
///
/// [`futures-channel`]: https://docs.rs/futures-channel
/// [`std channel`]: https://doc.rust-lang.org/std/sync/mpsc/fn.channel.html
pub fn spawn<F: Future<Output = ()> + Send + 'static>(task: F) {
  let (task, _) = async_task::spawn(task, |t| SYSTEM.push(t), TaskTag::new());
  task.schedule();
}
