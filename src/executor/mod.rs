// Design doc:
// Every worker thread must have a "processor".
// only thread with valid "processor" can execute task.
// there only fix number (number of cpus) of valid "processor" at same time.
// when a processor is blocking, it will fail to update "last_seen" and then
// sysmon thread will mark that "processor" is invalid (downgrade it into normal thread)
// and replace it with new processor.
// A thread without valid "processor" should terminate and should not execute more task.

mod processor;
mod sysmon;

use std::future::Future;

use self::processor::schedule_task;

type Task = async_task::Task<()>;

/// Run the task.
///
/// It's okay to do blocking operation in the task, the executor will detect
/// this and scale the pool.
pub fn spawn<F: Future<Output = ()> + Send + 'static>(f: F) {
  let (task, _) = async_task::spawn(f, schedule_task, ());
  task.schedule();
}
