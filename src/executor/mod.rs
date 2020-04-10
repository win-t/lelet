// Design doc:
// Every worker thread must have a "processor".
// only thread with valid "processor" can execute task.
// there only fix number (number of cpus) of valid "processor" at same time.
// when a processor is blocking, it will fail to update "last_seen" and then
// sysmon thread will mark that "processor" is invalid (downgrade it into normal thread)
// and replace it with new processor.
// A thread without valid "processor" should terminate and should not execute more task.

mod processor;
mod runtime;

use std::cell::RefCell;
use std::future::Future;
use std::rc::Rc;
use std::time::Duration;

use crossbeam_deque::Worker;

use self::runtime::RUNTIME;

const BLOCKING_THRESHOLD: Duration = Duration::from_millis(10);

type Task = async_task::Task<()>;

thread_local! {
  // Indicating that current thread have worker queue
  static WORKER: RefCell<Option<Rc<Worker<Task>>>> = RefCell::new(None);
}

// if current thread have worker queue, schedule the task to it
// otherwise schedule it to global queue
fn schedule_task(task: Task) {
  WORKER.with(|worker| {
    let worker = worker.borrow();
    match worker.as_ref() {
      Some(worker) => worker.push(task),
      None => RUNTIME.push_task(task),
    }
  });
}

/// Run the task.
///
/// It's okay to do blocking operation in the task, the executor will detect
/// this and scale the pool.
pub fn spawn<F: Future<Output = ()> + Send + 'static>(f: F) {
  let (task, _) = async_task::spawn(f, schedule_task, ());
  task.schedule();
}
