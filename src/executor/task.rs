use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(feature = "tracing")]
use log::trace;

#[cfg(feature = "tracing")]
static TASK_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

pub struct TaskTag {
    #[cfg(feature = "tracing")]
    id: usize,

    schedule_index_hint: AtomicUsize,
}

impl TaskTag {
    pub fn new() -> TaskTag {
        #[allow(clippy::let_and_return)]
        let tag = TaskTag {
            #[cfg(feature = "tracing")]
            id: TASK_ID_COUNTER.fetch_add(1, Ordering::Relaxed),

            schedule_index_hint: AtomicUsize::new(usize::MAX),
        };

        #[cfg(feature = "tracing")]
        trace!("{:?} is created", tag);

        tag
    }

    pub fn get_schedule_index_hint(&self) -> usize {
        self.schedule_index_hint.load(Ordering::Relaxed)
    }

    pub fn set_schedule_index_hint(&self, index: usize) {
        self.schedule_index_hint.store(index, Ordering::Relaxed);
    }
}

#[cfg(feature = "tracing")]
impl std::fmt::Debug for TaskTag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!("Task({})", self.id))
    }
}

#[cfg(feature = "tracing")]
impl Drop for TaskTag {
    fn drop(&mut self) {
        trace!("{:?} is destroyed", self);
    }
}
