#[cfg(feature = "tracing")]
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(feature = "tracing")]
use log::trace;

pub struct TaskTag {
    #[cfg(feature = "tracing")]
    id: usize,
}

impl TaskTag {
    pub fn new() -> TaskTag {
        #[cfg(feature = "tracing")]
        static TASK_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

        #[allow(clippy::let_and_return)]
        let tag = TaskTag {
            #[cfg(feature = "tracing")]
            id: TASK_ID_COUNTER.fetch_add(1, Ordering::Relaxed),
        };

        #[cfg(feature = "tracing")]
        trace!("{:?} is created", tag);

        tag
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
