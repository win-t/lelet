use std::ptr;
use std::sync::atomic::{AtomicPtr, Ordering};

use super::processor::Processor;

#[cfg(feature = "tracing")]
use std::sync::atomic::AtomicUsize;

#[cfg(feature = "tracing")]
use log::trace;

pub struct TaskTag {
    #[cfg(feature = "tracing")]
    id: usize,

    processor_hint: AtomicPtr<Processor>,
}

impl TaskTag {
    #[inline(always)]
    pub fn new() -> TaskTag {
        #[cfg(feature = "tracing")]
        static TASK_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

        #[allow(clippy::let_and_return)]
        let tag = TaskTag {
            #[cfg(feature = "tracing")]
            id: TASK_ID_COUNTER.fetch_add(1, Ordering::Relaxed),

            processor_hint: AtomicPtr::new(ptr::null_mut()),
        };

        #[cfg(feature = "tracing")]
        trace!("{:?} is created", tag);

        tag
    }

    #[inline(always)]
    pub fn processor_hint(&self) -> *const Processor {
        self.processor_hint.load(Ordering::Relaxed)
    }

    #[inline(always)]
    pub fn set_processor_hint(&self, processor: *const Processor) {
        self.processor_hint
            .store(processor as *mut _, Ordering::Relaxed);
    }
}

#[cfg(feature = "tracing")]
impl std::fmt::Debug for TaskTag {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(&format!("Task({})", self.id))
    }
}

#[cfg(feature = "tracing")]
impl Drop for TaskTag {
    fn drop(&mut self) {
        trace!("{:?} is destroyed", self);
    }
}
