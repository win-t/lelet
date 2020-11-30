use crossbeam_deque::Stealer as DequeStealer;
use crossbeam_deque::Worker as DequeWorker;

pub use crossbeam_deque::Injector;
pub use crossbeam_deque::Steal;

pub struct Worker<T> {
    pub priority: DequeWorker<T>,
    pub normal: DequeWorker<T>,
}

impl<T> Worker<T> {
    pub fn new() -> Worker<T> {
        Worker {
            priority: DequeWorker::new_fifo(),
            normal: DequeWorker::new_fifo(),
        }
    }

    pub fn pop(&self) -> Option<T> {
        self.priority.pop().or_else(|| self.normal.pop())
    }

    pub fn push(&self, t: T) {
        self.flush();
        self.priority.push(t);
    }

    pub fn flush(&self) {
        while let Some(t) = self.priority.pop() {
            self.normal.push(t);
        }
    }

    pub fn stealer(&self) -> Stealer<T> {
        Stealer {
            priority: self.priority.stealer(),
            normal: self.normal.stealer(),
        }
    }
}

pub struct Stealer<T> {
    pub priority: DequeStealer<T>,
    pub normal: DequeStealer<T>,
}

impl<T> Stealer<T> {
    pub fn is_empty(&self) -> bool {
        self.normal.is_empty() && self.priority.is_empty()
    }

    pub fn steal_batch_and_pop(&self, dest: &Worker<T>) -> Steal<T> {
        match self.normal.steal_batch_and_pop(&dest.normal) {
            Steal::Empty => self.priority.steal_batch_and_pop(&dest.normal),
            Steal::Success(t) => Steal::Success(t),
            Steal::Retry => self.priority.steal_batch_and_pop(&dest.normal),
        }
    }
}
