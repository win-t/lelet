use crossbeam_channel::{bounded, Receiver, Sender};

pub struct Sleeper(Sender<()>, Receiver<()>);

impl Sleeper {
    pub fn new() -> Sleeper {
        // channel with buffer size 1 to not miss a notification
        let (s, r) = bounded(1);
        Sleeper(s, r)
    }

    pub fn sleep(&self) {
        self.1.recv().unwrap();
    }

    pub fn wake_up(&self) -> bool {
        self.0.try_send(()).is_ok()
    }
}
