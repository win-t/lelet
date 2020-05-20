use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_channel::{bounded, Receiver, Sender};

pub struct Sleeper(Sender<()>, Receiver<()>);

impl Sleeper {
    pub fn new() -> Sleeper {
        // channel with buffer size 1 to not miss a notification
        let (s, r) = bounded(1);
        Sleeper(s, r)
    }

    #[inline(always)]
    pub fn sleep(&self) {
        self.1.recv().unwrap();
    }

    #[inline(always)]
    pub fn wake_up(&self) {
        let _ = self.0.try_send(());
    }
}

#[inline(always)]
pub fn atomic_usize_add_mod(p: &AtomicUsize, i: usize, m: usize) -> usize {
    let value = p.fetch_add(i, Ordering::Relaxed);
    if value < m {
        value
    } else {
        let new_value = value % m;
        p.compare_and_swap(value + i, new_value + i, Ordering::Relaxed);
        new_value
    }
}

#[inline(always)]
pub fn coprime(a: usize, b: usize) -> bool {
    gcd(a, b) == 1
}

#[inline(always)]
pub fn gcd(a: usize, b: usize) -> usize {
    let mut p = (a, b);
    while p.1 != 0 {
        p = (p.1, p.0 % p.1);
    }
    p.0
}
