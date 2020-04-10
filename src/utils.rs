use std::time::Instant;

pub fn abort_on_panic(f: impl FnOnce()) {
  struct Bomb;

  impl Drop for Bomb {
    fn drop(&mut self) {
      std::process::abort();
    }
  }

  let bomb = Bomb;
  f();
  std::mem::forget(bomb);
}

macro_rules! defer {
  ($($body:tt)*) => {
      let _guard = {
          pub struct Guard<F: FnOnce()>(Option<F>);

          impl<F: FnOnce()> Drop for Guard<F> {
              fn drop(&mut self) {
                  (self.0).take().map(|f| f());
              }
          }

          Guard(Some(|| {
              let _ = { $($body)* };
          }))
      };
  };
}

pub fn monotonic_ms() -> u64 {
  lazy_static! {
    static ref START: Instant = Instant::now();
  }
  let start = *START;
  Instant::now().duration_since(start).as_millis() as u64
}
