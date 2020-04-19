use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use once_cell::sync::Lazy;

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

#[inline]
pub fn monotonic_ms() -> u64 {
  static START: Lazy<Instant> = Lazy::new(|| Instant::now());
  let start = *START;
  Instant::now().duration_since(start).as_millis() as u64
}

pub struct Yields(usize);

impl Future for Yields {
  type Output = ();

  fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
    if self.0 == 0 {
      Poll::Ready(())
    } else {
      self.0 -= 1;
      cx.waker().wake_by_ref();
      Poll::Pending
    }
  }
}

#[inline]
pub async fn yield_now() {
  Yields(1).await;
}
