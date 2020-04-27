use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

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
              let _: () = { $($body)* };
          }))
      };
  };
}

struct Yields(usize);

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

pub async fn yield_now() {
    Yields(1).await;
}
