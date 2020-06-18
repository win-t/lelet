<h1 align="center">lelet</h1>
<div align="center">
  <strong>
    A golang like task executor
  </strong>
</div>

<br />

<div align="center">
  <!-- Crates version -->
  <a href="https://crates.io/crates/lelet">
    <img src="https://img.shields.io/crates/v/lelet.svg?style=flat-square"
    alt="Crates.io version" />
  </a>
  <!-- Downloads -->
  <a href="https://crates.io/crates/lelet">
    <img src="https://img.shields.io/crates/d/lelet.svg?style=flat-square"
      alt="Download" />
  </a>
  <!-- docs.rs docs -->
  <a href="https://docs.rs/lelet">
    <img src="https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square"
      alt="docs.rs docs" />
  </a>
</div>

## Lelet executor

Task executor that inspired by golang runtime.

The executor is running in thread pool, and when it detect blocking call inside
a task, it will automatically scale the thread pool.

Because of this feature, it is always safe for you to do blocking operation in a task,
you don't need to worry about blocking the entire executor thread.

## Installation

With [cargo add][cargo-add] installed run:

```sh
$ cargo add lelet
```

[cargo-add]: https://github.com/killercup/cargo-edit

## Example

```rust
use std::thread;
use std::time::Duration;

use futures_timer::Delay;

fn main() {
    lelet::spawn(async {
        for _ in 0..10 {
            Delay::new(Duration::from_secs(1)).await;
            println!("Non-blocking Hello World");
        }
    });

    lelet::spawn(async {
        for _ in 0..10 {
            thread::sleep(Duration::from_secs(1));
            println!("Blocking Hello World");
        }
    });

    thread::sleep(Duration::from_secs(11));
}
```

## How about IO

`lelet-io` is still on progress, in the meantime you can use async IO library from tokio

for example HTTP server using hyper

```rust
use std::convert::Infallible;
use std::thread;
use std::time::Duration;

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server, StatusCode};

async fn handler(request: Request<Body>) -> Result<Response<Body>, Infallible> {
    match request.uri().path() {
        "/" => Ok(Response::new(Body::from("Hello World!"))),
        "/blocking" => {
            thread::sleep(Duration::from_secs(5));
            Ok(Response::new(Body::from("Blocking Hello World!")))
        }
        _ => {
            let mut resp = Response::new(Body::from("404 Not Found"));
            *resp.status_mut() = StatusCode::NOT_FOUND;
            Ok(resp)
        }
    }
}

fn main() {
    tokio::runtime::Builder::new()
        .enable_io()
        .build()
        .unwrap()
        .block_on(async {
            let addr = "127.0.0.1:3000";
            let listener = tokio::net::TcpListener::bind(addr).await.unwrap();

            let make_svc =
                make_service_fn(|_conn| async { Ok::<_, Infallible>(service_fn(handler)) });

            let server = Server::builder(compat::TcpListener(listener))
                .executor(compat::Executor)
                .serve(make_svc);

            println!("Listening on http://{}", addr);
            server.await.unwrap();
        });
}

pub mod compat {
    use std::future::Future;
    use std::io;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use hyper::server::accept::Accept;
    use tokio::io::{AsyncRead, AsyncWrite};

    #[derive(Clone)]
    pub struct Executor;

    impl<F> hyper::rt::Executor<F> for Executor
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        fn execute(&self, fut: F) {
            lelet::spawn(fut);
        }
    }

    pub struct TcpListener(pub tokio::net::TcpListener);

    impl Accept for TcpListener {
        type Conn = TcpStream;
        type Error = io::Error;

        fn poll_accept(
            mut self: Pin<&mut Self>,
            cx: &mut Context,
        ) -> Poll<Option<Result<Self::Conn, Self::Error>>> {
            Pin::new(&mut self.0.incoming())
                .poll_accept(cx)
                .map(|result| Some(result.map(TcpStream)))
        }
    }

    pub struct TcpStream(pub tokio::net::TcpStream);

    impl AsyncRead for TcpStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context,
            buf: &mut [u8],
        ) -> Poll<io::Result<usize>> {
            Pin::new(&mut self.0).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for TcpStream {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Pin::new(&mut self.0).poll_write(cx, buf)
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
            Pin::new(&mut self.0).poll_flush(cx)
        }

        fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
            Pin::new(&mut self.0).poll_shutdown(cx)
        }
    }
}
```

then test multiple request to `/blocking`, all of them will complete within 5 second

```bash
bash -c '
  date
  for i in {0..20}; do
    curl localhost:3000/blocking -s -o /dev/null &
  done
  wait
  date
'
```
