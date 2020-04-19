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
