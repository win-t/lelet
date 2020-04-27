use std::thread;
use std::time::Duration;

use futures_timer::Delay;

fn main() {
    #[cfg(feature = "tracing")]
    simple_logger::init().unwrap();

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
