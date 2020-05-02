use std::thread;
use std::time::Duration;

use futures_timer::Delay;

fn main() {
    #[cfg(feature = "tracing")]
    simple_logger::init().unwrap();

    for i in 0..10 {
        lelet::spawn(async move {
            for _ in 0..10 {
                Delay::new(Duration::from_secs(1)).await;
                println!("Non-blocking Hello World {}", i);
            }
        });
        thread::sleep(Duration::from_millis(10));
    }

    thread::sleep(Duration::from_secs(11));
}
