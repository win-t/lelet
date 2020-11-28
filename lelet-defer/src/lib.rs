#![no_std]
#![forbid(unsafe_code)]

/// Defer the execution until it get dropped
#[macro_export]
macro_rules! defer {
    ($($body:tt)*) => {
        let _guard = {
            struct Guard<F: Fn()>(F);

            impl<F: Fn()> Drop for Guard<F> {
                fn drop(&mut self) {
                    (self.0)();
                }
            }

            Guard(|| { $($body)* })
        };
    };
}
