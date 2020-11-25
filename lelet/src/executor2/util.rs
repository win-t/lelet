use std::env;

pub fn coprime(a: usize, b: usize) -> bool {
    gcd(a, b) == 1
}

pub fn gcd(a: usize, b: usize) -> usize {
    let mut p = (a, b);
    while p.1 != 0 {
        p = (p.1, p.0 % p.1);
    }
    p.0
}

pub fn max_procs() -> usize {
    let num = env::var("LELET_MAX_PROCS")
        .map_err(|_| ())
        .and_then(|v| v.parse().map_err(|_| ()))
        .unwrap_or(0);
    if num == 0 {
        num_cpus::get()
    } else {
        num
    }
}
