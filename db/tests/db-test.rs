extern crate db;
extern crate rand;
extern crate spin;

// Things to improve:
//  - Run for a duration instead of iters.

// Some quick stats with royal.
// With spin::Mutex
// 1 threads: 5763507 gets/s
// 2 threads: 3731027 gets/s
// 3 threads: 2605923 gets/s
// 4 threads: 2155379 gets/s
//
// With std::sync:Mutex
// 1 threads: 4879816 gets/s
// 2 threads: 1543036 gets/s
// 3 threads: 1563230 gets/s
// 4 threads: 995501 gets/s

use std::thread;
// Note: need to add lock unwrap below since ifaces differ between
// spinlock and normal Mutex.
//
//use std::sync::{Arc, Barrier, Mutex}; // Use regular Mutex.
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};
use rand::{Rng};

use db::Table;

const DEBUG_PRINT : bool = false;

const N_ITERS : u32 = (1u32 << 24);
const N_THREADS : usize = 8;

// false: all gets in measurement phase; true alternate gets/puts
const READ_ONLY : bool = false;

// 128 B
const VALUE : &str =
    concat!("1000000000000000000000000000000000000000000000000000000000000000",
            "0000000000000000000000000000000000000000000000000000000000000000");

/// Converts `d` to floating point duration in seconds.
fn to_seconds(d: &Duration) -> f64 {
    d.as_secs() as f64 + (d.subsec_nanos() as f64 / 1e9)
}

/// Creates a hash_map of `N_ITERS` entries with 128 byte values, then it perform runs with
/// `1..N_THREADS+1` all performing gets at a time. The hash_map is protected by a Mutex, which can
/// be swapped above for a standard or spinlock implemenation.
#[test]
fn bench_db_scale() {
    assert_eq!(N_ITERS.checked_next_power_of_two(), Some(N_ITERS));

    for (n, (duration, n_ops)) in (1..N_THREADS+1)
            .map(|n| (n, parallel_bench_db(n)))
    {
        let tput = n_ops as f64 / to_seconds(&duration);
        println!("{} threads: {:.0} gets/s", n, tput);
    }
}

/// Baseline to gauge cost of thread-local PRNG. Gets about 100 millions u32s per second per core
/// on royal.
#[test]
fn bench_prng_scale() {
    assert_eq!(N_ITERS.checked_next_power_of_two(), Some(N_ITERS));

    for (n, (duration, n_ops)) in (1..N_THREADS+1)
            .map(|n| (n, parallel_bench_prng(n)))
    {
        let tput = n_ops as f64 / to_seconds(&duration);
        println!("{} threads: {:.0} gens/s", n, tput);
    }
}

/// Set up a database with a million entries on a single thread, then run
/// a million gets against it in parallel using `n_threads`.
fn parallel_bench_db(n_threads: usize) -> (Duration, u32) {
    parallel_bench(n_threads,
        |db| {
            if DEBUG_PRINT {
                println!("Setting up db...");
            }

            let value = VALUE.as_bytes();

            let start = Instant::now();
            for i in 0..N_ITERS {
                let key : &[u8] = unsafe {
                    std::slice::from_raw_parts(&i as *const u32 as *const _,
                                               std::mem::size_of::<u32>())
                };
                db.put(key, value);
            }
            let put_time = start.elapsed();

            if DEBUG_PRINT {
                println!("Took: {:?}", put_time);
                println!("Per Put: {:?}", put_time/N_ITERS);
            }
        },
        |barrier, db| {
            let value = VALUE.as_bytes();

            barrier.wait();

            let start = Instant::now();
            let mut s = 0u64;
            for i in 0..N_ITERS {
                let v = rand::thread_rng().gen::<u32>() & (N_ITERS - 1);
                let key : &[u8] = unsafe {
                    std::slice::from_raw_parts(&v as *const u32 as *const _,
                                               std::mem::size_of::<u32>())
                };
                if READ_ONLY || (i & 1 == 0) {
                    s += db.get(key).unwrap()[0] as u64;
                } else {
                    db.put(key, value);
                }
            }
            let get_time = start.elapsed();

            if DEBUG_PRINT {
                println!("Per Get: {:?}", get_time/N_ITERS);
            }
            println!("{0:}", s/s);

            (get_time, N_ITERS)
        })
}

fn parallel_bench_prng(n_threads: usize) -> (Duration, u32) {
    parallel_bench(n_threads,
        |_db| {},
        |barrier, _db| {
            barrier.wait();

            let start = Instant::now();
            let mut s = 0u64;
            for _ in 0..N_ITERS {
                let v = rand::thread_rng().gen::<u32>() & (N_ITERS - 1);
                s += v as u64;
            }
            let get_time = start.elapsed();

            if DEBUG_PRINT {
                println!("Per Get: {:?}", get_time/N_ITERS);
            }
            println!("{0:}", s/s);

            (get_time, N_ITERS)
        })
}

fn parallel_bench(n_threads: usize,
                  setup: fn (&mut Table) -> (),
                  run: fn (Arc<Barrier>, Arc<Table>) -> (Duration, u32))
    -> (Duration, u32)
{
    let mut db = Table::default();
    setup(&mut db);

    let db = Arc::new(db);

    let mut threads = Vec::with_capacity(n_threads);
    let barrier = Arc::new(Barrier::new(n_threads));


    for _ in 0..n_threads {
        let b = barrier.clone();
        let db = db.clone();
        threads.push(thread::spawn(move || { run(b, db) }));
    }

    threads.into_iter().map(|t| t.join().expect("Thread join failed."))
        .fold((Duration::new(0, 0), 0),
              |(ll, lr), (rl, rr)| (std::cmp::max(ll, rl), lr + rr))
}

