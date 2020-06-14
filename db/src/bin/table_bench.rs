/* Copyright (c) 2018 University of Utah
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

extern crate db;
extern crate rand;
extern crate time;

use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use db::bytes::{BufMut, BytesMut};
use db::table::Table;
use rand::Rng;

// The number of iterations to run per thread.
const N_ITERS: u32 = 1u32 << 24;

// The maximum number of threads to run the benchmarks on.
const N_THREADS: usize = 8;

// If true, the database benchmark generates only get requests. If
// false, it alternates between get and put requests.
const READ_ONLY: bool = true;

// Set to true to enable more verbose output like the average time
// per iteration.
const DEBUG_PRINT: bool = false;

// The 128 Byte value on every object in the database.
const VALUE: &str = concat!(
    "1000000000000000000000000000000000000000000000000000000000000000",
    "0000000000000000000000000000000000000000000000000000000000000000"
);

// Converts a Duration type to floating point in seconds.
//
// # Arguments
//
// * `d`: The duration to be converted.
//
// # Return
//
// The duration in seconds, as an f64 type.
fn to_seconds(d: &Duration) -> f64 {
    d.as_secs() as f64 + (d.subsec_nanos() as f64 / 1e9)
}

// This function runs a benchmark on multiple threads.
//
// # Arguments
//
// * `n_threads`: The number of threads to run the benchmark on.
// * `setup`:     The function to setup the database before running the
//                benchmark.
// * `run`:       The benchmark function to be run on each thread.
//
// # Return
//
// A tupule of the form (Duration, u32). The first member represents the
// amount of time it took to complete the benchmark. The second member
// represents the total number of iterations that were run.
fn parallel_bench(
    n_threads: usize,
    setup: fn(&mut Table) -> (),
    run: fn(Arc<Barrier>, Arc<Table>) -> (Duration, u32),
) -> (Duration, u32) {
    // Create and setup the table/database;
    let mut db = Table::default();
    setup(&mut db);
    let db = Arc::new(db);

    // Spawn multiple threads, and run the benchmark function on each of them.
    let mut threads = Vec::with_capacity(n_threads);
    let barrier = Arc::new(Barrier::new(n_threads));

    for _ in 0..n_threads {
        let b = barrier.clone();
        let db = db.clone();
        threads.push(thread::spawn(move || run(b, db)));
    }

    // Iterate across all threads. Return a tupule whose first member consists
    // of the highest execution time across all threads, and whose second member
    // is the sum of the number of iterations run on each benchmark thread.
    // Dividing the second member by the first, will yeild the throughput.
    threads
        .into_iter()
        .map(|t| t.join().expect("ERROR: Thread join failed."))
        .fold((Duration::new(0, 0), 0), |(ll, lr), (rl, rr)| {
            (std::cmp::max(ll, rl), lr + rr)
        })
}

// This function benchmarks the performance of the psuedo random number
// generator.
//
// # Arguments
//
// * `n_threads`: The number of threads to run the benchmark on.
//
// # Return
//
// A tupule of the form (Duration, u32). The first member represents the
// amount of time it took to run the benchmark, and the second represents
// the total number of iterations that were run during the benchmark.
fn parallel_bench_prng(n_threads: usize) -> (Duration, u32) {
    parallel_bench(
        n_threads,
        // No setup required for this benchmark. This is because this
        // benchmark measures the performance of the rand crate, and
        // not that of the database.
        |_db| {},
        // The main benchmark function to be run on each thread.
        |barrier, _db| {
            barrier.wait();

            // Measure the total time to generate a bunch of random numbers.
            let start = Instant::now();
            for _ in 0..N_ITERS {
                let _v = rand::thread_rng().gen::<u32>() & (N_ITERS - 1);
            }
            let gen_time = start.elapsed();

            if DEBUG_PRINT {
                println!(
                    "Average time per random number generated: {:?}",
                    gen_time / N_ITERS
                );
            }

            return (gen_time, N_ITERS);
        },
    )
}

// This function sets up a database and issues gets and puts against it.
//
// # Arguments
//
// * `n_threads`: The number of threads to run the benchmark on.
//
// # Return
//
// A tupule of the form (Duration, u32). The first member represents the
// amount of time it took to run the benchmark, and the second represents
// the total number of iterations that were run during the benchmark.
fn parallel_bench_db(n_threads: usize) -> (Duration, u32) {
    parallel_bench(
        n_threads,
        // The setup function populates the database with a bunch of objects.
        |db| {
            if DEBUG_PRINT {
                println!("Setting up the database.");
            }

            // All objects will have the same value, but a different key.
            let value = VALUE.as_bytes();

            // Populate the table with N_ITERS number of objects.
            let start = Instant::now();
            for i in 0..N_ITERS {
                let key: &[u8] = unsafe {
                    std::slice::from_raw_parts(
                        &i as *const u32 as *const _,
                        std::mem::size_of::<u32>(),
                    )
                };

                // Both, the key and the value are written to a contiguous
                // piece of memory.
                let mut object = BytesMut::with_capacity(key.len() + value.len());
                object.put_slice(key);
                object.put_slice(value);
                let mut object = object.freeze();

                let key = object.split_to(key.len());
                db.put(key, object);
            }
            let put_time = start.elapsed();

            if DEBUG_PRINT {
                println!("Took {:?} to populate database.", put_time);
                println!("Time per Put: {:?}", put_time / N_ITERS);
            }
        },
        // The benchmark function. This function issues back to back gets/puts
        // to the database/table.
        |barrier, db| {
            // Required for put operations.
            let value = VALUE.as_bytes();

            barrier.wait();

            // Issue back-to-back operations to the database.
            let start = Instant::now();
            for i in 0..N_ITERS {
                let v = rand::thread_rng().gen::<u32>() & (N_ITERS - 1);
                let key: &[u8] = unsafe {
                    std::slice::from_raw_parts(
                        &v as *const u32 as *const _,
                        std::mem::size_of::<u32>(),
                    )
                };

                if READ_ONLY || (i & 1 == 0) {
                    let _s = db.get(key).unwrap().value[0] as u64;
                } else {
                    let mut object = BytesMut::with_capacity(key.len() + value.len());
                    object.put_slice(key);
                    object.put_slice(value);
                    let mut object = object.freeze();

                    let key = object.split_to(key.len());
                    db.put(key, object);
                }
            }
            let get_time = start.elapsed();

            if DEBUG_PRINT {
                println!("Average time per operation: {:?}", get_time / N_ITERS);
            }

            (get_time, N_ITERS)
        },
    )
}

// Baseline to gauge cost of thread-local PRNG. Gets about 100 millions u32s per
// second per core. Royal can do about 100 million u32's per core per second.
fn bench_prng_scale() {
    // Make sure that the number of iterations is a power of two.
    assert_eq!(N_ITERS.checked_next_power_of_two(), Some(N_ITERS));

    // Run the benchmark on an increasing number of threads.
    println!("Benchmarking Random number generation.");
    for (n, (duration, n_ops)) in (1..N_THREADS + 1).map(|n| (n, parallel_bench_prng(n))) {
        let tput = n_ops as f64 / to_seconds(&duration);
        println!("{} threads: {:.0} gens/s", n, tput);
    }
    println!("");
}

// This function benchmarks the performance of a database table.
fn bench_db_scale() {
    // Make sure that the number of iterations is a power of two.
    assert_eq!(N_ITERS.checked_next_power_of_two(), Some(N_ITERS));

    // Run the benchmark on an increasing number of threads.
    println!("Benchmarking Database table.");
    for (n, (duration, n_ops)) in (1..N_THREADS + 1).map(|n| (n, parallel_bench_db(n))) {
        let tput = n_ops as f64 / to_seconds(&duration);
        println!("{} threads: {:.0} gets/s", n, tput);
    }
    println!("");
}

fn main() {
    // Set to true to enable random number generation benchmark.
    let bench_prng: bool = true;
    // Set to true to enable database benchmark.
    let bench_table: bool = true;

    // Benchmark random number generation if enabled.
    if bench_prng {
        bench_prng_scale();
    }

    // Benchmark the database if enabled.
    if bench_table {
        bench_db_scale();
    }
}
