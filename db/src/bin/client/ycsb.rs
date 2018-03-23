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

extern crate rand;
extern crate zipf;

use std::{mem, slice};
use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicBool, Ordering};

use self::rand::{Rng, XorShiftRng};
use self::rand::distributions::Sample;
use self::zipf::ZipfDistribution;

type GetFn = fn(&[u8]);
type PutFn = fn(&[u8], &[u8]);

// YCSB A, B, and C benchmark.
// The benchmark is created and parameterized with `new()`. Many threads
// share the same benchmark instance. Each thread can call `abc()` which
// runs the benchmark until another thread calls `stop()`. Each thread
// then returns their runtime and the number of gets and puts they have done.
// This benchmark doesn't care about how get/put are implemented; it takes
// function pointers to get/put on `new()` and just calls those as it runs.
//
// The tests below give an example of how to use it and how to aggregate the results.
struct Ycsb {
    key_len: usize,
    value_len: usize,
    n_keys: usize,
    put_pct: usize,
    skew: f64,
    get: GetFn,
    put: PutFn,
    done: AtomicBool,
}

impl Ycsb {
    // Create a new benchmark instance.
    //
    // # Arguments
    //  - key_len: Length of the keys to generate per get/put. Most bytes will be zero, since
    //             the benchmark poplates them from a random 32-bit value.
    //  - value_len: Length of the values to store per put. Always all zero bytes.
    //  - n_keys: Number of keys from which random keys are drawn.
    //  - put_pct: Number between 0 and 100 indicating percent of ops that are sets.
    //  - skew: Zipfian skew parameter. 0.99 is YCSB default.
    //  - get: A function that fetches the data stored under a bytestring key of `key_len` bytes.
    //  - set: A function that stores the data stored under a bytestring key of `key_len` bytes
    //         with a bytestring value of `value_len` bytes.
    // # Return
    //  A new instance of YCSB that threads can call `abc()` on to run.
    fn new(
        key_len: usize,
        value_len: usize,
        n_keys: usize,
        put_pct: usize,
        skew: f64,
        get: GetFn,
        put: PutFn,
    ) -> Ycsb {
        Ycsb {
            key_len: key_len,
            value_len: value_len,
            n_keys: n_keys,
            put_pct: put_pct,
            skew: skew,
            get: get,
            put: put,
            done: AtomicBool::new(false),
        }
    }

    // Run YCSB A, B, or C (depending on `new()` parameters).
    // The calling thread will not return until `done()` is called on this `Ycsb` instance.
    // # Return
    //  A three tuple consisting of the duration that this thread ran the benchmark, the
    //  number of gets it performed, and the number of puts it performed.
    fn abc(&self) -> (Duration, u32, u32) {
        // TODO(stutsman) It's unclear why this won't work.
        // let rng = XorShiftRng::from_rng(rand::thread_rng()).expect("Couldn't create PRNG.");
        let mut rng = XorShiftRng::new_unseeded();

        let mut key_buf: Vec<u8> = Vec::with_capacity(self.key_len);
        key_buf.resize(self.key_len, 0);
        let mut value_buf: Vec<u8> = Vec::with_capacity(self.value_len);
        value_buf.resize(self.value_len, 0);

        let mut zipf =
            ZipfDistribution::new(self.n_keys, self.skew).expect("Couldn't create zipf PRNG.");

        let mut n_gets = 0u32;
        let mut n_puts = 0u32;

        let start = Instant::now();
        while !self.done.load(Ordering::Relaxed) {
            let is_get = (rng.gen::<u32>() % 100) >= self.put_pct as u32;

            let k = zipf.sample(&mut rng) as u32;
            let kp = &k as *const u32 as *const u8;
            let kslice = unsafe { slice::from_raw_parts(kp, mem::size_of::<u32>()) };

            key_buf[..mem::size_of::<u32>()].copy_from_slice(kslice);

            if is_get {
                (self.get)(key_buf.as_slice());
                n_gets += 1;
            } else {
                (self.put)(key_buf.as_slice(), value_buf.as_slice());
                n_puts += 1;
            }
        }

        (start.elapsed(), n_gets, n_puts)
    }

    // Break all threads running `abc()`.
    fn stop(&self) {
        self.done.store(true, Ordering::Relaxed);
    }
}

mod test {
    use std;
    use std::thread;
    use std::time::Duration;
    use std::sync::Arc;

    fn get(_key: &[u8]) {}
    fn put(_key: &[u8], _value: &[u8]) {}

    #[test]
    fn ycsb_abc_basic() {
        let b = Arc::new(super::Ycsb::new(10, 100, 1000000, 5, 0.99, get, put));

        let n_threads = 32;
        let mut threads = Vec::with_capacity(n_threads);

        for _ in 0..n_threads { let b = b.clone();
            threads.push(thread::spawn(move || b.abc()));
        }

        thread::sleep(Duration::from_secs(2));

        b.stop();

        // Iterate across all threads. Return a tupule whose first member consists
        // of the highest execution time across all threads, and whose second member
        // is the sum of the number of iterations run on each benchmark thread.
        // Dividing the second member by the first, will yeild the throughput.
        let (duration, n_gets, n_puts) = threads
            .into_iter()
            .map(|t| t.join().expect("ERROR: Thread join failed."))
            .fold(
                (Duration::new(0, 0), 0, 0),
                |(ldur, lgets, lputs), (rdur, rgets, rputs)| {
                    (std::cmp::max(ldur, rdur), lgets + rgets, lputs + rputs)
                },
            );

        let secs = duration.as_secs() as f64 + (duration.subsec_nanos() as f64 / 1e9);
        println!(
            "{} threads: {:.0} gets/s {:.0} puts/s {:.0} ops/s",
            n_threads,
            n_gets as f64 / secs,
            n_puts as f64 / secs,
            (n_gets + n_puts) as f64 / secs
        );
    }
}
