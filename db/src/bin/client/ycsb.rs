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
use self::rand::{Rng, SeedableRng, XorShiftRng};
use self::zipf::ZipfDistribution;
use ycsb::rand::distributions::Sample;

// YCSB A, B, and C benchmark.
// The benchmark is created and parameterized with `new()`. Many threads
// share the same benchmark instance. Each thread can call `abc()` which
// runs the benchmark until another thread calls `stop()`. Each thread
// then returns their runtime and the number of gets and puts they have done.
// This benchmark doesn't care about how get/put are implemented; it takes
// function pointers to get/put on `new()` and just calls those as it runs.
//
// The tests below give an example of how to use it and how to aggregate the results.
pub struct Ycsb {
    put_pct: usize,
    rng: Box<Rng>,
    key_rng: Box<ZipfDistribution>,
    key_buf: Vec<u8>,
    value_buf: Vec<u8>,
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
    // # Return
    //  A new instance of YCSB that threads can call `abc()` on to run.
    pub fn new(
        key_len: usize,
        value_len: usize,
        n_keys: usize,
        put_pct: usize,
        skew: f64,
    ) -> Ycsb {
        let seed: [u32; 4] = rand::random::<[u32; 4]>();

        let mut key_buf: Vec<u8> = Vec::with_capacity(key_len);
        key_buf.resize(key_len, 0);
        let mut value_buf: Vec<u8> = Vec::with_capacity(value_len);
        value_buf.resize(value_len, 0);

        Ycsb {
            put_pct: put_pct,
            rng: Box::new(XorShiftRng::from_seed(seed)),
            key_rng: Box::new(ZipfDistribution::new(n_keys, skew).expect("Couldn't create key RNG.")),
            key_buf: key_buf,
            value_buf: value_buf,
        }
    }

    // Run YCSB A, B, or C (depending on `new()` parameters).
    // The calling thread will not return until `done()` is called on this `Ycsb` instance.
    //
    // # Arguments
    //  - get: A function that fetches the data stored under a bytestring key of `self.key_len` bytes.
    //  - set: A function that stores the data stored under a bytestring key of `self.key_len` bytes
    //         with a bytestring value of `self.value_len` bytes.
    // # Return
    //  A three tuple consisting of the duration that this thread ran the benchmark, the
    //  number of gets it performed, and the number of puts it performed.
    pub fn abc<G, P, R>(&mut self, mut get: G, mut put: P) -> R
        where G: FnMut(&[u8]) -> R,
              P: FnMut(&[u8], &[u8]) -> R,
    {
        let is_get = (self.rng.gen::<u32>() % 100) >= self.put_pct as u32;

        let k = self.key_rng.sample(&mut self.rng) as u32;
        let kp = &k as *const u32 as *const u8;
        let kslice = unsafe { slice::from_raw_parts(kp, mem::size_of::<u32>()) };

        self.key_buf[..mem::size_of::<u32>()].copy_from_slice(kslice);

        if is_get {
            get(self.key_buf.as_slice())
        } else {
            put(self.key_buf.as_slice(), self.value_buf.as_slice())
        }
    }
}

#[cfg(test)]
mod test {
    use std;
    use std::thread;
    use std::time::{Duration, Instant};
    use std::sync::{Arc, Mutex};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn ycsb_abc_basic() {
        let n_threads = 32;
        let mut threads = Vec::with_capacity(n_threads);
        let done = Arc::new(AtomicBool::new(false));

        for _ in 0..n_threads {
            let done = done.clone();
            threads.push(thread::spawn(move || {
                let mut b = super::Ycsb::new(10, 100, 1000000, 5, 0.99);
                let mut n_gets = 0u64;
                let mut n_puts = 0u64;
                let start = Instant::now();
                while !done.load(Ordering::Relaxed) {
                    b.abc(|_key| {
                            n_gets += 1
                        },
                        |_key, _value| {
                            n_puts += 1
                        });
                }
                (start.elapsed(), n_gets, n_puts)
            }));
        }

        thread::sleep(Duration::from_secs(2));
        done.store(true, Ordering::Relaxed);

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

    fn convert_key(key: &[u8]) -> u32 {
        assert_eq!(4, key.len());
        let mut k: u32 = 0;
        for b in key {
            k *= 256;
            k += *b as u32;
        }
        k
    }

    #[test]
    fn ycsb_abc_histogram() {
        let hist = Arc::new(Mutex::new(HashMap::new()));

        let n_keys = 20;
        let n_threads = 32;

        let mut threads = Vec::with_capacity(n_threads);
        let done = Arc::new(AtomicBool::new(false));
        for _ in 0..n_threads {
            let hist = hist.clone();
            let done = done.clone();
            threads.push(thread::spawn(move || {
                let mut b = super::Ycsb::new(4, 100, n_keys, 5, 0.99);
                let mut n_gets = 0u64;
                let mut n_puts = 0u64;
                let start = Instant::now();
                while !done.load(Ordering::Relaxed) {
                    b.abc(
                    |key| { // get
                        let k = convert_key(key);
                        let mut ht = hist.lock().unwrap();
                        ht.entry(k).or_insert((0, 0)).0 += 1;
                        n_gets += 1
                    },
                    |key, _value| { // put
                        let k = convert_key(key);
                        let mut ht = hist.lock().unwrap();
                        ht.entry(k).or_insert((0, 0)).1 += 1;
                        n_puts += 1
                    });
                }
                (start.elapsed(), n_gets, n_puts)
            }));
        }

        thread::sleep(Duration::from_secs(2));
        done.store(true, Ordering::Relaxed);

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

        let ht = hist.lock().unwrap();
        let mut kvs : Vec<_> = ht.iter().collect();
        kvs.sort();
        let v : Vec<_> = kvs.iter().map(|&(k, v)| println!("Key {:?}: {:?} gets/puts", k, v)).collect();
        println!("Unique key count: {}", v.len());
        assert_eq!(n_keys, v.len());

        let total : i64 = kvs.iter().map(|&(_, &(g, s))| (g + s) as i64).sum();

        let mut sum = 0;
        for &(k, v) in kvs.iter() {
            let &(g, s) = v;
            sum += g + s;
            let percentile = sum as f64 / total as f64;
            println!("Key {:?}: {:?} percentile", k, percentile);
        }
        // For 20 keys median key should be near 4th key, so this checks out.
    }
}
