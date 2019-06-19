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

#![feature(generator_trait)]

extern crate db;
extern crate time;
extern crate sandstorm;

use std::rc::Rc;
use std::ops::GeneratorState;

use db::cycles::*;

use time::{Duration, PreciseTime};

use sandstorm::db::DB;
use sandstorm::ext::ExtensionManager;
use sandstorm::null::NullDB;

fn main() {
    // Create an extension manager and null db interface.
    let db = Rc::new(NullDB::new());
    let ext_manager = ExtensionManager::new();

    // Number of tiny TAO extensions that will be loaded and called into.
    let n = 100;

    // Benchmark the amount of time taken to load multiple extensions.
    let start = PreciseTime::now();
    for i in 0..n {
        let ret = ext_manager.load(
                            &format!("../ext/test/target/release/libtest{}.so", i),
                            0, &format!("test{}", i),
                            );
        if ret == false {
            panic!("Failed to load test extension!");
        }
    }
    let end: Duration = start.to(PreciseTime::now());
    println!("Time taken to load {} tiny extensions: {} nano seconds",
             n, end.num_nanoseconds().expect("ERROR: Duration overflow!"));

    // Next, call each extension once, and assert that it prints out something.
    let expected : Vec<String> = (0..n)
                                    .map(| _ | format!("TAO Initialized! 0"))
                                    .collect();
    let proc_names : Vec<String> = (0..n)
                                        .map(| i | format!("test{}", i))
                                        .collect();
    for p in proc_names.iter() {
        let mut ext = ext_manager.get(0, p.to_string())
                                    .unwrap()
                                    .get(Rc::clone(&db) as Rc<DB>);
        ext.as_mut().resume();
    }

    db.assert_messages(expected.as_slice());
    db.clear_messages();

    // Then, benchmark the amount of time it takes to call into
    // these extensions.
    let expected : Vec<String> = (0..n)
                                    .map(| _ | format!("TAO Initialized! 1"))
                                    .collect();

    let mut load = Vec::with_capacity(10000000);
    let mut enter = Vec::with_capacity(10000000);

    for _ in 0..1000000 {
        for p in proc_names.iter() {
            let l = rdtsc();
            let mut ext = ext_manager.get(0, p.to_string())
                                        .unwrap()
                                        .get(Rc::clone(&db) as Rc<DB>);
            let r = rdtsc();
            load.push(r - l);

            let l = rdtsc();
            ext.as_mut().resume();
            let r = rdtsc();
            enter.push(r - l);
        }
    }

    let ret = ext_manager.load(
                            "../ext/test/target/release/libtest.so",
                            0, "test",
                            );
    if ret == false {
        panic!("Failed to load test extension!");
    }

    let mut ext = ext_manager.get(0, String::from("test")).unwrap().get(Rc::clone(&db) as Rc<DB>);

    while ext.as_mut().resume() != GeneratorState::Complete(0) {};

    load.sort();
    enter.sort();

    let lm = load[load.len() / 2];
    let em = enter[enter.len() / 2];

    println!("Load: {} cycles {} ns, Enter: {} cycles {} ns",
             lm,
             to_seconds(lm) * 1e9,
             em,
             to_seconds(em) * 1e9,
             );

    db.assert_messages(expected.as_slice());
}
