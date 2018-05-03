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

#![crate_type = "dylib"]
#![feature(no_unsafe)]
#![feature(generators, generator_trait)]

extern crate db;
extern crate sandstorm;

use db::cycles::*;

use std::rc::Rc;
use std::ops::Generator;

use sandstorm::db::DB;

/// This function does a yield followed by a return. It is a simple extension
/// required for testing.
///
/// # Arguments
///
/// * `db`: An argument whose type implements the `DB` trait which can be used
///         to interact with the database.
///
/// # Return
///
/// A coroutine that can be run inside the database.
#[no_mangle]
#[allow(unused_assignments)]
pub fn init(db: Rc<DB>) -> Box<Generator<Yield=u64, Return=u64>> {
    Box::new(move || {
        yield 0;

        let mut y = Vec::with_capacity(1000000);
        let mut c = Vec::with_capacity(1000000);

        for _j in 0u64..1000000u64 {
            let l = rdtsc();
            db.debug_log("");
            let r = rdtsc();

            c.push(r - l);
        }

        for _i in 0u64..1000000u64 {
            let a = rdtsc();
            yield 0;
            let b = rdtsc();

            y.push(b - a);
        }

        y.sort();
        c.sort();

        println!("Yield: {} ns, Call: {} ns", to_seconds(y[y.len() / 2]) * 1e9, to_seconds(c[c.len() / 2]) * 1e9);

        return 0;
    })
}
