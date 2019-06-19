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
#![forbid(unsafe_code)]
#![feature(generators, generator_trait)]

extern crate sandstorm;

use std::rc::Rc;
use std::ops::Generator;
use std::pin::Pin;

use sandstorm::db::DB;

/// This function implements an uncooperative extension using the sandstorm interface.
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
#[allow(unreachable_code)]
#[allow(unused_assignments)]
pub fn init(_db: Rc<DB>) -> Pin<Box<Generator<Yield=u64, Return=u64>>> {
    Box::pin(move || {
        loop {}
        // let array: [i64; 5000000000000] = [0;5000000000000];
        // println!("{}", array[50000000]);

        // These two statements are needed to make the compiler happy.
        yield 0;
        return 0;
    })
}
