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
#![feature(generators)]
#![feature(generator_trait)]
#![no_std]

extern crate sandstorm;

use sandstorm::rc::Rc;
use sandstorm::db::DB;
use sandstorm::Generator;
use sandstorm::boxed::Box;
use sandstorm::pack::{consume, pack};

/// Status codes for the response to the tenant.
const SUCCESSFUL: u8 = 0x01;
const INVALIDARG: u8 = 0x02;
const INVALIDKEY: u8 = 0x03;

/// This function serves as the entry to the aggregate extension.
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
pub fn init(db: Rc<DB>) -> Box<Generator<Yield = u64, Return = u64>> {
    Box::new(move || {
        dispatch(db);
        return 0;

        // XXX: Unreachable, but required for compilation.
        yield 0;
    })
}

/// Unwraps arguments, performs the aggregation, and writes a response.
///
/// # Arguments
///
/// * `db`: An argument whose type implements the `DB` trait which can be used
///         to interact with the database.
#[inline(always)]
fn dispatch(db: Rc<DB>) {
    let mut err = INVALIDARG;

    // Unwrap args, lookup the db, perform the aggregate.
    let arg: &[u8] = db.args();
    let res = consume::<u64>(arg)
        .and_then(|(t, k)| {
            err = INVALIDKEY;
            db.get(*t, k)
        })
        .and_then(|val| {
            err = SUCCESSFUL;
            Some(aggregate(0, val.read()))
        });

    // Write out a response.
    db.resp(pack(&err));

    if let Some(sum) = res {
        db.resp(pack(&sum));
    }
}

/// Aggregates a sequence of bytes into an unsigned 64 bit integer.
///
/// # Arguments
///
/// * `init`: The initial value to be used in the aggregation.
/// * `vec`:  Slice with the sequence of bytes to aggregate. Read from the database.
#[inline(always)]
fn aggregate(mut init: u64, vec: &[u8]) -> u64 {
    for e in vec {
        init += (*e) as u64;
    }

    init
}
