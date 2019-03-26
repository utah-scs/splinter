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
#![forbid(unsafe_code)]
#![feature(generators)]
#![feature(generator_trait)]
#![no_std]

extern crate sandstorm;

use sandstorm::boxed::Box;
use sandstorm::db::DB;
use sandstorm::pack::pack;
use sandstorm::rc::Rc;
use sandstorm::size_of;
use sandstorm::vec::*;
use sandstorm::Generator;

/// Status codes for the response to the tenant.
const SUCCESSFUL: u8 = 0x01;
const INVALIDARG: u8 = 0x02;
const INVALIDKEY: u8 = 0x03;

const KEYLENGTH: u16 = 30;

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
    // Unwrap args, lookup the db, perform the aggregate.
    let arg: &[u8] = db.args();
    let (t, val) = arg.split_at(size_of::<u64>());
    let (n, val) = val.split_at(size_of::<u32>());
    let (o, key) = val.split_at(size_of::<u32>());

    // Get the table id from the unwrapped arguments.
    let mut table: u64 = 0;
    for (idx, e) in t.iter().enumerate() {
        table |= (*e as u64) << (idx << 3);
    }

    // Get the number of keys to aggregate across.
    let mut num_k: u32 = 0;
    for (idx, e) in n.iter().enumerate() {
        num_k |= (*e as u32) << (idx << 3);
    }

    // Get the order.
    let mut order: u32 = 0;
    for (idx, e) in o.iter().enumerate() {
        order |= (*e as u32) << (idx << 3);
    }

    // Retrieve the list of keys to aggregate across.
    let obj = db.get(table, key);

    // Error code and response defined upfront so that results are written only
    // at the end of this function.
    let mut err = INVALIDARG;
    let mut res = 0;

    // Try performing the aggregate if the key list was successfully retrieved.
    if let Some(val) = obj {
        let r = aggregate(Rc::clone(&db), table, val.read(), num_k, order);
        err = r.0;
        res = r.1;
    }

    // First write in the response code.
    db.resp(pack(&err));

    // If the invocation was successful, then write in the result of the aggregation too.
    if err == SUCCESSFUL {
        db.resp(pack(&res));
    }
}

/// Aggregates a column across a list of records.
///
/// # Arguments
///
/// * `db`:    An argument whose type implements the `DB` trait which can be used
///            to interact with the database.
/// * `table`: Table the records belong to. Required to look them up from the db.
/// * `key`:   List of keys to lookup and aggregate across.
/// * `num`:   Number of objects to aggregate across.
/// * `order`: The order of the final polynomial.
///
/// # Return
/// A tupule consisting of an error code and the result of the aggregation. This
/// result is valid only if the error code is `SUCCESSFUL`.
#[inline(always)]
fn aggregate(db: Rc<DB>, table: u64, keys: &[u8], num: u32, order: u32) -> (u8, u64) {
    let mut col = Vec::new();

    let buf = db.multiget(
        table,
        KEYLENGTH,
        keys.split_at((KEYLENGTH as usize) * (num as usize)).0,
    );

    match buf {
        Some(vals) => {
            if vals.num() > 0 {
                col.push(vals.read()[0]);
            }

            while vals.next() {
                col.push(vals.read()[0]);
            }
        }

        None => return (INVALIDKEY, 0),
    }

    // Aggregate the saved column.
    let mut aggr = col.iter().fold(0, |a, e| a + (*e as u64));

    // Compute pow(aggr, order).
    for _mul in 1..order {
        aggr *= aggr;
    }

    (SUCCESSFUL, aggr)
}
