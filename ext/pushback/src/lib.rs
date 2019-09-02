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

extern crate db;
#[macro_use]
extern crate sandstorm;

use std::ops::Generator;
use std::rc::Rc;

use db::cycles;

use sandstorm::db::DB;
use sandstorm::pack::pack;

/// This function implements the get() extension using the sandstorm interface.
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
        let mut obj = None;
        let mut t_table: u64 = 0;
        let mut num: u32 = 0;
        let mut ord: u32 = 0;
        let mut keys: Vec<u8> = Vec::with_capacity(30);

        {
            // First off, retrieve the arguments to the extension.
            let args = db.args();

            // Check that the arguments received is long enough to contain an
            // 8 byte table id and a key to be looked up. If not, then write
            // an error message to the response and return to the database.
            if args.len() <= 8 {
                let error = "Invalid args";
                db.resp(error.as_bytes());
                return 1;
            }

            // Next, split the arguments into a view over the table identifier
            // (first eight bytes), and a view over the key to be looked up.
            // De-serialize the table identifier into a u64.
            let (table, value) = args.split_at(8);
            let (number, value) = value.split_at(4);
            let (order, key) = value.split_at(4);
            keys.extend_from_slice(key);

            for (idx, e) in table.iter().enumerate() {
                t_table |= (*e as u64) << (idx << 3);
            }

            for (idx, e) in number.iter().enumerate() {
                num |= (*e as u32) << (idx << 3);
            }

            for (idx, e) in order.iter().enumerate() {
                ord |= (*e as u32) << (idx << 3);
            }
        }

        let mut mul: u64 = 0;

        for i in 0..num {
            GET!(db, t_table, keys, obj);

            if i == num - 1 {
                match obj {
                    // If the object was found, use the response.
                    Some(val) => {
                        mul = val.read()[0] as u64;
                    }

                    // If the object was not found, write an error message to the
                    // response.
                    None => {
                        let error = "Object does not exist";
                        db.resp(error.as_bytes());
                        return 1;
                    }
                }
            } else {
                // find the key for the second request.
                match obj {
                    // If the object was found, find the key from the response.
                    Some(val) => {
                        keys[0..4].copy_from_slice(&val.read()[0..4]);
                    }

                    // If the object was not found, write an error message to the
                    // response.
                    None => {
                        let error = "Object does not exist";
                        db.resp(error.as_bytes());
                        return 1;
                    }
                }
            }
        }

        if ord >= 600 {
            let start = cycles::rdtsc();
            while cycles::rdtsc() - start < 600 as u64 {}
            ord -= 600;
            yield 0;
        }

        // Compute part for this extension
        loop {
            if ord <= 2000 {
                let start = cycles::rdtsc();
                while cycles::rdtsc() - start < ord as u64 {}
                break;
            } else {
                let start = cycles::rdtsc();
                while cycles::rdtsc() - start < 2000 as u64 {}
                ord -= 2000;
                yield 0;
            }
        }

        db.resp(pack(&mul));
        return 0;
    })
}
