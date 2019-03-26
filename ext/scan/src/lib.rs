/* Copyright (c) 2019 University of Utah
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

use std::ops::Generator;
use std::rc::Rc;

use sandstorm::db::DB;
use sandstorm::pack::pack;
use sandstorm::size_of;

/// This function implements the scan() extension using the sandstorm interface.
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
        let mut range: u32 = 0;
        let mut table: u64 = 0;
        let mut keys: Vec<u8> = Vec::with_capacity(30);
        let mut sum: u64 = 0;

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
            let (t, value) = args.split_at(size_of::<u64>());
            let (ord, key) = value.split_at(size_of::<u32>());
            keys.extend_from_slice(key);

            // Get the table id from the unwrapped arguments.
            for (idx, e) in t.iter().enumerate() {
                table |= (*e as u64) << (idx << 3);
            }

            // Get the number of keys to aggregate across.
            for (idx, e) in ord.iter().enumerate() {
                range |= (*e as u32) << (idx << 3);
            }
        }

        for _in in 0..range - 1 {
            // Finally, lookup the database for the object.
            obj = db.get(table, &keys);

            match obj {
                // If the object was found, write it to the response.
                Some(val) => {
                    sum += val.read()[0] as u64;
                    keys[0..4].copy_from_slice(&val.read()[0..4]);
                }

                // If the object was not found, write an error message to the
                // response.
                None => {
                    let error = "Object does not exist";
                    db.resp(error.as_bytes());
                    return 0;
                }
            }
        }
        db.resp(pack(&sum));
        return 0;

        // XXX: This yield is required to get the compiler to compile this closure into a
        // generator. It is unreachable and benign.
        yield 0;
    })
}
