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

use std::ops::Generator;
use std::rc::Rc;

use sandstorm::db::DB;
use sandstorm::pack::pack;

macro_rules! GET {
    ($db:ident, $table:ident, $key:ident, $obj:ident) => {
        let (server, found, val) = $db.search_get_in_cache($table, &$key);
        if server == false {
            if found == false {
                yield 0;
                $obj = $db.get($table, &$key);
            } else {
                $obj = val;
            }
        } else {
            $obj = $db.get($table, &$key);
        }
    };
}

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
            let (table, key) = args.split_at(8);
            keys.extend_from_slice(key);

            t_table = 0
                | table[0] as u64
                | (table[1] as u64) << 8
                | (table[2] as u64) << 16
                | (table[3] as u64) << 24
                | (table[4] as u64) << 32
                | (table[5] as u64) << 40
                | (table[6] as u64) << 48
                | (table[7] as u64) << 56;
        }

        let range: usize = 1;
        let mut mul: u64 = 1;

        for i in 0..range {
            GET!(db, t_table, keys, obj);

            if i == range - 1 {
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
        yield 0;

        // Second half of the extension, which does .5 us of CPU works and returns.
        for i in 1..2000 {
            mul *= i;
        }
        db.resp(pack(&mul));
        return 0;
    })
}
