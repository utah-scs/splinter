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
#![feature(no_unsafe)]
#![feature(generators, generator_trait)]

extern crate bincode;
extern crate rustlearn;
#[macro_use]
extern crate sandstorm;

use std::ops::Generator;
use std::rc::Rc;

use rustlearn::prelude::*;
use rustlearn::traits::SupervisedModel;

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
        let mut table: u64 = 0;
        let mut keys: Vec<u8> = Vec::with_capacity(30);
        let mut predict: Vec<f32> = Vec::with_capacity(25);

        {
            // First off, retrieve the arguments to the extension.
            let args = db.args();

            // Check that the arguments received is long enough to contain an
            // 8 byte table id and a key to be looked up. If not, then write
            // an error message to the response and return to the database.
            if args.len() <= 38 {
                let error = "Invalid args";
                db.resp(error.as_bytes());
                return 1;
            }

            // Next, split the arguments into a view over the table identifier
            // (first eight bytes), and a view over the key to be looked up.
            // De-serialize the table identifier into a u64.
            let (stable, key) = args.split_at(8);
            keys.extend_from_slice(key);

            for (idx, e) in stable.iter().enumerate() {
                table |= (*e as u64) << (idx << 3);
            }
        }

        // Finally, lookup the database for the object.
        GET!(db, table, keys, obj);
        match obj {
            Some(val) => {
                let value = val.read();
                predict = bincode::deserialize(&value).unwrap();
            }

            None => {
                let error = "Object does not exist";
                db.resp(error.as_bytes());
                return 0;
            }
        }

        // Deserialize took some CPU compute and the extension should yield to
        // avoid as classified as misbehaving extension.
        yield 0;

        // If the object was found, perform the classification on the object and write it
        // to the response.
        match db.get_model() {
            Some(model) => {
                let response = model
                    .deserialized
                    .predict(&Array::from(&vec![predict]))
                    .unwrap()
                    .data()[0];
                db.resp(pack(&response));
                return 0;
            }

            None => {
                let error = "ML Model does not exist";
                db.resp(error.as_bytes());
                return 0;
            }
        }
    })
}