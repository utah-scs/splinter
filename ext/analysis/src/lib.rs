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
#![allow(bare_trait_objects)]

extern crate bincode;
extern crate rustlearn;
#[macro_use]
extern crate sandstorm;

use std::ops::Generator;
use std::pin::Pin;
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
pub fn init(db: Rc<DB>) -> Pin<Box<Generator<Yield = u64, Return = u64>>> {
    Box::pin(move || {
        let mut obj = None;
        let mut table: u64 = 0;
        let mut key_value: u32 = 0;
        let mut number: u32 = 0;
        let mut keys: Vec<u8> = Vec::with_capacity(30);
        let mut values: Vec<Vec<u8>> = Vec::new();

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
            let (stable, remaining) = args.split_at(8);
            let (num, key) = remaining.split_at(4);
            keys.extend_from_slice(key);

            for (idx, e) in stable.iter().enumerate() {
                table |= (*e as u64) << (idx << 3);
            }

            for (idx, e) in num.iter().enumerate() {
                number |= (*e as u32) << (idx << 3);
            }

            for (idx, e) in key[0..4].iter().enumerate() {
                key_value |= (*e as u32) << (idx << 3);
            }
        }

        // Finally, lookup the database for the object.
        for _i in 0..number {
            GET!(db, table, keys, obj);
            match obj {
                Some(val) => {
                    values.push(val.read().clone().to_vec());
                }

                None => {
                    let error = "Object does not exist";
                    db.resp(error.as_bytes());
                    return 0;
                }
            }

            key_value += 1;
            keys[0..4].copy_from_slice(&transform_u32_to_u8_slice(key_value));
        }
        // Deserialize took some CPU compute and the extension should yield to
        // avoid as classified as misbehaving extension.
        yield 0;

        // If the object was found, perform the classification on the object and write it
        // to the response.
        for value in values.iter() {
            match db.get_model() {
                Some(model) => {
                    let predict: Vec<f32> = bincode::deserialize(&value).unwrap();
                    let response = model
                        .deserialized
                        .predict(&Array::from(&vec![predict]))
                        .unwrap()
                        .data()[0];
                    db.resp(pack(&response));
                }

                None => {
                    let error = "ML Model does not exist";
                    db.resp(error.as_bytes());
                    return 0;
                }
            }
        }
        return 0;
    })
}

fn transform_u32_to_u8_slice(x: u32) -> [u8; 4] {
    let b4: u8 = ((x >> 24) & 0xff) as u8;
    let b3: u8 = ((x >> 16) & 0xff) as u8;
    let b2: u8 = ((x >> 8) & 0xff) as u8;
    let b1: u8 = (x & 0xff) as u8;
    return [b1, b2, b3, b4];
}
