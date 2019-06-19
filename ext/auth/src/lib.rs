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

extern crate crypto;
#[macro_use]
extern crate sandstorm;

use crypto::bcrypt::bcrypt;

use std::ops::Generator;
use std::rc::Rc;
use std::pin::Pin;

use sandstorm::db::DB;
use sandstorm::pack::pack;

/// Status codes for the response to the tenant.
const INVALIDARG: u8 = 0x01;
const SUCCESSFUL: u8 = 0x02;
const UNSUCCESSFUL: u8 = 0x03;
const ABSENTOBJECT: u8 = 0x4;

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
        let mut status = INVALIDARG;
        let mut username: Vec<u8> = Vec::with_capacity(30);
        let mut password: Vec<u8> = Vec::with_capacity(72);

        {
            // First off, retrieve the arguments to the extension.
            let args = db.args();

            // Check that the arguments received is long enough to contain an
            // 8 byte table id, a 30 byte key to be looked up and a 72 byte
            // password to match. If not, then write an error message to the
            // response and return to the database.
            if args.len() != 110 {
                db.resp(pack(&status));
                return 1;
            }

            // Next, split the arguments into a view over the table identifier
            // (first eight bytes), and a view over the key to be looked up.
            // De-serialize the table identifier into a u64.
            let (s_table, remain_args) = args.split_at(8);
            let (userid, pass) = remain_args.split_at(30);
            username.extend_from_slice(userid);
            password.extend_from_slice(pass);

            // Get the table id from the unwrapped arguments.
            for (idx, e) in s_table.iter().enumerate() {
                table |= (*e as u64) << (idx << 3);
            }
        }

        // Finally, lookup the database for the object.
        GET!(db, table, username, obj);
        yield 0;

        // Populate a response to the tenant.
        match obj {
            // If the object was found, find it's hash and write it to the response.
            Some(val) => {
                // The value is 40 bytes long; 24 bytes for hash and 16 bytes for the salt.
                let bytes = val.read();
                if bytes.len() != 40 {
                    db.resp(pack(&status));
                    return 0;
                }
                let hash = &bytes[0..24];
                let salt = &bytes[24..40];

                // Compute the hash using salt and password, store in output.
                let output: &mut [u8] = &mut [0; 24];
                bcrypt(1, salt, &password, output);

                // Compare the calculated hash and DB stored hash.
                if output == hash {
                    status = SUCCESSFUL;
                    db.resp(pack(&status));
                } else {
                    status = UNSUCCESSFUL;
                    db.resp(pack(&status));
                }
                return 0;
            }

            // If the object was not found, write an error message to the
            // response.
            None => {
                status = ABSENTOBJECT;
                db.resp(pack(&status));
                return 0;
            }
        }

        // XXX: This yield is required to get the compiler to compile this closure into a
        // generator. It is unreachable and benign.
        yield 0;
    })
}
