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

extern crate crypto_hash;
extern crate sandstorm;

use crypto_hash::{digest, Algorithm};

use sandstorm::boxed::Box;
use sandstorm::buf::{MultiReadBuf, ReadBuf};
use sandstorm::db::DB;
use sandstorm::pack::pack;
use sandstorm::rc::Rc;
use sandstorm::size_of;
use sandstorm::Generator;

/// Status codes for the response to the tenant.
const SUCCESSFUL: u8 = 0x01;
const INVALIDARG: u8 = 0x02;
const INVALIDKEY: u8 = 0x03;

const KEYLENGTH: u16 = 30;

macro_rules! GET1 {
    ($db:ident, $table:ident, $key:ident, $obj:ident) => {
        let (server, _, val) = $db.search_get_in_cache($table, &$key);
        if server == false {
            $obj = val;
        } else {
            $obj = $db.get($table, &$key);
        }
    };
}

macro_rules! MULTIGET1 {
    ($db:ident, $table:ident, $keylen:ident, $keys:ident, $buf:ident) => {
        let (server, _, val) = $db.search_multiget_in_cache($table, $keylen, $keys);
        if server == true {
            $buf = $db.multiget($table, $keylen, &$keys);
        } else {
            $buf = val;
        }
    };
}

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
        // Error code and response defined upfront so that results are written only
        // at the end of this function.
        let mut err = INVALIDARG;
        let mut num: u32 = 0;
        let mut aggr: u64 = 0;
        let mut optype: u8 = 0;
        let mut obj: Option<ReadBuf> = None;
        let mut buf: Option<MultiReadBuf> = None;
        {
            let arg: &[u8] = db.args();
            let (t, rem) = arg.split_at(size_of::<u64>());
            let (n, rem) = rem.split_at(size_of::<u32>());
            let (key, op) = rem.split_at(size_of::<u64>());
            optype = op[0];

            // Get the table id from the unwrapped arguments.
            let mut table: u64 = 0;
            for (idx, e) in t.iter().enumerate() {
                table |= (*e as u64) << (idx << 3);
            }

            // Get the number of keys to aggregate across.
            for (idx, e) in n.iter().enumerate() {
                num |= (*e as u32) << (idx << 3);
            }

            // Retrieve the list of keys to aggregate across.
            //let obj = db.get(table, key);
            GET1!(db, table, key, obj);

            // Try performing the aggregate if the key list was successfully retrieved.
            if let Some(val) = obj {
                let value = val.read().split_at((KEYLENGTH as usize) * (num as usize)).0;

                MULTIGET1!(db, table, KEYLENGTH, value, buf);
            }
        }
        match buf {
            Some(vals) => {
                let mut i = 0;
                while vals.next() {
                    if i < num {
                        if optype == 1 {
                            let result = digest(Algorithm::MD5, vals.read());
                            aggr += result[0] as u64;
                        }
                        if optype == 2 {
                            let result = digest(Algorithm::SHA1, vals.read());
                            aggr += result[0] as u64;
                        }
                        if optype == 3 {
                            let result = digest(Algorithm::SHA256, vals.read());
                            aggr += result[0] as u64;
                        }
                        if optype == 4 {
                            let result = digest(Algorithm::SHA512, vals.read());
                            aggr += result[0] as u64;
                        }
                    } else {
                        break;
                    }
                    yield 0;
                }
            }

            None => {
                err = INVALIDKEY;
                db.resp(pack(&err));
                return 0;
            }
        }

        err = SUCCESSFUL;
        // First write in the response code.
        db.resp(pack(&err));
        // Second write the result.
        db.resp(pack(&aggr));

        return 0;

        // XXX: Unreachable, but required for compilation.
        yield 0;
    })
}
