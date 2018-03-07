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

extern crate sandstorm;

use sandstorm::db::DB;

#[no_mangle]
pub fn init(db: &DB) {
    let arg = db.args();

    let (table, key) = arg.split_at(8);
    let table: u64 = *table.get(0).unwrap() as u64 +
                        (*table.get(1).unwrap() as u64) * 2^8 +
                        (*table.get(2).unwrap() as u64) * 2^16 +
                        (*table.get(3).unwrap() as u64) * 2^24 +
                        (*table.get(4).unwrap() as u64) * 2^32 +
                        (*table.get(5).unwrap() as u64) * 2^40 +
                        (*table.get(6).unwrap() as u64) * 2^48 +
                        (*table.get(7).unwrap() as u64) * 2^56;

    let val = db.get(table, key)
                .unwrap();

    db.resp(val.read());

    println!("Table: {}, Key: {:?}, Value: {:?}", table, key, val.read());
}
