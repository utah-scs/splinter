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

use std::fmt::Debug;

use super::db::DB;
use super::buf::ReadBuf;

use bytes::Bytes;

pub struct NullDB {}

impl NullDB {
    pub fn new() -> NullDB {
        NullDB{}
    }

    pub fn assert_messages<S>(&self, _messages: &[S])
        where S: Debug + PartialEq<String>
    {}

    pub fn clear_messages(&self) {}
}

impl DB for NullDB {
    fn get(&self, _table: u64, _key: &[u8]) -> ReadBuf {
        unsafe {
            ReadBuf::new(Bytes::with_capacity(0))
        }
    }

    fn debug_log(&self, _message: &str) {}
}
