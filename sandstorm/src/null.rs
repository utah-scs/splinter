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

use super::buf::{ReadBuf, Record, WriteBuf, MultiReadBuf};

/// A null database of testing and benchmarking purposes.
pub struct NullDB {}

impl NullDB {
    /// This method creates a new instance of MockDB.
    pub fn new() -> NullDB {
        NullDB {}
    }

    /// Empty function.
    pub fn assert_messages<S>(&self, _messages: &[S])
    where
        S: Debug + PartialEq<String>,
    {
    }

    /// Empty function.
    pub fn clear_messages(&self) {}
}

impl DB for NullDB {
    fn get(&self, _table: u64, _key: &[u8]) -> Option<ReadBuf> {
        return None;
    }

    fn multiget(&self, _table: u64, _key_len: u16, _keys: &[u8]) -> Option<MultiReadBuf> {
        return None;
    }

    fn alloc(&self, _table: u64, _key: &[u8], _val_len: u64) -> Option<WriteBuf> {
        return None;
    }

    fn put(&self, _buf: WriteBuf) -> bool {
        return false;
    }

    fn del(&self, _table: u64, _key: &[u8]) {}

    fn args(&self) -> &[u8] {
        return &[];
    }

    fn resp(&self, _data: &[u8]) {}

    fn debug_log(&self, _message: &str) {}

    fn populate_read_write_set(&self, _record: Record) {}

    fn search_get_in_cache(&self, _table: u64, _key: &[u8]) -> (bool, bool, Option<ReadBuf>) {
        (false, false, None)
    }
}
