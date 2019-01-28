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

use sandstorm::buf::{MultiReadBuf, ReadBuf, Record, WriteBuf};
use sandstorm::db::DB;

extern crate bytes;
use self::bytes::{Bytes, BytesMut};

pub struct ProxyDB {
    parent_id: u64,
}

impl ProxyDB {
    pub fn new(id: u64) -> ProxyDB {
        ProxyDB { parent_id: id }
    }
}

impl DB for ProxyDB {
    fn get(&self, table: u64, key: &[u8]) -> Option<ReadBuf> {
        self.debug_log(&format!(
            "Invoked get() on table {} for key {:?}",
            table, key
        ));

        unsafe { Some(ReadBuf::new(Bytes::with_capacity(0))) }
    }

    fn multiget(&self, table: u64, key_len: u16, keys: &[u8]) -> Option<MultiReadBuf> {
        self.debug_log(&format!(
            "Invoked multiget() on table {} for keys {:?} with key length {}",
            table, keys, key_len
        ));

        unsafe { Some(MultiReadBuf::new(Vec::new())) }
    }

    fn alloc(&self, table: u64, key: &[u8], val_len: u64) -> Option<WriteBuf> {
        self.debug_log(&format!(
            "Invoked alloc(), table {}, key {:?}, val_len {}",
            table, key, val_len
        ));

        unsafe { Some(WriteBuf::new(table, BytesMut::with_capacity(0))) }
    }

    fn put(&self, buf: WriteBuf) -> bool {
        unsafe {
            self.debug_log(&format!("Invoked put(), buf {:?}", &buf.freeze().1[..]));
        }

        return true;
    }

    fn del(&self, table: u64, key: &[u8]) {
        self.debug_log(&format!(
            "Invoked del() on table {} for key {:?}",
            table, key
        ));
    }

    fn args(&self) -> &[u8] {
        self.debug_log(&format!("Invoked args(), {}", self.parent_id));
        return &[];
    }

    fn resp(&self, data: &[u8]) {
        self.debug_log(&format!("Invoked resp(), data {:?}", data));
    }

    fn debug_log(&self, _message: &str) {}

    fn populate_read_write_set(&self, _record: Record) {
        self.debug_log(&format!("Added a record to read/write set"));
    }

    /// Lookup the `DB` trait for documentation on this method.
    fn search_get_in_cache(&self, _table: u64, _key: &[u8]) -> (bool, bool, Option<ReadBuf>) {
        (false, false, None)
    }
}
