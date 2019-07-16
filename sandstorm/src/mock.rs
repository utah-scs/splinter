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

use super::buf::{MultiReadBuf, ReadBuf, WriteBuf};
use super::db::DB;

extern crate bytes;
use self::bytes::{Bytes, BytesMut};

use std::cell::RefCell;
use std::sync::Arc;
use util::model::Model;

/// A mock database of testing purposes.
pub struct MockDB {
    messages: RefCell<Vec<String>>,
    args: [u8; 30],
}

impl MockDB {
    /// This method creates a new instance of MockDB.
    pub fn new() -> MockDB {
        MockDB {
            messages: RefCell::new(Vec::new()),
            args: [97; 30],
        }
    }

    /// This method compares the given message with the already stored message.
    pub fn assert_messages<S>(&self, messages: &[S])
    where
        S: Debug + PartialEq<String>,
    {
        let found = self.messages.borrow();
        assert_eq!(messages, found.as_slice());
    }

    /// This method clears already stored message.
    pub fn clear_messages(&self) {
        let mut messages = self.messages.borrow_mut();
        messages.clear();
    }
}

impl DB for MockDB {
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
        self.debug_log(&format!("Invoked args()"));

        return &(self.args);
    }

    fn resp(&self, data: &[u8]) {
        self.debug_log(&format!("Invoked resp(), data {:?}", data));
    }

    fn debug_log(&self, message: &str) {
        let mut messages = self.messages.borrow_mut();
        messages.push(String::from(message));
    }

    fn search_get_in_cache(&self, table: u64, key: &[u8]) -> (bool, bool, Option<ReadBuf>) {
        self.debug_log(&format!(
            "Invoked search_get_in_cache() on table {} for key {:?}",
            table, key
        ));

        (false, false, unsafe {
            Some(ReadBuf::new(Bytes::with_capacity(0)))
        })
    }

    fn search_multiget_in_cache(
        &self,
        table: u64,
        key_len: u16,
        keys: &[u8],
    ) -> (bool, bool, Option<MultiReadBuf>) {
        self.debug_log(&format!(
            "Invoked multiget() on table {} for keys {:?} with key length {}",
            table, keys, key_len
        ));

        unsafe { (false, false, Some(MultiReadBuf::new(Vec::new()))) }
    }

    fn get_model(&self) -> Option<Arc<Model>> {
        None
    }
}
