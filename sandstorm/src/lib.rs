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

#![feature(type_ascription)]

extern crate bytes;

pub mod db;
pub mod buf;
pub mod null;
pub mod mock;
pub mod pack;

/// This type indicates to a procedure whether a call to the database
/// succeeded or failed.
pub enum SandstormErr {
    /// This value indicates that the call succeeded.
    Success,

    /// This value indicates that the call failed because the table
    /// does not exist.
    TableDoesNotExist,
}

pub trait DB {
    fn debug_log(&self, &str);

    fn create_table(&self, table_id: u64) -> bool;

    fn get_key<K, V>(&self, table_id: u64, key: &K) -> Result<&V, SandstormErr>;

    fn delete_key<K>(&self, table_id: u64, key: &K);

    fn put_key<K, V>(&self, table_id: u64, key: &K, value: &V) -> SandstormErr;

    fn alloc<K>(&self, table: u64, key: &K, val_len: usize) -> Option<Vec<u8>>;
}

use std::cell::RefCell;

pub struct MockDB {
    messages: RefCell<Vec<String>>,
}

impl MockDB {
    pub fn new() -> MockDB {
        MockDB{messages: RefCell::new(Vec::new())}
    }

    pub fn assert_messages<S>(&self, messages: &[S])
        where S: std::fmt::Debug + PartialEq<String>
    {
        let found = self.messages.borrow();
        assert_eq!(messages, found.as_slice());
    }

    pub fn clear_messages(&self) {
        let mut messages = self.messages.borrow_mut();
        messages.clear();
    }
}

impl DB for MockDB {

    fn debug_log(&self, message: &str) {
        let mut messages = self.messages.borrow_mut();
        messages.push(String::from(message));
    }

    fn create_table(&self, _id: u64) -> bool{
        return true
    }

    fn put_key<K, V>(&self, _table_id: u64, _key: &K, _value: &V) -> SandstormErr {
        SandstormErr::Success
    }

    fn get_key<K, V>(&self, _table_id: u64, _key: &K) -> Result<&V, SandstormErr> {
        Err(SandstormErr::TableDoesNotExist)
    }

    fn delete_key<K>(&self, _table_id: u64, _key: &K){
    }

    fn alloc<K>(&self, _table: u64, _key: &K, _val_len: usize) -> Option<Vec<u8>> {
        None
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
