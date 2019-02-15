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

use std::cell::RefCell;
use std::sync::Arc;

use db::cycles::*;

use sandstorm::buf::{MultiReadBuf, ReadBuf, Record, WriteBuf};
use sandstorm::db::DB;

use super::dispatch::*;

extern crate bytes;
use self::bytes::{Bytes, BytesMut};

/// This struct represents a record for a read/write set. Each record in the read/write set will
/// be of this type.
#[derive(Clone)]
pub struct KV {
    /// This variable stores the Key for the record.
    pub key: Bytes,

    /// This variable stores the Value for the record.
    pub value: Bytes,
}

impl KV {
    /// This method creates and returns a record consists of key and value.
    ///
    /// # Arguments
    /// * `rkey`: The key in the record.
    /// * `rvalue`: The value in the record.
    ///
    /// # Return
    ///
    /// A record with a key and a value.
    fn new(rkey: Bytes, rvalue: Bytes) -> KV {
        KV {
            key: rkey,
            value: rvalue,
        }
    }
}

pub struct ProxyDB {
    // The tenant-id for which the invoke() function was called for the parent request.
    tenant: u32,

    // After pushback, each subsequent request(get/put) will have the same packet identifier
    // as the first request.
    parent_id: u64,

    // The buffer consisting of the RPC payload that invoked the extension. This is required
    // to potentially pass in arguments to an extension. For example, a get() extension might
    // require a key and table identifier to be passed in.
    req: Arc<Vec<u8>>,

    // The offset inside the request packet/buffer's payload at which the
    // arguments to the extension begin.
    args_offset: usize,

    // The flag to indicate if the current extension is waiting for the DB operation to complete.
    // This flag will be used by the scheduler to avoid scheduling the task until the response comes.
    waiting: RefCell<bool>,

    // Network stack required to actually send RPC requests out the network.
    sender: Arc<Sender>,

    // A list of the records in Read-set for the extension.
    readset: RefCell<Vec<KV>>,

    // A list of records in the write-set for the extension.
    writeset: RefCell<Vec<KV>>,

    // The credit which the extension has earned by making the db calls.
    db_credit: RefCell<u64>,
}

impl ProxyDB {
    /// This method creates and returns the `ProxyDB` object. This DB issues the remote RPC calls
    /// instead of local table lookups.
    ///
    /// #Arguments
    ///
    /// * `tenant_id`: Tenant id will be needed reuqest generation.
    /// * `id`: This is unique-id for the request and consecutive requests will have same id.
    /// * `request`: A reference to the request sent by the client, it will be helpful in task creation
    ///             if the requested is pushed back.
    /// * `name_length`: This will be useful in parsing the request and find out the argument for consecutive requests.
    /// * `sender_service`: A reference to the service which helps in the RPC request generation.
    ///
    /// # Return
    ///
    /// A DB object which either manipulates the record in RW set or perform remote RPCs.
    pub fn new(
        tenant_id: u32,
        id: u64,
        request: Arc<Vec<u8>>,
        name_length: usize,
        sender_service: Arc<Sender>,
    ) -> ProxyDB {
        ProxyDB {
            tenant: tenant_id,
            parent_id: id,
            req: request,
            args_offset: name_length,
            waiting: RefCell::new(false),
            sender: sender_service,
            readset: RefCell::new(Vec::with_capacity(4)),
            writeset: RefCell::new(Vec::with_capacity(4)),
            db_credit: RefCell::new(0),
        }
    }

    /// This method can change the waiting flag to true/false. This flag is used to move the
    /// task between suspended or blocked task queue.
    ///
    /// # Arguments
    ///
    /// * `value`: A boolean value, which can be true or false.
    fn set_waiting(&self, value: bool) {
        *self.waiting.borrow_mut() = value;
    }

    /// This method return the current value of waiting flag. This flag is used to move the
    /// task between suspended or blocked task queue.
    pub fn get_waiting(&self) -> bool {
        self.waiting.borrow().clone()
    }

    /// This method is used to add a record to the read set. The return value of get()/multiget()
    /// goes the read set.
    ///
    /// # Arguments
    /// * `record`: A reference to a record with a key and a value.
    pub fn set_read_record(&self, record: &[u8]) {
        let (key, value) = record.split_at(30);
        self.readset
            .borrow_mut()
            .push(KV::new(Bytes::from(key), Bytes::from(value)));
    }

    /// This method is used to add a record to the write set. The return value of put()
    /// goes the write set.
    ///
    /// # Arguments
    /// * `record`: A reference to a record with a key and a value.
    pub fn set_write_record(&self, record: &[u8]) {
        let (key, value) = record.split_at(30);
        self.writeset
            .borrow_mut()
            .push(KV::new(Bytes::from(key), Bytes::from(value)));
    }

    /// This method search the a list of records to find if a record with the given key
    /// exists or not.
    ///
    /// # Arguments
    ///
    /// * `list`: A list of records, which can be the read set or write set for the extension.
    /// * `key`: A reference to a key to be looked up in the list.
    ///
    /// # Return
    ///
    /// The index of the element, if present. 1024 otherwise.
    pub fn search_cache(&self, list: Vec<KV>, key: &[u8]) -> usize {
        let length = list.len();
        for i in 0..length {
            if list[i].key == key {
                return i;
            }
        }
        //Return some number way bigger than the cache size.
        return 1024;
    }

    /// This method returns the value of the credit which an extension has accumulated over time.
    /// The extension credit is increased whenever it makes a DB function call; like get(),
    /// multiget(), put(), etc.
    ///
    /// # Return
    ///
    /// The current value of the credit for the extension.
    pub fn db_credit(&self) -> u64 {
        self.db_credit.borrow().clone()
    }
}

impl DB for ProxyDB {
    /// Lookup the `DB` trait for documentation on this method.
    fn get(&self, _table: u64, _key: &[u8]) -> Option<ReadBuf> {
        let start = rdtsc();
        self.set_waiting(false);
        *self.db_credit.borrow_mut() += rdtsc() - start;
        unsafe { Some(ReadBuf::new(Bytes::with_capacity(0))) }
    }

    /// Lookup the `DB` trait for documentation on this method.
    fn multiget(&self, _table: u64, _key_len: u16, _keys: &[u8]) -> Option<MultiReadBuf> {
        unsafe { Some(MultiReadBuf::new(Vec::new())) }
    }

    /// Lookup the `DB` trait for documentation on this method.
    fn alloc(&self, table: u64, _key: &[u8], _val_len: u64) -> Option<WriteBuf> {
        unsafe { Some(WriteBuf::new(table, BytesMut::with_capacity(0))) }
    }

    /// Lookup the `DB` trait for documentation on this method.
    fn put(&self, _buf: WriteBuf) -> bool {
        return true;
    }

    /// Lookup the `DB` trait for documentation on this method.
    fn del(&self, _table: u64, _key: &[u8]) {}

    /// Lookup the `DB` trait for documentation on this method.
    fn args(&self) -> &[u8] {
        self.req.split_at(self.args_offset).1
    }

    /// Lookup the `DB` trait for documentation on this method.
    fn resp(&self, _data: &[u8]) {}

    /// Lookup the `DB` trait for documentation on this method.
    fn debug_log(&self, _message: &str) {}

    /// Lookup the `DB` trait for documentation on this method.
    fn populate_read_write_set(&self, _record: Record) {}

    /// Lookup the `DB` trait for documentation on this method.
    fn search_get_in_cache(&self, table: u64, key: &[u8]) -> (bool, bool, Option<ReadBuf>) {
        let start = rdtsc();
        let index = self.search_cache(self.readset.borrow().to_vec(), key);
        if index != 1024 {
            let value = self.readset.borrow()[index].value.clone();
            *self.db_credit.borrow_mut() += rdtsc() - start;
            return (false, true, unsafe { Some(ReadBuf::new(value)) });
        }
        self.set_waiting(true);
        self.sender
            .send_get_from_extension(self.tenant, table, key, self.parent_id);
        *self.db_credit.borrow_mut() += rdtsc() - start;
        (false, false, None)
    }
}
