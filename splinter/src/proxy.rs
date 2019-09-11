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
use std::{mem, slice};

use db::cycles::*;
use db::wireformat::*;

use sandstorm::buf::{MultiReadBuf, ReadBuf, WriteBuf};
use sandstorm::db::DB;

use super::dispatch::*;

extern crate bytes;
use self::bytes::{Bytes, BytesMut};
use util::model::Model;

/// This struct represents a record for a read/write set. Each record in the read/write set will
/// be of this type.
#[derive(Clone)]
pub struct KV {
    /// This variable stores the Version for the record.
    pub version: Bytes,

    /// This variable stores the Key for the record.
    pub key: Bytes,

    /// This variable stores the Value for the record.
    pub value: Bytes,
}

impl KV {
    /// This method creates and returns a record consists of key and value.
    ///
    /// # Arguments
    /// * `rversion`: The version in the record.
    /// * `rkey`: The key in the record.
    /// * `rvalue`: The value in the record.
    ///
    /// # Return
    ///
    /// A record with a key and a value.
    pub fn new(rversion: Bytes, rkey: Bytes, rvalue: Bytes) -> KV {
        KV {
            version: rversion,
            key: rkey,
            value: rvalue,
        }
    }
}

/// A proxy to the database on the client side; which searches the
/// local cache before issuing the operations to the server.
pub struct ProxyDB {
    // The tenant-id for which the invoke() function was called for the parent request.
    tenant: u32,

    // After pushback, each subsequent request(get/put) will have the same packet identifier
    // as the first request.
    parent_id: u64,

    // The buffer consisting of the RPC payload that invoked the extension. This is required
    // to potentially pass in arguments to an extension. For example, a get() extension might
    // require a key and table identifier to be passed in.
    req: Vec<u8>,

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

    // The model for a given extension which is stored based on the name of the extension.
    model: Option<Arc<Model>>,

    // This maintains the read-write records accessed by the extension.
    commit_payload: RefCell<Vec<u8>>,
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
        request: Vec<u8>,
        name_length: usize,
        sender_service: Arc<Sender>,
        model: Option<Arc<Model>>,
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
            model: model,
            commit_payload: RefCell::new(Vec::new()),
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
    pub fn set_read_record(&self, record: &[u8], keylen: usize) {
        let ptr = &OpType::SandstormRead as *const _ as *const u8;
        let optype = unsafe { slice::from_raw_parts(ptr, mem::size_of::<OpType>()) };
        self.commit_payload.borrow_mut().extend_from_slice(optype);
        self.commit_payload.borrow_mut().extend_from_slice(record);
        let (version, entry) = record.split_at(8);
        let (key, value) = entry.split_at(keylen);
        self.readset.borrow_mut().push(KV::new(
            Bytes::from(version),
            Bytes::from(key),
            Bytes::from(value),
        ));
    }

    /// This method is used to add a record to the write set. The return value of put()
    /// goes the write set.
    ///
    /// # Arguments
    /// * `record`: A reference to a record with a key and a value.
    pub fn set_write_record(&self, record: &[u8], keylen: usize) {
        let ptr = &OpType::SandstormWrite as *const _ as *const u8;
        let optype = unsafe { slice::from_raw_parts(ptr, mem::size_of::<OpType>()) };
        self.commit_payload.borrow_mut().extend_from_slice(optype);
        self.commit_payload.borrow_mut().extend_from_slice(record);
        let (version, entry) = record.split_at(8);
        let (key, value) = entry.split_at(keylen);
        self.writeset.borrow_mut().push(KV::new(
            Bytes::from(version),
            Bytes::from(key),
            Bytes::from(value),
        ));
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

    /// This method send a request to the server to commit the transaction.
    pub fn commit(&self) {
        if self.readset.borrow().len() > 0 || self.writeset.borrow().len() > 0 {
            let mut table_id = 0;
            let mut key_len = 0;
            let mut val_len = 0;

            // find the table_id for the transaction.
            let args = self.args();
            let (table, _) = args.split_at(8);
            for (idx, e) in table.iter().enumerate() {
                table_id |= (*e as u64) << (idx << 3);
            }

            // Find the key length and value length for records in RWset.
            if self.readset.borrow().len() > 0 {
                key_len = self.readset.borrow()[0].key.len();
                val_len = self.readset.borrow()[0].value.len();
            }

            if key_len == 0 && self.writeset.borrow().len() > 0 {
                key_len = self.writeset.borrow()[0].key.len();
                val_len = self.writeset.borrow()[0].value.len();
            }

            self.sender.send_commit(
                self.tenant,
                table_id,
                &self.commit_payload.borrow(),
                self.parent_id,
                key_len as u16,
                val_len as u16,
            );
        }
    }
}

impl DB for ProxyDB {
    /// Lookup the `DB` trait for documentation on this method.
    fn get(&self, _table: u64, key: &[u8]) -> Option<ReadBuf> {
        let start = rdtsc();
        self.set_waiting(false);
        let index = self.search_cache(self.readset.borrow().to_vec(), key);
        let value = self.readset.borrow()[index].value.clone();
        *self.db_credit.borrow_mut() += rdtsc() - start;
        unsafe { Some(ReadBuf::new(value)) }
    }

    /// Lookup the `DB` trait for documentation on this method.
    fn multiget(&self, _table: u64, key_len: u16, keys: &[u8]) -> Option<MultiReadBuf> {
        let mut objs = Vec::new();
        for key in keys.chunks(key_len as usize) {
            if key.len() != key_len as usize {
                break;
            }
            let index = self.search_cache(self.readset.borrow().to_vec(), key);
            if index != 1024 {
                let value = self.readset.borrow()[index].value.clone();
                objs.push(value);
            } else {
                info!("Multiget Failed");
            }
        }
        unsafe { Some(MultiReadBuf::new(objs)) }
    }

    /// Lookup the `DB` trait for documentation on this method.
    fn alloc(&self, table: u64, key: &[u8], val_len: u64) -> Option<WriteBuf> {
        unsafe {
            // Alloc for version, key and value.
            let mut writebuf = WriteBuf::new(
                table,
                BytesMut::with_capacity(8 + key.len() + val_len as usize),
            );
            writebuf.write_slice(&[0; 8]);
            writebuf.write_slice(key);
            Some(writebuf)
        }
    }

    /// Lookup the `DB` trait for documentation on this method.
    fn put(&self, buf: WriteBuf) -> bool {
        unsafe {
            let (_table_id, buf) = buf.freeze();
            assert_eq!(buf.len(), 138);
            self.set_write_record(&buf, 30);
        }
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

    /// Lookup the `DB` trait for documentation on this method.
    fn search_multiget_in_cache(
        &self,
        table: u64,
        key_len: u16,
        keys: &[u8],
    ) -> (bool, bool, Option<MultiReadBuf>) {
        let start = rdtsc();
        let mut objs = Vec::new();
        for key in keys.chunks(key_len as usize) {
            if key.len() != key_len as usize {
                return (false, false, None);
            }

            let index = self.search_cache(self.readset.borrow().to_vec(), key);
            if index != 1024 {
                let value = self.readset.borrow()[index].value.clone();
                objs.push(value);
            } else {
                self.set_waiting(true);
                self.sender
                    .send_get_from_extension(self.tenant, table, key, self.parent_id);
                *self.db_credit.borrow_mut() += rdtsc() - start;
                return (false, false, None);
            }
        }
        *self.db_credit.borrow_mut() += rdtsc() - start;
        return (false, true, unsafe { Some(MultiReadBuf::new(objs)) });
    }

    /// Lookup the `DB` trait for documentation on this method.
    fn get_model(&self) -> Option<Arc<Model>> {
        match self.model {
            Some(ref model) => Some(Arc::clone(&model)),
            None => None,
        }
    }
}
