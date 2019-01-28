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

use sandstorm::buf::{MultiReadBuf, ReadBuf, Record, WriteBuf};
use sandstorm::db::DB;

use super::dispatch::*;

extern crate bytes;
use self::bytes::{Bytes, BytesMut};

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
}

impl ProxyDB {
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
        }
    }

    fn set_waiting(&self, value: bool) {
        *self.waiting.borrow_mut() = value;
    }

    pub fn get_waiting(&self) -> bool {
        self.waiting.borrow().clone()
    }
}

impl DB for ProxyDB {
    fn get(&self, table: u64, key: &[u8]) -> Option<ReadBuf> {
        self.set_waiting(false);
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
        self.req.split_at(self.args_offset).1
    }

    fn resp(&self, data: &[u8]) {
        self.debug_log(&format!("Invoked resp(), data {:?}", data));
    }

    fn debug_log(&self, _message: &str) {}

    fn populate_read_write_set(&self, _record: Record) {
        self.debug_log(&format!("Added a record to read/write set"));
    }

    /// Lookup the `DB` trait for documentation on this method.
    fn search_get_in_cache(&self, table: u64, key: &[u8]) -> (bool, bool, Option<ReadBuf>) {
        let found = false;
        if found == false {
            self.set_waiting(true);
            self.sender.send_get(self.tenant, table, key, self.parent_id);
        }
        (false, found, None)
    }
}
