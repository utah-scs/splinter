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

use std::cell::{Cell, RefCell};
use std::sync::Arc;
use std::{mem, slice, str};

use super::alloc::Allocator;
use super::cycles::*;
use super::tenant::Tenant;
use super::tx::TX;
use super::wireformat::{InvokeRequest, InvokeResponse, RpcStatus};
use util::model::Model;

use sandstorm::buf::{MultiReadBuf, OpType, ReadBuf, Record, WriteBuf};
use sandstorm::common::*;
use sandstorm::db::DB;

use e2d2::common::EmptyMetadata;
use e2d2::interface::Packet;

/// The maximum number of bytes that can be allocated by an instance of an
/// extension on the table heap.
const MAX_ALLOC: usize = 10240;

/// The flag to enable-disable including the RW set in the pushback response.
const INCLUDE_RWSET: bool = true;

/// This type is passed into the init method of every extension. The methods
/// on this type form the interface allowing extensions to read and write
/// data from and to the database. The constructors for this type (new() and
/// default()) should be exposed only to trusted code, and not to extensions.
pub struct Context<'a> {
    // The packet/buffer consisting of the RPC request header and payload
    // that invoked the extension. This is required to potentially pass in
    // arguments to an extension. For example, a get() extension might require
    // a key and table identifier to be passed in.
    request: Packet<InvokeRequest, EmptyMetadata>,

    // The offset inside the request packet/buffer's payload at which the
    // arguments to the extension begin.
    args_offset: usize,

    // The total length of the extension's arguments that were written into the
    // request packet/buffer's payload.
    args_length: usize,

    // A pre-populated RPC response packet/buffer for the invoked extension.
    // This is required because the extension might need to return something
    // to the issuing client/tenant. For example, a get() extension will need
    // to return a value to the issuing client/tenant.
    response: RefCell<Packet<InvokeResponse, EmptyMetadata>>,

    // The tenant that invoked this extension. Required to access the tenant's
    // data, and potentially for accounting.
    tenant: Arc<Tenant>,

    // The allocator that will be used to allow the extension to write data to
    // one of it's tables.
    heap: &'a Allocator,

    // The total number of bytes allocated by the extension so far
    // (on the table heap).
    allocs: Cell<usize>,

    // The buffer which maintains the read/write set per extension.
    tx: RefCell<TX>,

    // The credit which the extension has earned by making the db calls.
    db_credit: RefCell<u64>,

    // The model for a given extension which is stored based on the name of the extension.
    model: Option<Arc<Model>>,
}

// Methods on Context.
impl<'a> Context<'a> {
    /// This function returns a context that can be used to invoke an extension.
    ///
    /// # Arguments
    ///
    /// * `req`:      The invoke() RPC request packet/buffer consisting of the
    ///               header and payload.
    /// * `args_off`: The offset into the payload of `req` at which the
    ///               extension's arguments begin.
    /// * `args_len`: The length of the extension's arguments that were written
    ///               into the payload of `req`.
    /// * `res`:      A pre-allocated RPC response packet/buffer consisting of a
    ///               response header for the invoke() request.
    /// * `tenant`:   An `Arc` to the tenant that issued the invoke() request.
    /// * `alloc`:    An `Arc` to the memory allocator. Required to allow the
    ///               extension to issue writes to the database.
    ///
    /// # Result
    /// A context that can be used to invoke an extension.
    pub fn new(
        req: Packet<InvokeRequest, EmptyMetadata>,
        args_off: usize,
        args_len: usize,
        res: Packet<InvokeResponse, EmptyMetadata>,
        tenant: Arc<Tenant>,
        alloc: &'a Allocator,
        model: Option<Arc<Model>>,
    ) -> Context<'a> {
        Context {
            request: req,
            args_offset: args_off,
            args_length: args_len,
            response: RefCell::new(res),
            tenant: tenant,
            heap: alloc,
            allocs: Cell::new(0),
            tx: RefCell::new(TX::new()),
            db_credit: RefCell::new(0),
            model: model,
        }
    }

    /// This method commits any changes made by an extension to the database.
    /// It consumes the context, and returns the request and response
    /// packets/buffers to the caller.
    ///
    /// # Return
    /// A tupule whose first member is the request packet/buffer for the
    /// extension, and whose second member is the response packet/buffer
    /// that can be sent back to the tenant.
    pub unsafe fn commit(
        self,
    ) -> (
        Packet<InvokeRequest, EmptyMetadata>,
        Packet<InvokeResponse, EmptyMetadata>,
    ) {
        return (self.request, self.response.into_inner());
    }

    /// This method modifies the response for the pushback. It changes the status in the response
    /// from StatusOk to StatusPushback. Besides that the function also modifies the response
    /// packet to remove the old response and attach the records which the extension has read or
    /// written(Read Write Set), so that the client can resume the execution on its end.
    pub fn prepare_for_pushback(&self) {
        self.response
            .borrow_mut()
            .get_mut_header()
            .common_header
            .status = RpcStatus::StatusPushback;

        if INCLUDE_RWSET {
            // Remove the original payload and append the read-write set to the response payload.
            let payload_len = self.response.borrow().get_payload().len();
            match self
                .response
                .borrow_mut()
                .remove_from_payload_tail(payload_len)
            {
                Ok(_) => {}

                Err(ref err) => {
                    error!(
                        "Unable to delete previous payload while doing pushback {}",
                        err
                    );
                }
            }

            // Add the read-set to the pushback response.
            for record in self.tx.borrow_mut().reads().iter() {
                let ptr = &record.get_optype() as *const _ as *const u8;
                let slice = unsafe { slice::from_raw_parts(ptr, mem::size_of::<OpType>()) };
                self.resp(slice);
                self.resp(record.get_key().as_ref());
                self.resp(record.get_object().as_ref());
            }

            // Add the write-set to the pushback response.
            for record in self.tx.borrow_mut().writes().iter() {
                let ptr = &record.get_optype() as *const _ as *const u8;
                let slice = unsafe { slice::from_raw_parts(ptr, mem::size_of::<OpType>()) };
                self.resp(slice);
                self.resp(record.get_key().as_ref());
                self.resp(record.get_object().as_ref());
            }
        }
    }

    /// This method returns the value of the credit which an extension has accumulated over time.
    /// The extension credit is increased whenever it makes a DB function call; like get(),
    /// multiget(), put(), etc. For each DB call the credit is time spent in the called function
    /// plus some extra credit which the datastore need to waste in RPC handling.
    ///
    /// # Return
    ///
    /// The current value of the credit for the extension.
    pub fn db_credit(&self) -> u64 {
        self.db_credit.borrow().clone()
    }
}

// The DB trait for Context.
impl<'a> DB for Context<'a> {
    /// Lookup the `DB` trait for documentation on this method.
    fn get(&self, table_id: u64, key: &[u8]) -> Option<ReadBuf> {
        // Lookup the database for the key value pair. If it exists, then update
        // the read set and return the value.
        let start = rdtsc();
        self.tenant.get_table(table_id)
                    .and_then(| table | { table.get(key) })
                    // The object exists in the database. Get a handle to it's
                    // key and value.
                    .and_then(| entry | { self.heap.resolve(entry.value) })
                    // Return the value wrapped up inside a safe type.
                    .and_then(| (k, v) | {
                        self.tx.borrow_mut().record_get(Record::new(OpType::SandstormRead, k.clone(), v.clone()));
                        *self.db_credit.borrow_mut() += rdtsc() - start + GET_CREDIT;
                        unsafe { Some(ReadBuf::new(v)) }
                        })
    }

    /// Lookup the `DB` trait for documentation on this method.
    fn multiget(&self, table_id: u64, key_len: u16, keys: &[u8]) -> Option<MultiReadBuf> {
        // Lookup the database for each key in the supplied list of keys. If all exist,
        // return a MultiReadBuf to the extension.
        let start = rdtsc();
        if let Some(table) = self.tenant.get_table(table_id) {
            let mut objs = Vec::new();

            // Iterate through the list of keys. Lookup each one of them at the database.
            for key in keys.chunks(key_len as usize) {
                if key.len() != key_len as usize {
                    break;
                }

                let r = table
                    .get(key)
                    .and_then(|obj| self.heap.resolve(obj.value))
                    .and_then(|(k, v)| {
                        self.tx.borrow_mut().record_get(Record::new(
                            OpType::SandstormRead,
                            k.clone(),
                            v.clone(),
                        ));
                        objs.push(v);
                        Some(())
                    });

                if r.is_none() {
                    *self.db_credit.borrow_mut() += rdtsc() - start + MULTIGET_CREDIT;
                    return None;
                }
            }

            unsafe {
                *self.db_credit.borrow_mut() += rdtsc() - start + MULTIGET_CREDIT;
                return Some(MultiReadBuf::new(objs));
            }
        }
        *self.db_credit.borrow_mut() += rdtsc() - start + MULTIGET_CREDIT;
        return None;
    }

    /// Lookup the `DB` trait for documentation on this method.
    fn alloc(&self, table_id: u64, key: &[u8], val_len: u64) -> Option<WriteBuf> {
        // If the extension has exceeded it's quota, do not allow any more allocs.
        if self.allocs.get() >= MAX_ALLOC {
            return None;
        }

        // Check if the tenant owns a table with the requested identifier.
        // If it does, perform and return an allocation.
        self.tenant
            .get_table(table_id)
            .and_then(|_table| self.heap.raw(self.tenant.id(), table_id, key, val_len))
            .and_then(|buf| {
                self.allocs.set(self.allocs.get() + buf.len());
                unsafe { Some(WriteBuf::new(table_id, buf)) }
            })
    }

    /// Lookup the `DB` trait for documentation on this method.
    fn put(&self, buf: WriteBuf) -> bool {
        let start = rdtsc();
        // Convert the passed in Writebuf to read only.
        let (table_id, buf) = unsafe { buf.freeze() };

        // If the table exists, write to the database.
        if let Some(table) = self.tenant.get_table(table_id) {
            return self.heap.resolve(buf.clone()).map_or(false, |(k, _v)| {
                self.tx.borrow_mut().record_put(Record::new(
                    OpType::SandstormWrite,
                    k.clone(),
                    buf.clone(),
                ));
                table.put(k, buf);
                *self.db_credit.borrow_mut() += rdtsc() - start + PUT_CREDIT;
                true
            });
        }

        *self.db_credit.borrow_mut() += rdtsc() - start + PUT_CREDIT;
        return false;
    }

    /// Lookup the `DB` trait for documentation on this method.
    fn del(&self, table_id: u64, key: &[u8]) {
        // Delete the key-value pair from the database
        if let Some(table) = self.tenant.get_table(table_id) {
            table.delete(key);
        }
    }

    /// Lookup the `DB` trait for documentation on this method.
    fn args(&self) -> &[u8] {
        // Return a slice to the arguments off the request packet/buffer's
        // payload.
        self.request
            .get_payload()
            .split_at(self.args_offset)
            .1
            .split_at(self.args_length)
            .0
    }

    /// Lookup the `DB` trait for documentation on this method.
    fn resp(&self, data: &[u8]) {
        // Write the passed in data to the response packet/buffer.
        self.response
            .borrow_mut()
            .add_to_payload_tail(data.len(), data)
            .unwrap();
    }

    /// Lookup the `DB` trait for documentation on this method.
    fn debug_log(&self, _msg: &str) {}

    /// Lookup the `DB` trait for documentation on this method.
    fn search_get_in_cache(&self, _table: u64, _key: &[u8]) -> (bool, bool, Option<ReadBuf>) {
        (true, false, None)
    }

    fn search_multiget_in_cache(
        &self,
        _table: u64,
        _key_len: u16,
        _keys: &[u8],
    ) -> (bool, bool, Option<MultiReadBuf>) {
        return (true, false, None);
    }

    fn get_model(&self) -> Option<Arc<Model>> {
        match self.model {
            Some(ref model) => Some(Arc::clone(&model)),
            None => None,
        }
    }
}
