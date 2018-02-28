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

use std::mem::size_of;
use std::collections::HashMap;

use super::wireformat::{InvokeRequest, InvokeResponse};

use bytes::Bytes;
use sandstorm::db::DB;
use sandstorm::buf::ReadBuf;
use e2d2::common::EmptyMetadata;
use e2d2::headers::{IpHeader, MacHeader, UdpHeader};
use e2d2::interface::{Packet, packet_from_mbuf_no_increment};

/// This type is passed into the init method of every extension. The methods
/// on this type form the interface allowing extensions to read and write
/// data from and to the database. The constructors for this type (new() and
/// default()) should be exposed only to trusted code, and not to extensions.
pub struct Context {
    // The packet/buffer consisting of the RPC request header and payload
    // that invoked the extension. This is required to potentially pass in
    // arguments to an extension. For example, a get() extension might require
    // a key and table identifier to be passed in.
    request: Packet<InvokeRequest, EmptyMetadata>,

    // A pre-populated RPC response packet/buffer for the invoked extension.
    // This is required because the extension might need to return something
    // to the issuing client/tenant. For example, a get() extension will need
    // to return a value to the issuing client/tenant.
    response: Packet<InvokeResponse, EmptyMetadata>,

    // The extension's read set. This is required to avoid holding locks for
    // every object read by an extension. Avoiding locks is extremely critical
    // because the database will not run requests to completion, and might
    // occassionally pre-empt an extension if it has been running for too long.
    reads: HashMap<Bytes, Bytes>,

    // The extension's write set. Required for the same reason as the read set.
    writes: HashMap<Bytes, Bytes>,

    // TODO: Add an Arc to an object manager in here. This is required
    // for allocations from the table heap, gets, and puts.
}

// Methods on Context.
impl Context {
    /// This function returns a context that can be used to invoke an extension.
    ///
    /// # Arguments
    ///
    /// * `req`: The invoke() RPC request packet/buffer consisting of the header
    ///          and payload.
    /// * `res`: A pre-allocated RPC response packet/buffer consisting of a
    ///          response header for the invoke() request.
    ///
    /// # Result
    /// A context that can be used to invoke an extension.
    pub fn new(req: Packet<InvokeRequest, EmptyMetadata>,
               res: Packet<InvokeResponse, EmptyMetadata>)
               -> Context
    {
        Context {
            request: req,
            response: res,
            reads: HashMap::new(),
            writes: HashMap::new(),
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
    pub unsafe fn commit(self) -> (Packet<InvokeRequest, EmptyMetadata>,
                                   Packet<InvokeResponse, EmptyMetadata>)
    {
        // Rewrap the request into a new packet.
        let req = packet_from_mbuf_no_increment::<InvokeRequest>(
                    self.request.get_mbuf(), size_of::<MacHeader>() +
                    size_of::<IpHeader>() + size_of::<UdpHeader>() +
                    size_of::<InvokeRequest>());

        // Rewrap the response into a new packet.
        let res = packet_from_mbuf_no_increment::<InvokeResponse>(
                    self.response.get_mbuf(), size_of::<MacHeader>() +
                    size_of::<IpHeader>() + size_of::<UdpHeader>() +
                    size_of::<InvokeResponse>());

        // Return the request and response packet. At this point, the context
        // is dropped, and can never be used again.
        return (req, res);
    }
}

// The DB trait for Context.
impl DB for Context {
    fn get(&self, _table: u64, _key: &[u8]) -> ReadBuf {
        unsafe {
            ReadBuf::new(Bytes::with_capacity(0))
        }
    }

    fn debug_log(&self, _msg: &str) {
        ;
    }
}
