/* Copyright (c) 2017 University of Utah
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

extern crate e2d2;

use std::mem::size_of;

use e2d2::headers::{ EndOffset, UdpHeader };

/// This enum represents the different sets of services that a Sandstorm server
/// can provide, and helps identify the service an incoming remote procedure
/// call (RPC) must be dispatched to.
///
/// Each service defines a set of remote procedure calls that can be invoked by
/// a Sandstorm client over the network. For example, 'MasterService' implements
/// a simple get() rpc that looks up a given key in the hash table.
///
/// The first field on the header of every rpc request identifies the service
/// that it should be dispatched to.
#[repr(u8)]
enum Service {
    // The most common of all services provided by a sandstorm server. This
    // service implements the primary interface to the database consisting of
    // operations such as get() and put().
    MasterService = 0x01,
}

/// This enum represents the different operations that can be invoked by a
/// client over a remote procedure call (RPC). Each operation is typically
/// provided by a service within a Sandstorm server. For example,
/// 'SandstormGetRpc' corresponds to a simple get() RPC provided by
/// 'MasterService'.
///
/// The second field on the header of every rpc request identifies the
/// operation it should perform within the Sandstorm server.
#[repr(u8)]
enum OpCode {
    // A simple operation that looks up the hash table for a given key.
    SandstormGetRpc = 0x01,
}

/// This type represents the header on a typical remote procedure call (RPC)
/// received at a Sandstorm server. In addition to identifying a service and
/// operation, the header also identifies the tenant that sent the request for
/// the purpose of security and accounting.
#[repr(C, packed)]
struct RpcRequestHeader {
    // The service within a server that the request must be dispatched to
    // (ex: MasterService).
    service: Service,

    // The opcode identifying the operation to perform within the server. This
    // operation must be provided by the above service.
    opcode: OpCode,

    // An identifier for the tenant that sent this RPC request.
    tenant: u32,
}

impl RpcRequestHeader {
    /// This function can be used to construct the header for an RPC request.
    ///
    /// \param rpc_service
    ///     The server side service that the RPC request will be dispatched to.
    /// \param rpc_opcode
    ///     The operation that the RPC must perform at the server. This
    ///     operation must be supported by rpc_service.
    /// \param rpc_tenant
    ///     An identifier for the tenant sending this request.
    ///
    /// \return
    ///     A header identifying the RPC. This header is of type
    ///     'RpcRequestHeader'.
    pub fn new(rpc_service: Service, rpc_opcode: OpCode,
               rpc_tenant: u32) -> RpcRequestHeader {
        RpcRequestHeader {
            service: rpc_service,
            opcode: rpc_opcode,
            tenant: rpc_tenant,
        }
    }
}

/// This type represents the header for a get() RPC request.
#[repr(C, packed)]
pub struct GetRequest {
    // The generic RPC header identifying the request as a get() RPC.
    rpc_header: RpcRequestHeader,

    // The identifier for the data table the key belongs to. Required
    // for the hash table lookup performed at the server.
    table_id: u64,

    // The length of the key being looked up. This field allows the key
    // to be unpacked from the request at the server.
    key_length: u16,
}

impl GetRequest {
    /// This method constructs an RPC header for the get() RPC.
    ///
    /// \param req_tenant
    ///     An identifier for the tenant sending the RPC.
    /// \param req_table_id
    ///     An identifier for the data table the key belongs to.
    /// \param req_key_length
    ///     The length of the key being looked up.
    ///
    /// \return
    ///     An RPC header for the get() request. The header is of type
    ///     'GetRequest'.
    pub fn new(req_tenant: u32, req_table_id: u64,
               req_key_length: u16) -> GetRequest {
        GetRequest {
            rpc_header: RpcRequestHeader::new(Service::MasterService,
                                              OpCode::SandstormGetRpc,
                                              req_tenant),
            table_id: req_table_id,
            key_length: req_key_length,
        }
    }
}

// Implementation of the 'EndOffset' trait for the GetRequest header.
// This is required for the client to add the header to a Netbricks
// packet. The methods on this trait are primarily used by Netbricks
// to track where the headers end and payload begins on a packet.
impl EndOffset for GetRequest {
    /// A GetRequest RPC header should always be preceeded by a transport
    /// layer UDP header.
    type PreviousHeader = UdpHeader;

    /// This method returns the position at which the GetRequest header
    /// ends relative to the beginning of the header (effectively the
    /// size of the header).
    ///
    /// \return
    ///     The offset of the payload relative to the GetRequest header.
    fn offset(&self) -> usize {
        size_of::<GetRequest>()
    }

    /// This method returns the size of the GetRequest RPC header.
    ///
    /// \return
    ///     The size of the GetRequest RPC header.
    fn size() -> usize {
        size_of::<GetRequest>()
    }

    /// This method returns the size of the payload on a packet with
    /// respect to the GetRequest header.
    ///
    /// \param hint
    ///     'hint' is typically the size of the payload as reported by
    ///     'PreviousHeader'. For example, if the UDP header reports that the
    ///     payload is 128 Bytes long, then GetRequest will report the payload
    ///     to be 128 minus sizeof::<GetRequest>() Bytes long.
    ///
    /// \return
    ///     The payload size relative to the GetRequest header.
    fn payload_size(&self, hint: usize) -> usize {
        hint - self.offset()
    }

    /// This method checks if the type on the previous header in the packet
    /// is correct.
    ///
    /// If this method is invoked, the compiler will compile the program only
    /// if the type on _prev is the same as 'PreviousHeader'.
    ///
    /// \param _prev
    ///     A reference to the previous header on the packet.
    ///
    /// \return
    ///     Always true.
    fn check_correct(&self, _prev: &PreviousHeader) -> bool {
        true
    }
}
