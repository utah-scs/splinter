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

use e2d2::headers::{EndOffset, UdpHeader, IpHeader, MacHeader};
use e2d2::interface::*;
use e2d2::common::EmptyMetadata;

pub type UdpPacket = Packet<UdpHeader, EmptyMetadata>;
pub type IpPacket = Packet<IpHeader, EmptyMetadata>;
pub type MacPacket = Packet<MacHeader, EmptyMetadata>;

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
pub enum Service {
    /// The most common of all services provided by a sandstorm server. This
    /// service implements the primary interface to the database consisting of
    /// operations such as get() and put().
    MasterService  = 0x01,

    /// Any value beyond this represents an invalid service.
    InvalidService = 0x02,
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
pub enum OpCode {
    /// A simple operation that looks up the hash table for a given key.
    SandstormGetRpc    = 0x01,

    /// A simple operation that adds a key-value pair to the database.
    SandstormPutRpc    = 0x02,

    /// This operation invokes a procedure inside the database at runtime.
    SandstormInvokeRpc = 0x03,

    /// Any value beyond this represents an invalid rpc.
    InvalidOperation   = 0x04,
}

/// This enum represents the status of a completed RPC. A status of 'StatusOk'
/// means that the RPC completed successfully, and that the payload on the
/// response can be safely read and interpreted.
#[repr(u8)]
pub enum RpcStatus {
    /// The RPC completed successfully. The response can be safely unpacked
    /// at the client.
    StatusOk                 = 0x01,

    /// The RPC failed at the server because the tenant sending it could not
    /// be identified.
    StatusTenantDoesNotExist = 0x02,

    /// The RPC failed at the server because the table being looked up could
    /// not be found at the server.
    StatusTableDoesNotExist  = 0x03,

    /// The RPC failed at the server because the object being looked up could
    /// not be found at the server.
    StatusObjectDoesNotExist = 0x04,

    /// The RPC failed at the server because the request was malformed. For
    /// example, the size of the payload on a get() request was lesser than
    /// the supplied key length.
    StatusMalformedRequest   = 0x05,

    /// The RPC failed at the server because of an internal error (ex:
    /// insufficent memory etc).
    StatusInternalError      = 0x06,

    /// The RPC failed at the server because it requested for an
    /// invalid/unsupported operation.
    StatusInvalidOperation   = 0x07,
}

/// This type represents the request header on a typical remote procedure call
/// (RPC) received at a Sandstorm server. In addition to identifying a service
/// and operation, the header also identifies the tenant that sent the request
/// for the purpose of security and accounting.
///
/// Every RPC request should consist of a member of this type.
///
/// This type cannot be parsed or deparsed like the UdpHeader for example.
/// This is intentional, and makes it easier to construct RPC requests because
/// there is only one unique type (like GetRequest) identifying the request.
#[repr(C, packed)]
pub struct RpcRequestHeader {
    /// The service within a server that the request must be dispatched to
    /// (ex: MasterService).
    pub service: Service,

    /// The opcode identifying the operation to perform within the server. This
    /// operation must be provided by the above service.
    pub opcode: OpCode,

    /// An identifier for the tenant that sent this RPC request.
    pub tenant: u32,
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

/// This type represents the header on a typical RPC response received by a
/// client. This header indicates as to whether the RPC succeeded or failed
/// at the server.
///
/// Every RPC response should consist of a member of this type.
///
/// This type cannot be parsed or deparsed like the UdpHeader for example.
/// This is intentional, and makes it easier to construct RPC responses because
/// there is only one unique type (like GetResponse) identifying the response.
#[repr(C, packed)]
pub struct RpcResponseHeader {
    /// The status of the RPC indicating whether it completed successfully.
    pub status: RpcStatus,
}

impl RpcResponseHeader {
    /// This method returns a header of type RpcResponseHeader that can be
    /// added to an RPC response. The status on the header is set to StatusOk.
    ///
    /// - `return`: A header of type RpcResponseHeader with the status field
    ///             set to RpcStatus::StatusOk.
    pub fn new() -> RpcResponseHeader {
        RpcResponseHeader {
            status: RpcStatus::StatusOk,
        }
    }
}

/// This type represents the header for a get() RPC request.
#[repr(C, packed)]
pub struct GetRequest {
    /// The generic RPC header identifying the request as a get() RPC.
    pub common_header: RpcRequestHeader,

    /// The identifier for the data table the key belongs to. Required
    /// for the hash table lookup performed at the server.
    pub table_id: u64,

    /// The length of the key being looked up. This field allows the key
    /// to be unpacked from the request at the server.
    pub key_length: u16,
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
            common_header: RpcRequestHeader::new(Service::MasterService,
                                                 OpCode::SandstormGetRpc,
                                                 req_tenant),
            table_id: req_table_id,
            key_length: req_key_length,
        }
    }

    /// Populate a packet with an RPC header that requests a server
    /// "get" operation.
    /// May panic if there is a problem allocating constructing headers
    /// or if `key` is too big.
    ///
    /// # Arguments
    ///  * `request`: Packet into which this RPC request is populated.
    ///               Must already ready contain proper network headers.
    ///  * `tenant`: Id of the tenant requesting the item.
    ///  * `table_id`: Id of the table from which the key is looked up.
    ///  * `key`: Byte string of key whose value is to be fetched. Limit 64 KB.
    /// # Return
    ///  Packet populated with the request parameters.
    pub fn construct(request: UdpPacket,
                     tenant: u32,
                     table_id: u64,
                     key: &[u8])
        -> IpPacket
    {
        if key.len() > u16::max_value() as usize {
            // TODO(stutsman) This function should return Result instead of panic.
            panic!("Key too long ({} bytes).", key.len());
        }

        let mut request =
            request.push_header(
                    &GetRequest {
                        common_header: RpcRequestHeader::new(Service::MasterService,
                                                             OpCode::SandstormGetRpc,
                                                             tenant),
                        table_id: table_id,
                        key_length: key.len() as u16,
                    })
            .expect("Failed to push RPC header into request!");

        request.add_to_payload_tail(key.len(), &key)
            .expect("Failed to write key into get() request!");

        fixup_header_length_fields(request.deparse_header(size_of::<UdpHeader>()))
    }
}

// Implementation of the 'EndOffset' trait for the GetRequest header.
// This is required for the client to add the header to a Netbricks
// packet. The methods on this trait are primarily used by Netbricks
// to track where the headers end and payload begins on a packet.
impl EndOffset for GetRequest {
    // A GetRequest RPC header should always be preceeded by a transport
    // layer UDP header.
    type PreviousHeader = UdpHeader;

    // This method returns the position at which the GetRequest header
    // ends relative to the beginning of the header (effectively the
    // size of the header).
    //
    // \return
    //     The offset of the payload relative to the GetRequest header.
    fn offset(&self) -> usize {
        size_of::<GetRequest>()
    }

    // This method returns the size of the GetRequest RPC header.
    //
    // \return
    //     The size of the GetRequest RPC header.
    fn size() -> usize {
        size_of::<GetRequest>()
    }

    // This method returns the size of the payload on a packet with
    // respect to the GetRequest header.
    //
    // \param hint
    //     'hint' is typically the size of the payload as reported by
    //     'PreviousHeader'. For example, if the UDP header reports that the
    //     payload is 128 Bytes long, then GetRequest will report the payload
    //     to be 128 minus sizeof::<GetRequest>() Bytes long.
    //
    // \return
    //     The payload size relative to the GetRequest header.
    fn payload_size(&self, hint: usize) -> usize {
        hint - self.offset()
    }

    // This method checks if the type on the previous header in the packet
    // is correct.
    //
    // If this method is invoked, the compiler will compile the program only
    // if the type on _prev is the same as 'PreviousHeader'.
    //
    // \param _prev
    //     A reference to the previous header on the packet.
    //
    // \return
    //     Always true.
    fn check_correct(&self, _prev: &Self::PreviousHeader) -> bool {
        true
    }
}

/// This type represents the header on a response to a get() RPC request.
#[repr(C, packed)]
pub struct GetResponse {
    /// The common RPC header required to determine if the RPC completed
    /// successfully.
    pub common_header: RpcResponseHeader,

    /// The length of the value returned in the response if the RPC completed
    /// successfully.
    pub value_length: u32,
}

impl GetResponse {
    /// This method returns a header that can be added to the response to a
    /// get() RPC request. The value_length field is set to zero.
    ///
    /// - `return`: A header of type GetResponse that can be added to an RPC
    ///             response.
    pub fn new() -> GetResponse {
        GetResponse {
            common_header: RpcResponseHeader::new(),
            value_length: 0,
        }
    }
}

// Implementation of the EndOffset trait for GetResponse. Refer to GetRequest's
// implementation of this trait to understand what the methods and types mean.
impl EndOffset for GetResponse {
    type PreviousHeader = UdpHeader;

    fn offset(&self) -> usize {
        size_of::<GetResponse>()
    }

    fn size() -> usize {
        size_of::<GetResponse>()
    }

    fn payload_size(&self, hint: usize) -> usize {
        hint - self.offset()
    }

    fn check_correct(&self, _prev: &Self::PreviousHeader) -> bool {
        true
    }
}

/// This type represents the header on a put() RPC request.
#[repr(C, packed)]
pub struct PutRequest {
    /// A generic RPC header identifying the tenant, service, and opcode of the
    /// request.
    pub common_header: RpcRequestHeader,

    /// The data table to add the key-value pair to.
    pub table_id: u64,

    /// The length of the key within the RPC's payload.
    pub key_length: u16,
}

// Implementation of methods on PutRequest.
impl PutRequest {
    /// This method returns an RPC header that can be added to a put() request.
    ///
    /// # Arguments
    ///
    /// * `req_tenant`:  An identifier for the tenant issuing the request.
    /// * `req_table`:   An identifier for the table to add the key-value pair
    ///                  to.
    /// * `req_key_len`: The length of the key inside the RPC request's payload.
    ///
    /// # Return
    ///
    /// An RPC header that can be appended to a put() request.
    pub fn new(req_tenant: u32, req_table: u64, req_key_len: u16)
               -> PutRequest
    {
        let common = RpcRequestHeader::new(Service::MasterService,
                                           OpCode::SandstormPutRpc,
                                           req_tenant);

        PutRequest {
            common_header: common,
            table_id: req_table,
            key_length: req_key_len,
        }
    }
}

// Implementation of the EndOffset trait for PutRequest. Refer to GetRequest's
// implementation of this trait to understand what the methods and types mean.
impl EndOffset for PutRequest {
    type PreviousHeader = UdpHeader;

    fn offset(&self) -> usize {
        size_of::<PutRequest>()
    }

    fn size() -> usize {
        size_of::<PutRequest>()
    }

    fn payload_size(&self, hint: usize) -> usize {
        hint - self.offset()
    }

    fn check_correct(&self, _prev: &Self::PreviousHeader) -> bool {
        true
    }
}

/// This type represents the header on a response to a put() RPC request.
#[repr(C, packed)]
pub struct PutResponse {
    /// A generic RPC header indicating whether the RPC request succeeded
    /// or failed.
    pub common_header: RpcResponseHeader,
}

// Implementation of methods on PutResponse.
impl PutResponse {
    /// This method returns a header that can be appended to the response
    /// to a put() RPC request.
    pub fn new() -> PutResponse {
        PutResponse {
            common_header: RpcResponseHeader::new(),
        }
    }
}

// Implementation of the EndOffset trait for PutResponse. Refer to GetRequest's
// implementation of this trait to understand what the methods and types mean.
impl EndOffset for PutResponse {
    type PreviousHeader = UdpHeader;

    fn offset(&self) -> usize {
        size_of::<PutResponse>()
    }

    fn size() -> usize {
        size_of::<PutResponse>()
    }

    fn payload_size(&self, hint: usize) -> usize {
        hint - self.offset()
    }

    fn check_correct(&self, _prev: &Self::PreviousHeader) -> bool {
        true
    }
}

/// This type represents the request header corresponding to an invoke() RPC.
#[repr(C, packed)]
pub struct InvokeRequest {
    /// The common RPC header identifying the opcode, service, and tenant.
    pub common_header: RpcRequestHeader,

    /// The length of the name of the procedure to be invoked. Required to
    /// deserialize the procedure's name from the request packet at the server.
    pub name_length: u32,

    /// The total length of the args to be passed into the procedure. Required
    /// to deserialize the arguments to the procedure from the request packet
    /// at the server.
    pub args_length: u32,
}

impl InvokeRequest {
    /// This method returns a header corresponding to an invoke() RPC request.
    /// The returned header can be appended onto a request packet.
    ///
    /// # Arguments
    ///
    /// * `tenant`:      An identifier for the tenant issuing this RPC.
    /// * `name_length`: The length of the name of the procedure to be invoked.
    ///                  Required so that the name can be read from the request
    ///                  packet by the server.
    /// * `args_length`: The length of the args to be supplied to the procedure.
    ///                  Required so that the server can unpack them from a
    ///                  request packet.
    ///
    /// # Return
    ///
    /// An RPC request header of type `InvokeRequest`.
    pub fn new(tenant: u32, name_length: u32, args_length: u32)
               -> InvokeRequest {
        InvokeRequest {
            common_header: RpcRequestHeader::new(Service::MasterService,
                                                 OpCode::SandstormInvokeRpc,
                                                 tenant),
            name_length: name_length,
            args_length: args_length,
        }
    }

    /// Populate a packet with an RPC header that requests a server
    /// "get" operation.
    /// May panic if there is a problem constructing headers or if
    /// `name` or `args` are too big.
    ///
    /// # Arguments
    ///  * `request`: Packet into which this RPC request is populated.
    ///               Must already ready contain proper network headers.
    ///  * `tenant`: Id of the tenant requesting the item.
    ///  * `name`: Name of the tenant's procedure to invoke. Limit 4 GB.
    ///  * `args`: Arguments to the invoked procedure. Limit 4 GB.
    /// # Return
    ///  Packet populated with the request parameters.
    pub fn construct(request: UdpPacket,
                     tenant: u32,
                     name: &[u8],
                     args: &[u8])
        -> IpPacket
    {
        if name.len() > u32::max_value() as usize {
            // TODO(stutsman) This function should return Result instead of panic.
            panic!("Name too long ({} bytes).", name.len());
        }

        if args.len() > u32::max_value() as usize {
            // TODO(stutsman) This function should return Result instead of panic.
            panic!("Args too long ({} bytes).", args.len());
        }

        let mut request =
            request.push_header(
                    &InvokeRequest {
                        common_header: RpcRequestHeader::new(Service::MasterService,
                                                             OpCode::SandstormGetRpc,
                                                             tenant),
                        name_length: name.len() as u32,
                        args_length: args.len() as u32,
                    })
            .expect("Failed to push RPC header into request!");

        request.add_to_payload_tail(name.len(), &name)
            .expect("Failed to write name into invoke() request!");
        request.add_to_payload_tail(args.len(), &args)
            .expect("Failed to write args into invoke() request!");

        fixup_header_length_fields(request.deparse_header(size_of::<UdpHeader>()))
    }
}

// Implementation of the EndOffset trait for InvokeRequest. Refer to
// GetRequest's implementation of this trait to understand what the methods
// and types mean.
impl EndOffset for InvokeRequest {
    type PreviousHeader = UdpHeader;

    fn offset(&self) -> usize {
        size_of::<InvokeRequest>()
    }

    fn size() -> usize {
        size_of::<InvokeRequest>()
    }

    fn payload_size(&self, hint: usize) -> usize {
        hint - self.offset()
    }

    fn check_correct(&self, _prev: &Self::PreviousHeader) -> bool {
        true
    }
}

/// This type represents the response header for an invoke() RPC request.
#[repr(C, packed)]
pub struct InvokeResponse {
    /// A common RPC response header containing the status of the RPC.
    pub common_header: RpcResponseHeader,
}

impl InvokeResponse {
    /// This method returns a header that can be appended to the response
    /// packet for an invoke() RPC request.
    pub fn new() -> InvokeResponse {
        InvokeResponse {
            common_header: RpcResponseHeader::new(),
        }
    }
}

// Implementation of the EndOffset trait for InvokeResponse. Refer to
// GetRequest's implementation of this trait to understand what the methods
// and types mean.
impl EndOffset for InvokeResponse {
    type PreviousHeader = UdpHeader;

    fn offset(&self) -> usize {
        size_of::<InvokeResponse>()
    }

    fn size() -> usize {
        size_of::<InvokeResponse>()
    }

    fn payload_size(&self, hint: usize) -> usize {
        hint - self.offset()
    }

    fn check_correct(&self, _prev: &Self::PreviousHeader) -> bool {
        true
    }
}

/// Compute and populate UDP and IP header length fields for `request`.
/// This should be called at the tail of every `construct()` call,
/// otherwise headers may indicate incorrect payload sizes.
fn fixup_header_length_fields(mut request: UdpPacket) -> IpPacket
{
    let udp_len = (size_of::<UdpHeader>() + request.get_payload().len()) as u16;
    request.get_mut_header().set_length(udp_len);

    let mut request = request.deparse_header(size_of::<IpHeader>());
    request.get_mut_header().set_length(size_of::<IpHeader>() as u16 + udp_len);

    request
}

