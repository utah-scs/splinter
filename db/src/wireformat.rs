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

use super::bytes::Bytes;
use super::table::Version;
use e2d2::headers::{EndOffset, UdpHeader};
use std::mem::size_of;

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
#[derive(PartialEq)]
pub enum Service {
    /// The most common of all services provided by a sandstorm server. This
    /// service implements the primary interface to the database consisting of
    /// operations such as get() and put().
    MasterService = 0x01,

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
#[derive(PartialEq)]
pub enum OpCode {
    /// A simple operation that looks up the hash table for a given key.
    SandstormGetRpc = 0x01,

    /// A simple operation that adds a key-value pair to the database.
    SandstormPutRpc = 0x02,

    /// This operation invokes a procedure inside the database at runtime.
    SandstormInvokeRpc = 0x03,

    /// This operation installs a procedure into the database at runtime.
    SandstormInstallRpc = 0x04,

    /// This operation fetches multiple records in a single round trip.
    SandstormMultiGetRpc = 0x05,

    /// This operation commits an invoke procedure run on the client-side.
    SandstormCommitRpc = 0x6,

    /// Any value beyond this represents an invalid rpc.
    InvalidOperation = 0x07,
}

/// This enum represents the status of a completed RPC. A status of 'StatusOk'
/// means that the RPC completed successfully, and that the payload on the
/// response can be safely read and interpreted.
#[repr(u8)]
#[derive(PartialEq, Clone)]
pub enum RpcStatus {
    /// The RPC completed successfully. The response can be safely unpacked
    /// at the client.
    StatusOk = 0x01,

    /// The RPC failed at the server because the tenant sending it could not
    /// be identified.
    StatusTenantDoesNotExist = 0x02,

    /// The RPC failed at the server because the table being looked up could
    /// not be found at the server.
    StatusTableDoesNotExist = 0x03,

    /// The RPC failed at the server because the object being looked up could
    /// not be found at the server.
    StatusObjectDoesNotExist = 0x04,

    /// The RPC failed at the server because the request was malformed. For
    /// example, the size of the payload on a get() request was lesser than
    /// the supplied key length.
    StatusMalformedRequest = 0x05,

    /// The RPC failed at the server because of an internal error (ex:
    /// insufficent memory etc).
    StatusInternalError = 0x06,

    /// The RPC failed at the server because the extension did not exist.
    StatusInvalidExtension = 0x07,

    /// The RPC failed at the server because it requested for an
    /// invalid/unsupported operation.
    StatusInvalidOperation = 0x08,

    /// The RPC was spending too much time on CPU, so the server pushed-back
    /// the extension without completing it.
    StatusPushback = 0x09,

    /// This indicates that the invocation fails at transaction commit time.
    StatusTxAbort = 0x10,
}

/// This enum represents the Generator value in the GetRequest header type.
/// Either the Sandstorm client can request for a get() operation, or after
/// pushback an extension can issue a get request. When an extension issues
/// the request, the server also need to add the key to the response payload.
#[repr(u8)]
#[derive(PartialEq, Clone)]
pub enum GetGenerator {
    /// The GetRequest was issued by a Sandstorm client.
    SandstormClient = 0x1,

    /// The GetRequest was issued by an extension on the client; which was pushed
    /// back to the client from the server.
    SandstormExtension = 0x2,

    /// This option means that the generator is unknown and decision based on this
    /// value can't be made.
    InvalidGenerator = 0x3,
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

    /// An identifier for the RPC request.
    pub stamp: u64,
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
    /// \param rpc_stamp
    ///     An identifier for the rpc request.
    ///
    /// \return
    ///     A header identifying the RPC. This header is of type
    ///     'RpcRequestHeader'.
    pub fn new(
        rpc_service: Service,
        rpc_opcode: OpCode,
        rpc_tenant: u32,
        rpc_stamp: u64,
    ) -> RpcRequestHeader {
        RpcRequestHeader {
            service: rpc_service,
            opcode: rpc_opcode,
            tenant: rpc_tenant,
            stamp: rpc_stamp,
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

    /// The opcode on the original RPC request.
    pub opcode: OpCode,

    /// The tenant this response is destined for.
    pub tenant: u32,

    /// Identifier of the RPC request this response is being generated for.
    pub stamp: u64,
}

impl RpcResponseHeader {
    /// This method returns a header of type RpcResponseHeader that can be
    /// added to an RPC response. The status on the header is set to StatusOk.
    ///
    /// - `req_stamp`:  RPC identifier.
    /// - `opcode`:     The opcode on the original RPC request.
    /// - `tenant`:     The tenant this response should be sent to.
    ///
    /// - `return`: A header of type RpcResponseHeader with the status field
    ///             set to RpcStatus::StatusOk.
    pub fn new(req_stamp: u64, opcode: OpCode, tenant: u32) -> RpcResponseHeader {
        RpcResponseHeader {
            status: RpcStatus::StatusOk,
            opcode: opcode,
            tenant: tenant,
            stamp: req_stamp,
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

    /// This enum determines the issuer for the GetRequest, which can either be a
    /// Sandstorm client or an extension running on the client side.
    pub generator: GetGenerator,
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
    /// \param req_stamp
    ///     RPC identifier.
    ///
    /// \return
    ///     An RPC header for the get() request. The header is of type
    ///     'GetRequest'.
    pub fn new(
        req_tenant: u32,
        req_table_id: u64,
        req_key_length: u16,
        req_stamp: u64,
        req_generator: GetGenerator,
    ) -> GetRequest {
        GetRequest {
            common_header: RpcRequestHeader::new(
                Service::MasterService,
                OpCode::SandstormGetRpc,
                req_tenant,
                req_stamp,
            ),
            table_id: req_table_id,
            key_length: req_key_length,
            generator: req_generator,
        }
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
    /// - `req_stamp`: RPC identifier.
    /// - `opcode`:    The opcode on the original RPC request.
    /// - `tenant`:    The tenant this response should be sent to.
    ///
    /// - `return`: A header of type GetResponse that can be added to an RPC
    ///             response.
    pub fn new(req_stamp: u64, opcode: OpCode, tenant: u32) -> GetResponse {
        GetResponse {
            common_header: RpcResponseHeader::new(req_stamp, opcode, tenant),
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
    /// * `req_stamp`:   RPC identifier.
    ///
    /// # Return
    ///
    /// An RPC header that can be appended to a put() request.
    pub fn new(req_tenant: u32, req_table: u64, req_key_len: u16, req_stamp: u64) -> PutRequest {
        let common = RpcRequestHeader::new(
            Service::MasterService,
            OpCode::SandstormPutRpc,
            req_tenant,
            req_stamp,
        );

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
    ///
    /// # Arguments
    ///
    /// * `req_stamp`: RPC identifier.
    /// * `opcode`:    The opcode on the original RPC request.
    /// * `tenant`:    The tenant this response should be sent to.
    pub fn new(req_stamp: u64, opcode: OpCode, tenant: u32) -> PutResponse {
        PutResponse {
            common_header: RpcResponseHeader::new(req_stamp, opcode, tenant),
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
    /// * `req_stamp`:   RPC identifier.
    ///
    /// # Return
    ///
    /// An RPC request header of type `InvokeRequest`.
    pub fn new(tenant: u32, name_length: u32, args_length: u32, req_stamp: u64) -> InvokeRequest {
        InvokeRequest {
            common_header: RpcRequestHeader::new(
                Service::MasterService,
                OpCode::SandstormInvokeRpc,
                tenant,
                req_stamp,
            ),
            name_length: name_length,
            args_length: args_length,
        }
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
    ///
    /// # Arguments
    ///
    /// * `req_stamp`: RPC identifier.
    /// * `opcode`:    The opcode on the original RPC request.
    /// * `tenant`:    The tenant this response should be sent to.
    pub fn new(req_stamp: u64, opcode: OpCode, tenant: u32) -> InvokeResponse {
        InvokeResponse {
            common_header: RpcResponseHeader::new(req_stamp, opcode, tenant),
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

/// This type represents the header for an install() RPC request.
#[repr(C, packed)]
pub struct InstallRequest {
    /// Generic RPC header identifying the service, opcode, and tenant.
    pub common_header: RpcRequestHeader,

    /// Length of the name in bytes of the extension being installed. The payload of the RPC should
    /// start with the name of the extension.
    pub name_length: u32,

    /// Length of the extension in bytes. The extension should follow the name on the RPC's
    /// payload.
    pub extn_length: u32,
}

// Implementation of methods on InstallRequest.
impl InstallRequest {
    /// Returns a header for the install() RPC request. The header is of type `InstallRequest`.
    ///
    /// # Arguments
    ///
    /// * `tenant`:      Tenant identifier.
    /// * `name_length`: Length of the name of the extension in bytes. The payload of the RPC
    ///                  should start with the name of the extension.
    /// * `extn_length`: Length of the extension in bytes. The extension should follow the name on
    ///                  the RPC's payload.
    /// * `req_stamp`:   RPC identifier.
    pub fn new(tenant: u32, name_length: u32, extn_length: u32, req_stamp: u64) -> InstallRequest {
        InstallRequest {
            common_header: RpcRequestHeader::new(
                Service::MasterService,
                OpCode::SandstormInstallRpc,
                tenant,
                req_stamp,
            ),
            name_length: name_length,
            extn_length: extn_length,
        }
    }
}

// Implementation of the EndOffset trait for InstallRequest. Refer to
// GetRequest's implementation of this trait to understand what the methods
// and types mean.
impl EndOffset for InstallRequest {
    type PreviousHeader = UdpHeader;

    fn offset(&self) -> usize {
        size_of::<InstallRequest>()
    }

    fn size() -> usize {
        size_of::<InstallRequest>()
    }

    fn payload_size(&self, hint: usize) -> usize {
        hint - self.offset()
    }

    fn check_correct(&self, _prev: &Self::PreviousHeader) -> bool {
        true
    }
}

/// This type represents the response header for an install() RPC request.
#[repr(C, packed)]
pub struct InstallResponse {
    /// A generic response header with the status of the RPC (indicating whether it
    /// succeeded or failed).
    pub common_header: RpcResponseHeader,
}

// Implementation of methods on InstallResponse.
impl InstallResponse {
    /// Returns a header for the install() RPC request.
    ///
    /// # Arguments
    ///
    /// * `req_stamp`: RPC identifier.
    /// * `opcode`:    The opcode on the original RPC request.
    /// * `tenant`:    The tenant this response should be sent to.
    pub fn new(req_stamp: u64, opcode: OpCode, tenant: u32) -> InstallResponse {
        InstallResponse {
            common_header: RpcResponseHeader::new(req_stamp, opcode, tenant),
        }
    }
}

// Implementation of the EndOffset trait for InstallResponse. Refer to
// GetRequest's implementation of this trait to understand what the methods
// and types mean.
impl EndOffset for InstallResponse {
    type PreviousHeader = UdpHeader;

    fn offset(&self) -> usize {
        size_of::<InstallResponse>()
    }

    fn size() -> usize {
        size_of::<InstallResponse>()
    }

    fn payload_size(&self, hint: usize) -> usize {
        hint - self.offset()
    }

    fn check_correct(&self, _prev: &Self::PreviousHeader) -> bool {
        true
    }
}

/// This type represents the RPC header on a multiget() request.
#[repr(C, packed)]
pub struct MultiGetRequest {
    /// Generic RPC header consisting of service, opcode, and tenant id.
    pub common_header: RpcRequestHeader,

    /// Table that should be looked up for the records.
    pub table_id: u64,

    /// The length of every key to be looked up. All keys to be looked up are assumed to be of
    /// equal length.
    pub key_len: u16,

    /// The number of keys to be looked up at the database. Every key should be `key_len` bytes
    /// long.
    pub num_keys: u32,
}

// Implementation of methods on MultiGetRequest.
impl MultiGetRequest {
    /// Constructs an RPC header that can be added to the multiget() request. The header is of type
    /// `MultiGetRequest`.
    ///
    /// # Arguments
    ///
    /// * `tenant`: Identifier of the tenant sending the request.
    /// * `table`:  Identifier of the table to be looked up.
    /// * `k_len`:  Length of every key to be looked up. All keys are assumed to be of equal
    ///             length.
    /// * `n_keys`: The number of keys to be looked up (each of length `k_len`).
    /// * `stamp`:  Identifier of the RPC. Can be used as a timestamp.
    pub fn new(tenant: u32, table: u64, k_len: u16, n_keys: u32, stamp: u64) -> MultiGetRequest {
        MultiGetRequest {
            common_header: RpcRequestHeader::new(
                Service::MasterService,
                OpCode::SandstormMultiGetRpc,
                tenant,
                stamp,
            ),
            table_id: table,
            key_len: k_len,
            num_keys: n_keys,
        }
    }
}

// Implementation of the EndOffset trait for MultiGetRequest. Refer to
// GetRequest's implementation of this trait to understand what the methods
// and types mean.
impl EndOffset for MultiGetRequest {
    type PreviousHeader = UdpHeader;

    fn offset(&self) -> usize {
        size_of::<MultiGetRequest>()
    }

    fn size() -> usize {
        size_of::<MultiGetRequest>()
    }

    fn payload_size(&self, hint: usize) -> usize {
        hint - self.offset()
    }

    fn check_correct(&self, _prev: &Self::PreviousHeader) -> bool {
        true
    }
}

/// This type represents the response header for a multiget() RPC request.
#[repr(C, packed)]
pub struct MultiGetResponse {
    /// Generic response header consisting of RPC status and identifier.
    pub common_header: RpcResponseHeader,

    /// Number of records returned by the RPC.
    pub num_records: u32,
}

// Implementation of methods on MultiGetResponse.
impl MultiGetResponse {
    /// Constructs a response header for the multiget() RPC. The header is of type
    /// `MultiGetResponse`.
    ///
    /// # Arguments
    ///
    /// * `stamp`:     RPC identifier. Can be used to timestamp the RPC.
    /// * `opcode`:    The opcode on the original RPC request.
    /// * `tenant`:    The tenant this response should be sent to.
    /// * `n_records`: Number of records being returned in the response.
    pub fn new(stamp: u64, opcode: OpCode, tenant: u32, n_records: u32) -> MultiGetResponse {
        MultiGetResponse {
            common_header: RpcResponseHeader::new(stamp, opcode, tenant),
            num_records: n_records,
        }
    }
}

// Implementation of the EndOffset trait for MultiGetResponse. Refer to
// GetRequest's implementation of this trait to understand what the methods
// and types mean.
impl EndOffset for MultiGetResponse {
    type PreviousHeader = UdpHeader;

    fn offset(&self) -> usize {
        size_of::<MultiGetResponse>()
    }

    fn size() -> usize {
        size_of::<MultiGetResponse>()
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
pub struct CommitRequest {
    /// The common RPC header identifying the opcode, service, and tenant.
    pub common_header: RpcRequestHeader,

    /// An identifier for the table to commit the transaction to.
    pub table_id: u64,

    /// The length of the key; needed to parse the payload.
    pub key_length: u16,

    /// The length of the value; needed to parse the payload.
    pub value_length: u16,
}

impl CommitRequest {
    /// This method returns a header corresponding to an invoke() RPC request.
    /// The returned header can be appended onto a request packet.
    ///
    /// # Arguments
    ///
    /// * `tenant`:      An identifier for the tenant issuing this RPC.
    /// * `name_length`: The length of the name of the procedure to be invoked.
    ///                  Required so that the name can be read from the request
    ///                  packet by the server.
    /// * `table_id`: An identifier for the table to commit the transaction to.
    /// * `req_stamp`:   RPC identifier.
    ///
    /// # Return
    ///
    /// An RPC request header of type `CommitRequest`.
    pub fn new(
        tenant: u32,
        req_stamp: u64,
        table_id: u64,
        key_len: u16,
        val_len: u16,
    ) -> CommitRequest {
        CommitRequest {
            common_header: RpcRequestHeader::new(
                Service::MasterService,
                OpCode::SandstormCommitRpc,
                tenant,
                req_stamp,
            ),
            table_id: table_id,
            key_length: key_len,
            value_length: val_len,
        }
    }
}

// Implementation of the EndOffset trait for CommitRequest. Refer to
// GetRequest's implementation of this trait to understand what the methods
// and types mean.
impl EndOffset for CommitRequest {
    type PreviousHeader = UdpHeader;

    fn offset(&self) -> usize {
        size_of::<CommitRequest>()
    }

    fn size() -> usize {
        size_of::<CommitRequest>()
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
pub struct CommitResponse {
    /// A common RPC response header containing the status of the RPC.
    pub common_header: RpcResponseHeader,
}

impl CommitResponse {
    /// This method returns a header that can be appended to the response
    /// packet for an invoke() RPC request.
    ///
    /// # Arguments
    ///
    /// * `req_stamp`: RPC identifier.
    /// * `opcode`:    The opcode on the original RPC request.
    /// * `tenant`:    The tenant this response should be sent to.
    pub fn new(req_stamp: u64, opcode: OpCode, tenant: u32) -> CommitResponse {
        CommitResponse {
            common_header: RpcResponseHeader::new(req_stamp, opcode, tenant),
        }
    }
}

// Implementation of the EndOffset trait for CommitResponse. Refer to
// GetRequest's implementation of this trait to understand what the methods
// and types mean.
impl EndOffset for CommitResponse {
    type PreviousHeader = UdpHeader;

    fn offset(&self) -> usize {
        size_of::<CommitResponse>()
    }

    fn size() -> usize {
        size_of::<CommitResponse>()
    }

    fn payload_size(&self, hint: usize) -> usize {
        hint - self.offset()
    }

    fn check_correct(&self, _prev: &Self::PreviousHeader) -> bool {
        true
    }
}

/// This enum represents the type of a completed database operation. A value 'SandstormRead'
/// means that the operation was a get() operation  and a value 'SandstormWrite' means that the
/// operation was a put() operation. The value is used in the response to represent if the record
/// belongs to read set or the write set.
#[repr(u8)]
#[derive(PartialEq, Clone, Debug)]
pub enum OpType {
    /// When the value if SandstormRead, the record encapsulated in the response will be added to
    /// the read set corresponding to that extension.
    SandstormRead = 0x1,

    /// When the value if SandstormWrite, the record encapsulated in the response will be added to
    /// the write set corresponding to that extension.
    SandstormWrite = 0x2,

    /// Any value beyond this represents an invalid record.
    InvalidRecord = 0x3,
}

/// This struct represents a record for a read/write set. Each record in the read/write set will
/// be of this type.
pub struct Record {
    /// This variable shows the type of operation for the record, Read or Write.
    optype: OpType,

    /// The version number for the record.
    version: Version,

    /// This variable stores the Key for the record.
    key: Bytes,

    /// This variable stores the Value for the record.
    object: Bytes,
}

impl Record {
    /// This method returns a new record which can be transferred to the client as a read/write set
    /// which server decides to pushback the extension to the client.
    ///
    /// # Arguments
    /// * `r_optype`: The type of the record, either a Read or a Write.
    /// * `r_key`: The key for the record.
    /// * `r_object`: The value for the record.
    ///
    /// # Return
    /// A read-write set record with the operation type, a key and a value.
    pub fn new(r_optype: OpType, version: Version, r_key: Bytes, r_object: Bytes) -> Record {
        Record {
            optype: r_optype,
            version: version,
            key: r_key,
            object: r_object,
        }
    }

    /// Return the optype(Read/Write) for the operation.
    pub fn get_optype(&self) -> OpType {
        self.optype.clone()
    }

    /// Return the version for the performed operation.
    pub fn get_version(&self) -> Version {
        self.version.clone()
    }

    /// Return the key for the performed operation.
    pub fn get_key(&self) -> Bytes {
        self.key.clone()
    }

    /// Return the object for the performed operation.
    pub fn get_object(&self) -> Bytes {
        self.object.clone()
    }
}
