extern crate e2d2;

use std::mem::size_of;

use e2d2::headers::{ EndOffset, UdpHeader };

/// This enum represents the different sets of services that a Sandstorm server
/// can provide, and helps identify the service an incoming remote procedure
/// call (RPC) must be dispatched to.
///
/// Each service defines a set of remote procedure calls that can be invoked by
/// a Sandstorm client over the network. For example, MasterService implements a
/// simple get() rpc that looks up a given key in the hash table.
#[repr(u8)]
enum Service {
    // The most common of all services provided by a sandstorm server. This
    // service implements the primary interface to the database consisting of
    // operations such as get() and put().
    MasterService = 0x01,
}

#[repr(u8)]
enum OpCode {
    SandstormGetRpc = 0x01,
}

#[repr(C, packed)]
struct RpcRequestHeader {
    service: Service,
    opcode: OpCode,
    tenant: u32,
}

impl RpcRequestHeader {
    pub fn new(rpc_service: Service, rpc_opcode: OpCode,
               rpc_tenant: u32) -> RpcRequestHeader {
        RpcRequestHeader {
            service: rpc_service,
            opcode: rpc_opcode,
            tenant: rpc_tenant,
        }
    }
}

#[repr(C, packed)]
pub struct GetRequest {
    rpc_header: RpcRequestHeader,
    table_id: u64,
    key_length: u16,
}

impl GetRequest {
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

impl EndOffset for GetRequest {
    type PreviousHeader = UdpHeader;

    fn offset(&self) -> usize {
        size_of::<GetRequest>()
    }

    fn size() -> usize {
        size_of::<GetRequest>()
    }

    fn payload_size(&self, hint: usize) -> usize {
        hint - self.offset()
    }

    fn check_correct(&self, _prev: &UdpHeader) -> bool {
        true
    }
}
