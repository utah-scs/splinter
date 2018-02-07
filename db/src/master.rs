extern crate sandstorm;

use std::collections::HashMap;

use super::ext::*;
use super::table::*;
use super::service::{ Service };
use super::rpc::{ parse_rpc_opcode };
use super::common::{ UserId, TableId, PACKET_UDP_LEN };
use super::wireformat::{ OpCode, RpcStatus, GetRequest, GetResponse };

use e2d2::interface::{ Packet };
use e2d2::headers::{ UdpHeader };
use e2d2::common::{ EmptyMetadata };
use bytes::{ Bytes, BytesMut, BufMut };

struct User {
    // TODO(stutsman) Need some form of interior mutability here.
    id: UserId,
    tables: HashMap<TableId, Table>,
}

impl User {
    fn new(id: UserId) -> User {
        User{
            id: id,
            tables: HashMap::new(),
        }
    }

    fn create_table(&mut self, table_id: u64) {
        // Create one hash table for the user and populate it one
        // key-value pair.
        let table = Table::default();

        // Write the key-value pair to a single contiguous buffer.
        let mut value = BytesMut::with_capacity(130);
        value.put_slice(&[1; 30]);
        value.put_slice(&[91; 100]);
        let mut value: Bytes = value.freeze();

        // Populate the table with this key-value pair.
        let key: Bytes = value.split_to(30);
        table.put(key, value);
        self.tables.insert(table_id, table);
    }
}

pub struct Master {
    // TODO(stutsman) Need some form of interior mutability here.
    users: HashMap<UserId, User>,
    extensions: ExtensionManager,
}

impl Master {
    pub fn new() -> Master {
        let mut user = User::new(1);
        user.create_table(1);

        let mut master = Master{
            users: HashMap::new(),
            extensions: ExtensionManager::new(),
        };

        master.users.insert(user.id, user);

        master
    }

    /// This method handles the Get() RPC request. A hash table lookup is
    /// performed on a supplied tenant id, table id, and key. If successfull,
    /// the result of the lookup is written into a response packet, and the
    /// response header is updated. In the case of a failure, the response
    /// header is updated with a status indicating the reason for the failure.
    ///
    /// - `req_hdr`: A reference to the request header of the RPC.
    /// - `request`: A reference to the entire request packet.
    /// - `respons`: A mutable reference to the entire response packet.
    fn get(&self, req_hdr: &GetRequest,
           request: &Packet<GetRequest, EmptyMetadata>,
           respons: &mut Packet<GetResponse, EmptyMetadata>) {
        // Read fields of the request header.
        let tenant_id: UserId = req_hdr.common_header.tenant as UserId;
        let table_id: TableId = req_hdr.table_id as TableId;
        let key_length: u16 = req_hdr.key_length;

        // Get a reference to the key.
        let key: &[u8] = &request.get_payload()[0..key_length as usize];

        let mut status: RpcStatus = RpcStatus::StatusOk;

        let outcome =
                // Check if the tenant exists.
            self.users.get(&tenant_id)
                // If the tenant exists, check if it has a table with the
                // given id. If it does not exist, update the status to
                // reflect that.
                .map_or_else(|| {
                                status = RpcStatus::StatusTenantDoesNotExist;
                                None
                             }, | user | { user.tables.get(&table_id) })
                // If the table exists, lookup the provided key. If it does
                // not exist, update the status to reflect that.
                .map_or_else(|| {
                                status = RpcStatus::StatusTableDoesNotExist;
                                None
                             }, | table | { table.get(key) })
                // If the lookup succeeded, write the value to the
                // response payload. If it didn't, update the status to reflect
                // that.
                .map_or_else(|| {
                                status = RpcStatus::StatusObjectDoesNotExist;
                                None
                             }, | value | {
                                 respons.add_to_payload_tail(value.len(),
                                                            &value)
                                        .ok()
                             })
                // If the value could not be written to the response payload,
                // update the status to reflect that.
                .map_or_else(|| {
                                status = RpcStatus::StatusInternalError;
                                error!("Could not write to response payload.");
                                None
                             }, | _ | { Some(()) });

        match outcome {
            // The RPC completed successfully. Update the response header with
            // the status and value length.
            Some(()) => {
                let val_len = respons.get_payload().len() as u32;

                let resp_hdr: &mut GetResponse = respons.get_mut_header();
                resp_hdr.value_length = val_len;
                resp_hdr.common_header.status = status;
            }

            // The RPC failed. Update the response header with the status.
            None => {
                let resp_hdr: &mut GetResponse = respons.get_mut_header();
                resp_hdr.common_header.status = status;
            }
        }

        return;
    }

    pub fn test_exts(&mut self) {
        self.extensions.load_one_test_module();
        self.extensions.call(self, 0, "tao");
    }
}

impl Service for Master {
    /// This method takes in a request and a pre-allocated response packet for
    /// Master service, and processes the request.
    ///
    /// - `request`: A packet corresponding to an RPC request parsed upto and
    ///              including it's UDP header. The caller is responsible for
    ///              having determined that this request was destined for Master
    ///              service.
    /// - `respons`: A pre-allocated packet with headers upto UDP that will be
    ///              populated with the response to this particular RPC request.
    ///
    /// - `return`: A tupule consisting of the passed in request and response
    ///             packets de-parsed upto and including their UDP headers.
    fn dispatch(&self,
                request: Packet<UdpHeader, EmptyMetadata>,
                respons: Packet<UdpHeader, EmptyMetadata>) ->
        (Packet<UdpHeader, EmptyMetadata>, Packet<UdpHeader, EmptyMetadata>)
    {
        // Look at the opcode on the request, and figure out what to do with it.
        match parse_rpc_opcode(&request) {
            OpCode::SandstormGetRpc => {
                let request: Packet<GetRequest, EmptyMetadata> =
                    request.parse_header::<GetRequest>();

                // Create an response header for the request.
                let response_header = GetResponse::new();
                let mut respons: Packet<GetResponse, EmptyMetadata> =
                    respons.push_header(&response_header)
                        .expect("ERROR: Failed to setup Get() response header");

                // Handle the RPC request.
                self.get(request.get_header(), &request, &mut respons);

                // Deparse request and response headers so that packets can
                // be handed back to ServerDispatch.
                let request: Packet<UdpHeader, EmptyMetadata> =
                    request.deparse_header(PACKET_UDP_LEN as usize);
                let respons: Packet<UdpHeader, EmptyMetadata> =
                    respons.deparse_header(PACKET_UDP_LEN as usize);

                return (request, respons);
            }

            OpCode::InvalidOperation => {
                // TODO: Set error message on the response packet,
                // deparse respons to UDP header. At present, the
                // response packet will have an empty response header.
                return (request, respons);
            }
        }
    }
}

impl sandstorm::DB for Master {
    // TODO(stutsman): Clearly the DB needs a way to find the calling extension
    // information. We can force them to hand it to us, or we can track it in
    // e.g. TLS.
    fn debug_log(&self, message: &str) {
        info!("EXT {}", message);
    }
}
