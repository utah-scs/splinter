extern crate sandstorm;

use std::collections::HashMap;

use super::ext::*;
use super::table::*;
use super::service::Service;
use super::rpc::parse_rpc_opcode;
use super::wireformat::{ OpCode, GetRequest };
use super::common::{ UserId, TableId, PACKET_UDP_LEN };

use e2d2::interface::Packet;
use e2d2::headers::UdpHeader;
use e2d2::common::EmptyMetadata;

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
        let table = Table::default();
        let key: &[u8] = &[1; 30];
        let val: &[u8] = &[ 91 ];
        table.put(key, val);
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

    /// This method handles a Get RPC request.
    ///
    /// - `request`: The packet corresponding to the RPC request parsed
    ///              upto and including it's GetRequest header.
    /// - `response`: The packet corresponding to this RPC request's response.
    ///
    /// - `return`: A packet with the response to the passed in request.
    fn get(&self,
           request: &Packet<GetRequest, EmptyMetadata>,
           response: Packet<UdpHeader, EmptyMetadata>) ->
        Packet<UdpHeader, EmptyMetadata>
    {
        let req_hdr: &GetRequest = request.get_header();

        // Check if the tenant exists.
        let tenant_id: &u32 = &req_hdr.rpc_header.tenant;
        match self.users.get(tenant_id) {
            Some(tenant) => {
                // Check if the table exists
                let table_id: &u64 = &req_hdr.table_id;
                match tenant.tables.get(table_id) {
                    Some(table) => {
                        // Check if the key exists.
                        let key: &[u8] = request.get_payload();
                        match table.get(key) {
                            Some(value) => {
                                println!("Value: {}", value[0]);
                                return response;
                            }

                            None => {
                                warn!("Object not found!");
                                return response;
                            }
                        }
                    }

                    None => {
                        warn!("Received Get request on invalid table {}!",
                              table_id);
                        return response;
                    }
                }
            }

            None => {
                warn!("Received Get request from invalid tenant {}!",
                      tenant_id);
                return response;
            }
        }

        return response;
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

                let respons: Packet<UdpHeader, EmptyMetadata> =
                    self.get(&request, respons);

                let request: Packet<UdpHeader, EmptyMetadata> =
                    request.deparse_header(PACKET_UDP_LEN as usize);
                // TODO: deparse respons to UDP header.

                return (request, respons);
            }

            OpCode::InvalidOperation => {
                // TODO: Set error message on the response packet,
                // deparse respons to UDP header.
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
