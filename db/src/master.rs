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

use std::rc::Rc;
use std::sync::Arc;
use std::collections::HashMap;

use super::ext::*;
use super::wireformat::*;
use super::native::Native;
use super::tenant::Tenant;
use super::service::Service;
use super::context::Context;
use super::alloc::Allocator;
use super::container::Container;
use super::rpc::parse_rpc_opcode;
use super::task::{Task, TaskPriority, TaskState};
use super::common::{TenantId, TableId, PACKET_UDP_LEN};

use e2d2::interface::Packet;
use e2d2::headers::UdpHeader;
use e2d2::common::EmptyMetadata;

use spin::{RwLock};

pub struct Master {
    tenants: RwLock<HashMap<TenantId, Arc<Tenant>>>,
    extensions: ExtensionManager,
    heap: Arc<Allocator>,
}

impl Master {
    pub fn new() -> Master {
        let tenant = Tenant::new(1);
        tenant.create_table(1);

        let master = Master {
            tenants: RwLock::new(HashMap::new()),
            extensions: ExtensionManager::new(),
            heap: Arc::new(Allocator::new()),
        };

        let (key, obj) = master.heap.object(1, 1, &[1; 30], &[91, 100])
                                    .expect("Failed to create dummy object.");
        tenant.get_table(1)
                .expect("Failed to init test table.")
                .put(key, obj);

        master.insert_tenant(tenant);

        // Load a get extension for this tenant.
        let name = "../ext/get/target/release/libget.so";
        if master.extensions.load(name, 1, "get") == false {
            panic!("Failed to load get() extension.");
        }

        master
    }

    // This method returns a handle to a tenant if it exists.
    //
    // # Arguments
    //
    // * `tenant_id`: The identifier for the tenant to be returned.
    //
    // # Return
    //
    // An atomic reference counted handle to the tenant if it exists.
    fn get_tenant(&self, tenant_id: TenantId) -> Option<Arc<Tenant>> {
        // Acquire a read lock.
        let map = self.tenants.read();

        // Lookup, and return the tenant if it exists.
        map.get(&tenant_id)
            .and_then(| tenant | { Some(Arc::clone(tenant)) })
    }

    // This method adds a tenant to Master.
    //
    // # Arguments
    //
    // * `tenant`: The tenant to be added.
    fn insert_tenant(&self, tenant: Tenant) {
        // Acquire a write lock.
        let mut map = self.tenants.write();

        // Insert the tenant and return.
        map.insert(tenant.id(), Arc::new(tenant));
    }

    // This method handles the Get() RPC request. A hash table lookup is
    // performed on a supplied tenant id, table id, and key. If successfull,
    // the result of the lookup is written into a response packet, and the
    // response header is updated. In the case of a failure, the response
    // header is updated with a status indicating the reason for the failure.
    //
    // # Arguments
    //
    // * `request`: A reference to the entire request packet.
    // * `respons`: A mutable reference to the entire response packet.
    #[allow(unreachable_code)]
    fn get(&self, request: Packet<GetRequest, EmptyMetadata>, mut respons: Packet<GetResponse, EmptyMetadata>)
           -> (Packet<UdpHeader, EmptyMetadata>, Packet<UdpHeader, EmptyMetadata>)
    {
        // Read fields of the request header.
        let tenant_id: TenantId = request.get_header().common_header.tenant as TenantId;
        let table_id: TableId = request.get_header().table_id as TableId;
        let key_length: u16 = request.get_header().key_length;

        // If the payload size is less than the key length, return an error.
        if request.get_payload().len() < key_length as usize {
            respons.get_mut_header().common_header.status =
                                            RpcStatus::StatusMalformedRequest;
            let request = request.deparse_header(PACKET_UDP_LEN as usize);
            let respons = respons.deparse_header(PACKET_UDP_LEN as usize);

            return (request, respons);
        }

        // Lookup the tenant, and get a handle to the allocator. Required to avoid capturing a
        // reference to Master in the generator below.
        let tenant = self.get_tenant(tenant_id);
        let alloc = self.heap.clone();

        // Create a generator for this request.
        let gen = Box::new(move || {
            let mut status: RpcStatus = RpcStatus::StatusTenantDoesNotExist;

            let outcome =
                // Check if the tenant exists. If it does, then check if the
                // table exists, and update the status of the rpc.
                tenant.and_then(| tenant | {
                                status = RpcStatus::StatusTableDoesNotExist;
                                tenant.get_table(table_id)
                            })
                // If the table exists, lookup the provided key, and update
                // the status of the rpc.
                .and_then(| table | {
                                status = RpcStatus::StatusObjectDoesNotExist;
                                let (key, _) = request.get_payload().split_at(key_length as usize);
                                table.get(key)
                            })
                // If the lookup succeeded, obtain the value, and update the
                // status of the rpc.
                .and_then(| object | {
                                status = RpcStatus::StatusInternalError;
                                alloc.resolve(object)
                            })
                // If the value was obtained, then write to the response packet
                // and update the status of the rpc.
                .and_then(| (_k, value) | {
                                status = RpcStatus::StatusInternalError;
                                respons.add_to_payload_tail(value.len(),
                                                            &value)
                                       .ok()
                            })
                // If the value was written to the response payload,
                // update the status of the rpc.
                .and_then(| _ | {
                                status = RpcStatus::StatusOk;
                                Some(())
                            });

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
                    respons.get_mut_header().common_header.status = status;
                }
            }

            // Deparse request and response packets down to UDP.
            let request = request.deparse_header(PACKET_UDP_LEN as usize);
            let respons = respons.deparse_header(PACKET_UDP_LEN as usize);

            return Some((request, respons));

            // XXX: This yield is required to get the compiler to compile this closure into a
            // generator. It is unreachable and benign.
            yield 0;
        });

        // Create a native task and run it.
        let mut task = Native::new(TaskPriority::REQUEST, gen);

        while task.run().0 != TaskState::COMPLETED { ; }

        unsafe {
            return task.tear().expect("Failed to get packets!");
        }
    }

    #[allow(unreachable_code)]
    fn put(&self, request: Packet<PutRequest, EmptyMetadata>, mut respons: Packet<PutResponse, EmptyMetadata>)
           -> (Packet<UdpHeader, EmptyMetadata>, Packet<UdpHeader, EmptyMetadata>)
    {
        // Read fields of the request header.
        let tenant_id: TenantId = request.get_header().common_header.tenant as TenantId;
        let table_id: TableId = request.get_header().table_id as TableId;
        let key_length: u16 = request.get_header().key_length;

        // If the payload size is less than the key length, return an error.
        if request.get_payload().len() < key_length as usize {
            respons.get_mut_header().common_header.status = RpcStatus::StatusMalformedRequest;
            let request = request.deparse_header(PACKET_UDP_LEN as usize);
            let respons = respons.deparse_header(PACKET_UDP_LEN as usize);

            return (request, respons);
        }

        // Lookup the tenant, and get a handle to the allocator. Required to avoid capturing a
        // reference to Master in the generator below.
        let tenant = self.get_tenant(tenant_id);
        let alloc = self.heap.clone();

        // Create a generator for this request.
        let gen = Box::new(move || {
            let mut status: RpcStatus = RpcStatus::StatusTenantDoesNotExist;

            // If the tenant exists, check if it has a table with the given id,
            // and update the status of the rpc.
            let outcome = tenant.and_then(| tenant | {
                                status = RpcStatus::StatusTableDoesNotExist;
                                tenant.get_table(table_id)
                            });

            // If the table exists, update the status of the rpc, and allocate an
            // object.
            if let Some(table) = outcome {
                // Get a reference to the key and value.
                status = RpcStatus::StatusMalformedRequest;
                let (key, val) = request.get_payload().split_at(key_length as usize);

                // If there is a value, then write it in.
                if val.len() > 0 {
                    status = RpcStatus::StatusInternalError;
                    let _result = alloc.object(tenant_id, table_id, key, val)
                                        // If the allocation succeeds, update the
                                        // status of the rpc, and insert the object
                                        // into the table.
                                        .and_then(| (key, obj) | {
                                                status = RpcStatus::StatusOk;
                                                table.put(key, obj);
                                                Some(())
                                            });
                }
            }

            // Update the response header, and return.
            respons.get_mut_header().common_header.status = status;

            // Deparse request and response packets to UDP.
            let request = request.deparse_header(PACKET_UDP_LEN as usize);
            let respons = respons.deparse_header(PACKET_UDP_LEN as usize);

            return Some((request, respons));

            // XXX: This yield is required to get the compiler to compile this closure into a
            // generator. It is unreachable and benign.
            yield 0;
        });

        // Create a native task and run it.
        let mut task = Native::new(TaskPriority::REQUEST, gen);

        while task.run().0 != TaskState::COMPLETED { ; }

        unsafe {
            return task.tear().expect("Failed to get packets!");
        }
    }

    fn invoke(&self, request: Packet<InvokeRequest, EmptyMetadata>,
              mut respons: Packet<InvokeResponse, EmptyMetadata>)
              -> (Packet<UdpHeader, EmptyMetadata>,
                  Packet<UdpHeader, EmptyMetadata>)
    {
        // Read fields of the request header.
        let tenant_id: TenantId = request.get_header()
                                            .common_header.tenant as TenantId;
        let name_length: usize = request.get_header().name_length as usize;
        let args_length: usize = request.get_header().args_length as usize;

        // If the payload size is less than the sum of the name and args
        // length, return an error.
        if request.get_payload().len() < name_length + args_length {
            respons.get_mut_header().common_header.status =
                                            RpcStatus::StatusMalformedRequest;
            let request = request.deparse_header(PACKET_UDP_LEN as usize);
            let respons = respons.deparse_header(PACKET_UDP_LEN as usize);

            return (request, respons);
        }

        // Read the extension's name from the request payload.
        let mut raw_name = Vec::new();
        raw_name.extend_from_slice(request.get_payload()
                                            .split_at(name_length).0);
        let ext_name: String = String::from_utf8(raw_name)
                                    .expect("ERROR: Failed to get ext name.");

        // Check if the request was issued by a valid tenant.
        match self.get_tenant(tenant_id) {
            // The tenant exists. Do nothing for now.
            Some(tenant) => {
                // Construct a task for the extension.
                let db = Rc::new(Context::new(request, name_length, args_length,
                                      respons, tenant, Arc::clone(&self.heap)));
                let ext = self.extensions.get(tenant_id, &ext_name)
                                            .expect("Extension does not exist");
                let mut task = Container::new(TaskPriority::REQUEST, db, ext);

                // Run the task to completion.
                while task.run().0 != TaskState::COMPLETED { ; }

                // Tear down the task, and return.
                unsafe {
                    return task.tear().expect("Failed to get packets");
                }
            }

            // The issuing tenant does not exist. Return an error to the client.
            None => {
                respons.get_mut_header().common_header.status =
                                            RpcStatus::StatusTenantDoesNotExist;
                let request = request.deparse_header(PACKET_UDP_LEN as usize);
                let respons = respons.deparse_header(PACKET_UDP_LEN as usize);

                return (request, respons);
            }
        }
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

                // Create a response header for the request.
                let response_header = GetResponse::new();
                let mut respons: Packet<GetResponse, EmptyMetadata> =
                    respons.push_header(&response_header)
                        .expect("ERROR: Failed to setup Get() response header");

                // Handle the RPC request.
                return self.get(request, respons);
            }

            OpCode::SandstormPutRpc => {
                let request: Packet<PutRequest, EmptyMetadata> =
                    request.parse_header::<PutRequest>();

                // Create a response header for the request.
                let response_header = PutResponse::new();
                let mut respons: Packet<PutResponse, EmptyMetadata> =
                    respons.push_header(&response_header)
                        .expect("ERROR: Failed to setup Put() response header");

                // Handle the RPC request.
                return self.put(request, respons);
            }

            OpCode::SandstormInvokeRpc => {
                let request: Packet<InvokeRequest, EmptyMetadata> =
                    request.parse_header::<InvokeRequest>();

                // Create a response header for the request.
                let response_header = InvokeResponse::new();
                let mut respons: Packet<InvokeResponse, EmptyMetadata> =
                    respons.push_header(&response_header)
                        .expect("ERROR: Failed to setup invoke() resp header");

                // Handle the RPC request.
                return self.invoke(request, respons);
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
