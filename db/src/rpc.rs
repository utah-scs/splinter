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

use std::mem::{size_of, transmute};

use super::wireformat::*;

use e2d2::interface::*;
use e2d2::common::EmptyMetadata;
use e2d2::headers::{IpHeader, MacHeader, UdpHeader};

/// This function looks into a packet corresponding to an RPC request, and
/// reads it's service (assumed to be the first byte after the end of the
/// UDP header).
///
/// # Arguments
///
/// * `request`: A reference to a packet corresponding to an RPC request.
///              The packet should have been parsed upto it's UDP header.
///
/// # Return
///
/// If valid, the service the request should be dispatched to. If invalid, a
/// code corresponding to an invalid service (InvalidService).
pub fn parse_rpc_service(request: &Packet<UdpHeader, EmptyMetadata>) -> Service {
    // Read the service off the first byte on the payload.
    let service: u8 = request.get_payload()[0];
    match service.lt(&(Service::InvalidService as u8)) {
        true => unsafe {
            let service: Service = transmute(service);
            return service;
        },

        false => {
            return Service::InvalidService;
        }
    }
}

/// This function looks into a packet corresponding to an RPC request, and
/// reads it's opcode (assumed to be the second byte after the end of the
/// UDP header).
///
/// # Arguments
///
/// * `request`: A reference to a packet corresponding to an RPC request.
///              The packet should have been parsed upto it's UDP header.
///
/// # Return
///
/// If valid, the opcode on the RPC request. If invalid, an opcode corresponding
/// to an invalid operation (InvalidOperation) will be returned.
pub fn parse_rpc_opcode(request: &Packet<UdpHeader, EmptyMetadata>) -> OpCode {
    // Read the opcode off the second byte on the payload.
    let opcode: u8 = request.get_payload()[1];
    match opcode.lt(&(OpCode::InvalidOperation as u8)) {
        true => unsafe {
            let opcode: OpCode = transmute(opcode);
            return opcode;
        },

        false => {
            return OpCode::InvalidOperation;
        }
    }
}

/// Allocate a packet with MAC, IP, and UDP headers for an RPC request.
///
/// # Panic
///
/// Panics if allocation or header manipulation fails at any point.
///
/// # Arguments
///
/// * `mac`: Reference to the MAC header to be added to the request.
/// * `ip` : Reference to the IP header to be added to the request.
/// * `udp`: Reference to the UDP header to be added to the request.
///
/// # Return
///
/// A packet with the supplied network headers written into it.
#[inline]
fn create_request(
    mac: &MacHeader,
    ip: &IpHeader,
    udp: &UdpHeader,
) -> Packet<UdpHeader, EmptyMetadata> {
    new_packet()
        .expect("Failed to allocate packet for request!")
        .push_header(mac)
        .expect("Failed to push MAC header into request!")
        .push_header(ip)
        .expect("Failed to push IP header into request!")
        .push_header(udp)
        .expect("Failed to push UDP header into request!")
}

/// Sets the length fields on the UDP and IP headers of a packet.
///
/// # Arguments
///
/// * `request`: A packet parsed upto it's UDP header whose UDP and IP length fields need to be
///              set.
///
/// # Return
///
/// A packet parsed upto it's IP headers with said fields set.
pub fn fixup_header_length_fields(
    mut request: Packet<UdpHeader, EmptyMetadata>,
) -> Packet<IpHeader, EmptyMetadata> {
    // Set fields on the UDP header.
    let udp_len = (size_of::<UdpHeader>() + request.get_payload().len()) as u16;
    request.get_mut_header().set_length(udp_len);

    // Set fields on the IP header.
    let mut request = request.deparse_header(size_of::<IpHeader>());
    request
        .get_mut_header()
        .set_length(size_of::<IpHeader>() as u16 + udp_len);

    return request;
}

/// Allocate and populate a packet that requests a server "get" operation.
///
/// # Panic
///
/// May panic if there is a problem allocating the packet or constructing
/// headers.
///
/// # Arguments
///
/// * `mac`:      Reference to the MAC header to be added to the request.
/// * `ip` :      Reference to the IP header to be added to the request.
/// * `udp`:      Reference to the UDP header to be added to the request.
/// * `tenant`:   Id of the tenant requesting the item.
/// * `table_id`: Id of the table from which the key is looked up.
/// * `key`:      Byte string of key whose value is to be fetched. Limit 64 KB.
/// * `id`:       RPC identifier.
///
/// # Return
///
/// Packet populated with the request parameters.
#[inline]
pub fn create_get_rpc(
    mac: &MacHeader,
    ip: &IpHeader,
    udp: &UdpHeader,
    tenant: u32,
    table_id: u64,
    key: &[u8],
    id: u64,
) -> Packet<IpHeader, EmptyMetadata> {
    // Key length cannot be more than 16 bits. Required to construct the RPC header.
    if key.len() > u16::max_value() as usize {
        panic!("Key too long ({} bytes).", key.len());
    }

    // Allocate a packet, write the header and payload into it, and set fields on it's UDP and IP
    // header.
    let mut request = create_request(mac, ip, udp)
        .push_header(&GetRequest::new(tenant, table_id, key.len() as u16, id))
        .expect("Failed to push RPC header into request!");

    request
        .add_to_payload_tail(key.len(), &key)
        .expect("Failed to write key into get() request!");

    fixup_header_length_fields(request.deparse_header(size_of::<UdpHeader>()))
}

/// Allocate and populate a packet that requests a server "put" operation.
///
/// # Panic
///
/// May panic if there is a problem allocating the packet or constructing
/// headers.
///
/// # Arguments
///
/// * `mac`:      Reference to the MAC header to be added to the request.
/// * `ip` :      Reference to the IP header to be added to the request.
/// * `udp`:      Reference to the UDP header to be added to the request.
/// * `tenant`:   Id of the tenant requesting the insertion.
/// * `table_id`: Id of the table into which the key-value pair is to be inserted.
/// * `key`:      Byte string of key whose value is to be inserted. Limit 64 KB.
/// * `val`:      Byte string of the value to be inserted.
/// * `id`:       RPC identifier.
///
/// # Return
///
/// Packet populated with the request parameters.
#[inline]
pub fn create_put_rpc(
    mac: &MacHeader,
    ip: &IpHeader,
    udp: &UdpHeader,
    tenant: u32,
    table_id: u64,
    key: &[u8],
    val: &[u8],
    id: u64,
) -> Packet<IpHeader, EmptyMetadata> {
    // Key length cannot be more than 16 bits. Required to construct the RPC header.
    if key.len() > u16::max_value() as usize {
        panic!("Key too long ({} bytes).", key.len());
    }

    // Allocate a packet, write the header and payload into it, and set fields on it's UDP and IP
    // header.
    let mut request = create_request(mac, ip, udp)
        .push_header(&PutRequest::new(tenant, table_id, key.len() as u16, id))
        .expect("Failed to push RPC header into request!");

    let mut payload = Vec::with_capacity(key.len() + val.len());
    payload.extend_from_slice(key);
    payload.extend_from_slice(val);

    request
        .add_to_payload_tail(payload.len(), &payload)
        .expect("Failed to write key into put() request!");

    fixup_header_length_fields(request.deparse_header(size_of::<UdpHeader>()))
}

/// Allocate and populate a packet that requests a server "invoke" operation.
///
/// # Panic
///
/// May panic if there is a problem allocating the packet or constructing
/// headers.
///
/// # Arguments
///
/// * `mac`:      Reference to the MAC header to be added to the request.
/// * `ip` :      Reference to the IP header to be added to the request.
/// * `udp`:      Reference to the UDP header to be added to the request.
/// * `tenant`:   Id of the tenant requesting the invocation.
/// * `name_len`: Number of bytes at the head of the payload identifying the extension.
/// * `payload`:  The RPC payload to be written into the packet. Should contain the name of the
///               extension, followed by it's arguments.
/// * `id`:       RPC identifier.
///
/// # Return
///
/// Packet populated with the request parameters.
#[inline]
pub fn create_invoke_rpc(
    mac: &MacHeader,
    ip: &IpHeader,
    udp: &UdpHeader,
    tenant: u32,
    name_len: u32,
    payload: &[u8],
    id: u64,
) -> Packet<IpHeader, EmptyMetadata> {
    // The Arguments to the procedure cannot be more that 4 GB long.
    if payload.len() - name_len as usize > u32::max_value() as usize {
        panic!("Args too long ({} bytes).", payload.len() - name_len as usize);
    }

    // Allocate a packet, write the header and payload into it, and set fields on it's UDP and IP
    // header. Since the payload contains both, the name and arguments in it, args_len can be
    // calculated as payload length - name_len.
    let mut request = create_request(mac, ip, udp)
        .push_header(&InvokeRequest::new(
            tenant,
            name_len,
            (payload.len() - name_len as usize) as u32,
            id,
        ))
        .expect("Failed to push RPC header into request!");

    request
        .add_to_payload_tail(payload.len(), &payload)
        .expect("Failed to write args into invoke() request!");

    fixup_header_length_fields(request.deparse_header(size_of::<UdpHeader>()))
}
