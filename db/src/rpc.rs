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

use std::mem::transmute;

use super::wireformat::*;

use e2d2::interface::*;
use e2d2::headers::UdpHeader;
use e2d2::common::EmptyMetadata;

/// This function looks into a packet corresponding to an RPC request, and
/// reads it's service (assumed to be the first byte after the end of the
/// UDP header).
///
/// - `request`: A reference to a packet corresponding to an RPC request.
///              The packet should have been parsed upto it's UDP header.
///
/// - `result`: If valid, the service the request should be dispatched to. If
///             invalid, a code corresponding to an invalid service
///             (InvalidService).
pub fn parse_rpc_service(request: &Packet<UdpHeader, EmptyMetadata>) -> Service
{
    // Read the service off the first byte on the payload.
    let service: u8 = request.get_payload()[0];
    match service.lt(&(Service::InvalidService as u8)) {
        true => {
            unsafe {
                let service: Service = transmute(service);
                return service;
            }
        }

        false => {
            return Service::InvalidService;
        }
    }
}

/// This function looks into a packet corresponding to an RPC request, and
/// reads it's opcode (assumed to be the second byte after the end of the
/// UDP header).
///
/// - `request`: A reference to a packet corresponding to an RPC request.
///              The packet should have been parsed upto it's UDP header.
///
/// - `return`: If valid, the opcode on the RPC request. If invalid, an opcode
///             corresponding to an invalid operation (InvalidOperation) will
///             be returned.
pub fn parse_rpc_opcode(request: &Packet<UdpHeader, EmptyMetadata>) -> OpCode {
    // Read the opcode off the second byte on the payload.
    let opcode: u8 = request.get_payload()[1];
    match opcode.lt(&(OpCode::InvalidOperation as u8)) {
        true => {
            unsafe {
                let opcode: OpCode = transmute(opcode);
                return opcode;
            }
        }

        false => {
            return OpCode::InvalidOperation;
        }
    }
}
