/* Copyright (c) 2019 University of Utah
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

use db::e2d2::common::EmptyMetadata;
use db::e2d2::interface::*;
use db::wireformat::{GetResponse, MultiGetResponse, OpCode, PutResponse};

/// Definition of the Workload trait that will allow clients to implement
/// application specific code. This trait mostly helps for native side execution;
/// the only invoke() side exeuction function is to generate the request using
/// get_invoke_request()
pub trait Workload {
    /// This method helps the client to decide on the type of the next
    /// request(get/put/multiget) based on the operation distribution
    /// specified in the configuration file.
    ///
    /// # Return
    ///
    /// The enum OpCode which indicates the operation type.
    fn next_optype(&mut self) -> OpCode;

    /// This method returns the name length used in the invoke() payload. It helps
    /// both client and server to parse the argument passed in the request payload.
    ///
    /// # Return
    ///
    /// The name length for the extension name in the request payload.
    fn name_length(&self) -> u32;

    /// This method decides the arguements for the next invoke() operation.
    /// Usually, it changes call specific arguments in a vector and which
    /// is later added to the request payload.
    ///
    /// # Return
    ///
    /// The tenant-id and request payload reference.
    fn get_invoke_request(&mut self) -> (u32, &[u8]);

    /// This method decides the arguments for the next get() request.
    /// The tenant and the key are sampled through a pre-decided distribution
    /// and a from specific range.
    ///
    /// # Return
    ///
    /// The tenant-od and key-id for the get() request.
    fn get_get_request(&mut self) -> (u32, &[u8]);

    /// This method decides the arguments for the next put() request.
    /// The tenant, key and the value are sampled through a pre-decided
    /// distribution and from a specific range.
    ///
    /// # Return
    ///
    /// The tenant-id, key-id, and value for the put() request.
    fn get_put_request(&mut self) -> (u32, &[u8], &[u8]);

    /// This method decides the arguments for the next multiget() request.
    /// The tenant, number of keys and key-ids are sampled through a pre-decided
    /// distribution and from a specific range.
    ///
    /// # Return
    ///
    /// The tenant-id, key-id, and value for the put() request.
    fn get_multiget_request(&mut self) -> (u32, u32, &[u8]);

    /// This method handles the `GetResponse` based on application specific logic.
    /// And it is called only for the response for the native execution.
    ///
    /// # Arguments
    /// *`packet`: The reference to the `GetResponse` packet.
    fn process_get_response(&mut self, packet: &Packet<GetResponse, EmptyMetadata>);

    /// This method handles the `PutResponse` based on application specific logic.
    /// And it is called only for the response for the native execution.
    ///
    /// # Arguments
    /// *`packet`: The reference to the `PutResponse` packet.
    fn process_put_response(&mut self, packet: &Packet<PutResponse, EmptyMetadata>);

    /// This method handles the `MultiGetResponse` based on application specific logic.
    /// And it is called only for the response for the native execution.
    ///
    /// # Arguments
    /// *`packet`: The reference to the `MultiGetResponse` packet.
    fn process_multiget_response(&mut self, packet: &Packet<MultiGetResponse, EmptyMetadata>);
}
