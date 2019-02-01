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

use super::task::Task;
use super::wireformat::OpCode;

use e2d2::interface::Packet;
use e2d2::headers::UdpHeader;
use e2d2::common::EmptyMetadata;

/// The Service trait. When implemented, it allows for servicing of RPC requests sent by clients.
pub trait Service {
    /// Dispatches an RPC request, and generates a task that can be scheduled.
    ///
    /// # Arguments
    ///
    /// * `req`: The RPC request packet, parsed upto it's UDP header.
    /// * `res`: The RPC response packet. This has to be pre-allocated by the caller upto UDP.
    ///
    /// # Return
    ///
    /// A `Task` object that can be scheduled and run by the database. In the case of an error, the
    /// passed in request and response packets are returned.
    fn dispatch(
        &self,
        op: OpCode,
        req: Packet<UdpHeader, EmptyMetadata>,
        res: Packet<UdpHeader, EmptyMetadata>,
    ) -> Result<
        Box<Task>,
        (
            Packet<UdpHeader, EmptyMetadata>,
            Packet<UdpHeader, EmptyMetadata>,
        ),
    >;

    /// Dispatches extension invoke RPC, and generates a task that can scheduled.
    /// This function is different from dispatch() in that it only processes invoke() calls
    /// and is part of `fast path`.
    ///
    /// # Arguments
    ///
    /// * `req`: The RPC request packet parsed upto its UDP header.
    /// * `res`: The RPC response packet. This has to be pre-allocated by the caller upto UDP.
    ///
    /// # Return
    ///
    /// A `Task` object that can be scheduled and run by the databse. In the case of an error, the
    /// passed in request and response packets are returned.
    fn dispatch_invoke(
        &self,
        req: Packet<UdpHeader, EmptyMetadata>,
        res: Packet<UdpHeader, EmptyMetadata>,
    ) -> Result<
        Box<Task>,
        (
            Packet<UdpHeader, EmptyMetadata>,
            Packet<UdpHeader, EmptyMetadata>,
        ),
    >;

    /// Services native RPC calls for get(), put(), and multiget()
    /// This function processes the request right away.
    ///
    /// # Arguments
    ///
    /// * `req`: The RPC request packet parsed upto its UDP header.
    /// * `res`: The RPC response packet. This has to be pre-allocated by the caller upto UDP.
    ///
    /// # Return
    ///
    /// The request and response packets, latter containing the result of RPC.
    fn service_native(
        &self,
        op: OpCode,
        req: Packet<UdpHeader, EmptyMetadata>,
        res: Packet<UdpHeader, EmptyMetadata>,
    ) -> Result<
        (
            Packet<UdpHeader, EmptyMetadata>,
            Packet<UdpHeader, EmptyMetadata>,
        ),
        (
            Packet<UdpHeader, EmptyMetadata>,
            Packet<UdpHeader, EmptyMetadata>,
        ),
    >;
}
