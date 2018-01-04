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

use e2d2::interface::Packet;
use e2d2::headers::UdpHeader;
use e2d2::common::EmptyMetadata;

/// When implemented, this trait allows for a "service" (ex: Master service,
/// or Backup service) to receive RPC requests from ServerDispatch.
pub trait Service {
    /// This function will be invoked by ServerDispatch (which polls the
    /// network interface for incoming requests) to hand off a packet
    /// corresponding to an RPC request.
    ///
    /// - `request`: A packet corresponding to an RPC request which should
    ///              have been parsed upto (and including) it's UDP header.
    /// - `respons`: A pre-allocated packet with headers upto UDP for the
    ///              response to the RPC.
    ///
    /// - `return`: A tupule consisting off the original request packet and
    ///             the response packet populated with the response, both
    ///             deparsed to their UDP headers.
    fn dispatch(&self,
                request: Packet<UdpHeader, EmptyMetadata>,
                respons: Packet<UdpHeader, EmptyMetadata>) ->
        (Packet<UdpHeader, EmptyMetadata>, Packet<UdpHeader, EmptyMetadata>);
}
