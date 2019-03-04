/* Copyright (c) 2017 University of Utah
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

/// Type definitions for TenantId for convenience.
pub type TenantId = u32;
/// Type definitions for TableId for convenience.
pub type TableId = u64;

// The following are constants required to be able to send and receive packets
// between a server and client.
/// The length of the UDP header.
pub const PACKET_UDP_LEN: u16 = 8;
/// The UDP checksum to update in the packet.
pub const PACKET_UDP_CHECKSUM: u16 = 0;
/// Default value for IP header field, time to live.
pub const PACKET_IP_TTL: u8 = 1;
/// Default value for IP header field, IP version.
pub const PACKET_IP_VER: u8 = 4;
/// Default value for IP header field, IP header length.
pub const PACKET_IP_IHL: u8 = 5;
/// Default value for the IP header field, Header length.
pub const PACKET_IP_LEN: u16 = 20 + PACKET_UDP_LEN;
/// Default value for Ethernet type; 0x0800	indicated IPv4.
pub const PACKET_ETYPE: u16 = 0x0800;
/// The Length of IP header which is used for packet parsing.
pub const IP_HDR_LEN: usize = 20;
/// The Length of MAC header which is used for packet parsing.
pub const MAC_HDR_LEN: usize = 14;

/// The following are constants required to identify packets sent by the client.
pub const CLIENT_UDP_PORT: u16 = 0;

// The following are constants required to populate the credit system for each extension.
// XXX: Modify later.
/// Default value of the credit which is given to the extension after performing a get().
pub const GET_CREDIT: u64 = 0;
/// Default value of the credit which is given to the extension after performing a put().
pub const PUT_CREDIT: u64 = 0;
/// Default value of the credit which is given to the extension after performing a multiget().
pub const MULTIGET_CREDIT: u64 = 0;
