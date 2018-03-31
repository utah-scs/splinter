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

// Type definitions for convenience.
pub type TenantId = u32;
pub type TableId = u64;

// The following are constants required to be able to send and receive packets
// between a server and client.
pub const PACKET_UDP_LEN: u16 = 8;
pub const PACKET_UDP_CHECKSUM: u16 = 0;
pub const PACKET_IP_TTL: u8 = 1;
pub const PACKET_IP_VER: u8 = 4;
pub const PACKET_IP_IHL: u8 = 5;
pub const PACKET_IP_LEN: u16 = 20 + PACKET_UDP_LEN;
pub const PACKET_ETYPE: u16 = 0x0800;

// The following are constants required to identify packets sent by the client.
pub const CLIENT_UDP_PORT: u16 = 0;
