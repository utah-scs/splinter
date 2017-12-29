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

use std::sync::Arc;
use std::fmt::Display;
use std::str::FromStr;
use std::net::Ipv4Addr;
use std::option::Option;

use super::master::Master;

use super::e2d2::headers::*;
use super::e2d2::interface::*;
use super::e2d2::scheduler::Executable;
use super::e2d2::common::EmptyMetadata;

/// This type represents a requests-dispatcher in Sandstorm. When added to a
/// Netbricks scheduler, this dispatcher polls a network port for RPCs, and
/// dispatches them to workers.
pub struct ServerDispatch<T>
where
    T: PacketRx + PacketTx + Display + Clone + 'static,
{
    /// A ref counted pointer to a master service. The master service
    /// implements the primary interface to the database.
    master_service: Master,

    /// The network port/interface on which this dispatcher receives and
    /// transmits RPC requests and responses on.
    network_port: T,

    /// The type of Ethernet frames this dispatcher handles. Frames of any
    /// other type are discarded if received.
    network_eth_type: u16,

    /// The Ip address of the server. Any packet with a mismatched destination
    /// Ip address will be dropped by the dispatcher.
    network_ip_addr: u32,

    /// The UDP port this dispatcher listens on. Any packet with a mismatched
    /// destination UDP port will be dropped by the dispatcher.
    network_udp_port: u16,

    /// The maximum number of packets that the dispatcher can receive from the
    /// network interface in a single burst.
    max_rx_packets: u8,

    /// The maximum number of packets that the dispatched can transmit on the
    /// network interface in a single burst.
    max_tx_packets: u8,
}

impl<T> ServerDispatch<T>
where
    T: PacketRx + PacketTx + Display + Clone + 'static,
{
    /// This function creates and returns a requests-dispatcher which can be
    /// added to a Netbricks scheduler. This dispatcher will receive IPv4
    /// Ethernet frames from a network port with a destination IP address of
    /// 192.168.0.2 and UDP port of 0.
    ///
    /// - `net_port`: A network port/interface on which packets will be
    ///               received and transmitted.
    ///
    /// - `return`: A dispatcher of type ServerDispatch capable of receiving
    ///             RPCs, and responding to them.
    pub fn new(net_port: T) -> ServerDispatch<T> {
        let ether_type: u16 = 0x0800;
        let ip_addr = u32::from(Ipv4Addr::from_str("192.168.0.2")
                                    .expect("Failed to create server IP."));
        let udp_port: u16 = 0;
        let rx_batch_size: u8 = 32;
        let tx_batch_size: u8 = 32;

        ServerDispatch {
            master_service: Arc::new(Master::new()),
            network_port: net_port.clone(),
            network_eth_type: ether_type,
            network_ip_addr: ip_addr,
            network_udp_port: udp_port,
            max_rx_packets: rx_batch_size,
            max_tx_packets: tx_batch_size,
        }
    }

    /// This function attempts to receive a batch of packets from the
    /// dispatcher's network port.
    ///
    /// - `return`: A vector of packets wrapped up in Netbrick's
    ///             Packet<NullHeader, EmptyMetadata> type if there was
    ///             anything received at the network port.
    ///
    ///             None, if no packets were received or there was an error
    ///             during the receive.
    fn try_receive_packets(&self) ->
        Option<Vec<Packet<NullHeader, EmptyMetadata>>>
    {
        // Allocate a vector of mutable MBuf pointers into which packets will
        // be received.
        let mut mbuf_vector = Vec::with_capacity(self.max_rx_packets as usize);

        // This unsafe block is needed in order to populate mbuf_vector with a
        // bunch of pointers, and subsequently manipulate these pointers. DPDK
        // will take care of assigning these to actual MBuf's.
        unsafe {
            mbuf_vector.set_len(self.max_rx_packets as usize);

            // Try to receive packets from the network port.
            match self.network_port.recv(&mut mbuf_vector[..]) {
                // The receive call returned successfully.
                Ok(num_received) => {
                    if num_received == 0 {
                        // No packets were available for receive.
                        return None;
                    }

                    // Allocate a vector for the received packets.
                    let mut recvd_packets =
                    Vec::<Packet<NullHeader, EmptyMetadata>>::with_capacity(
                        self.max_rx_packets as usize);

                    // Clear out any dangling pointers in mbuf_vector.
                    for _dangling in num_received..self.max_rx_packets as u32 {
                        mbuf_vector.pop();
                    }

                    // Wrap up the received Mbuf's into Packets. The refcount
                    // on the mbuf's were set by DPDK, and do not need to be
                    // bumped up here. Hence, the call to
                    // packet_from_mbuf_no_increment().
                    for mbuf in mbuf_vector.iter_mut() {
                        recvd_packets.push(packet_from_mbuf_no_increment(*mbuf,
                                                                         0));
                    }

                    return Some(recvd_packets);
                }

                // There was an error during receive.
                Err(ref err) => {
                    error!("Failed to receive packet: {}", err);
                    return None;
                }
            }
        }
    }

    /// This function frees a set of packets that were received from DPDK.
    ///
    /// - `packets`: A vector of packets wrapped in Netbrick's Packet<> type.
    #[inline]
    fn free_packets<S: EndOffset>(&self,
                                  mut packets: Vec<Packet<S, EmptyMetadata>>) {
        while let Some(packet) = packets.pop() {
            packet.free_packet();
        }
    }

    /// This method parses the MAC headers on a vector of input packets.
    ///
    /// This method takes in a vector of packets that were received from
    /// DPDK and wrapped up in Netbrick's Packet<> type, and parses the MAC
    /// headers on the underlying MBufs, effectively rewrapping the packets
    /// into a new type (Packet<MacHeader, EmptyMetadata>).
    ///
    /// Any packets with an unexpected ethertype on the parsed header are
    /// dropped by this method.
    ///
    /// - `packets`: A vector of packets that were received from DPDK and
    ///              wrapped up in Netbrick's Packet<NullHeader, EmptyMetadata>
    ///              type.
    ///
    /// - `return`: A vector of valid packets with their MAC headers parsed.
    ///             The packets are of type Packet<MacHeader, EmptyMetadata>.
    fn parse_mac_headers(&self,
                         mut packets: Vec<Packet<NullHeader, EmptyMetadata>>) ->
        Vec<Packet<MacHeader, EmptyMetadata>>
    {
        // This vector will hold the set of *valid* parsed packets.
        let mut parsed_packets = Vec::<Packet<MacHeader, EmptyMetadata>>::
                                with_capacity(self.max_rx_packets as usize);
        // This vector will hold the set of invalid parsed packets.
        let mut ignore_packets = Vec::<Packet<MacHeader, EmptyMetadata>>::
                                with_capacity(self.max_rx_packets as usize);

        // Parse the MacHeader on each packet, and check if it is valid.
        while let Some(packet) = packets.pop() {
            let mut valid: bool = true;
            let packet: Packet<MacHeader, EmptyMetadata> =
                packet.parse_header::<MacHeader>();

            // The following block borrows the MAC header from the parsed
            // packet, and checks if the ethertype on it matches what the
            // server expects.
            {
                let mac_header: &MacHeader = packet.get_header();

                valid = self.network_eth_type.eq(&mac_header.etype());
            }

            match valid {
                true => { parsed_packets.push(packet); }

                false => { ignore_packets.push(packet); }
            }
        }

        // Drop any invalid packets.
        self.free_packets(ignore_packets);

        return parsed_packets;
    }

    /// This method parses the IP header on a vector of packets that have
    /// already had their MAC headers parsed. A vector of valid packets with
    /// their IP headers parsed is returned.
    ///
    /// This method drops a packet if:
    ///     - It is not an IPv4 packet,
    ///     - The TTL field on it is 0,
    ///     - It's destination IP address does not match that of the server,
    ///     - It's IP header and payload is less than 20 Bytes long.
    ///
    /// - `packets`: A vector of packets with their MAC headers parsed off
    ///              (type Packet<MacHeader, EmptyMetadata>).
    ///
    /// - `return`: A vector of packets with their IP headers parsed, and
    ///             wrapped up in Netbrick's Packet<MacHeader, EmptyMetadata>
    ///             type.
    fn parse_ip_headers(&self,
                        mut packets: Vec<Packet<MacHeader, EmptyMetadata>>) ->
        Vec<Packet<IpHeader, EmptyMetadata>>
    {
        // This vector will hold the set of *valid* parsed packets.
        let mut parsed_packets = Vec::<Packet<IpHeader, EmptyMetadata>>::
                                with_capacity(self.max_rx_packets as usize);
        // This vector will hold the set of invalid parsed packets.
        let mut ignore_packets = Vec::<Packet<IpHeader, EmptyMetadata>>::
                                with_capacity(self.max_rx_packets as usize);

        // Parse the IpHeader on each packet, and check if it is valid.
        while let Some(packet) = packets.pop() {
            let mut valid: bool = true;
            let packet: Packet<IpHeader, EmptyMetadata> =
                packet.parse_header::<IpHeader>();

            // The following block borrows the Ip header from the parsed
            // packet, and checks if it is valid. A packet is considered
            // valid if:
            //      - It is an IPv4 packet,
            //      - It's TTL (time to live) is greater than zero,
            //      - It is at least 20 Bytes long,
            //      - It's destination Ip address matches that of the server.
            {
                let ip_header: &IpHeader = packet.get_header();

                valid = (ip_header.version() == 4) &&
                        (ip_header.ttl() > 0) &&
                        (ip_header.length() >= 20) &&
                        (ip_header.dst() == self.network_ip_addr);
            }

            match valid {
                true => { parsed_packets.push(packet); }

                false => { ignore_packets.push(packet); }
            }
        }

        // Drop any invalid packets.
        self.free_packets(ignore_packets);

        return parsed_packets;
    }

    /// This function parses the UDP headers on a vector of packets that have
    /// had their IP headers parsed. A vector of valid packets with their UDP
    /// headers parsed is returned.
    ///
    /// A packet is dropped by this method if:
    ///     - It's destination UDP port does not match that of the server,
    ///     - It's UDP header plus payload is less than 8 Bytes long.
    ///
    /// - `packets`: A vector of packets with their IP headers parsed.
    ///
    /// - `return`: A vector of packets with their UDP headers parsed. These
    ///             packets are wrapped in Netbrick's
    ///             Packet<UdpHeader, EmptyMetadata> type.
    fn parse_udp_headers(&self,
                         mut packets: Vec<Packet<IpHeader, EmptyMetadata>>) ->
        Vec<Packet<UdpHeader, EmptyMetadata>>
    {
        // This vector will hold the set of *valid* parsed packets.
        let mut parsed_packets = Vec::<Packet<UdpHeader, EmptyMetadata>>::
                                with_capacity(self.max_rx_packets as usize);
        // This vector will hold the set of invalid parsed packets.
        let mut ignore_packets = Vec::<Packet<UdpHeader, EmptyMetadata>>::
                                with_capacity(self.max_rx_packets as usize);

        // Parse the UdpHeader on each packet, and check if it is valid.
        while let Some(packet) = packets.pop() {
            let mut valid: bool = true;
            let packet: Packet<UdpHeader, EmptyMetadata> =
                packet.parse_header::<UdpHeader>();

            // This block borrows the UDP header from the parsed packet, and
            // checks if it is valid. A packet is considered valid if:
            //      - It is atleast 8 Bytes long,
            //      - It's destination port matches that of the server.
            {
                let udp_header: &UdpHeader = packet.get_header();

                valid = (udp_header.length() >= 8) &&
                        (udp_header.dst_port() == self.network_udp_port);
            }

            match valid {
                true => { parsed_packets.push(packet); }

                false => { ignore_packets.push(packet); }
            }
        }

        // Drop any invalid packets.
        self.free_packets(ignore_packets);

        return parsed_packets;
    }

    /// This method polls the dispatchers network port for any received packets.
    #[inline]
    fn poll(&self) {
        match self.try_receive_packets() {
            Some(packets) => {
                // Perform basic network processing on the received packets.
                let mut packets = self.parse_mac_headers(packets);
                let mut packets = self.parse_ip_headers(packets);
                let mut packets = self.parse_udp_headers(packets);

                self.free_packets(packets);
                return;
            }

            None => {
                return;
            }
        }
    }
}

/// Implementation of the Executable trait which will allow the dispatcher to
/// be scheduled by Netbricks.
impl<T> Executable for ServerDispatch<T>
where
    T: PacketRx + PacketTx + Display + Clone + 'static,
{
    /// This method is called by Netbricks, and causes the dispatcher to
    /// receive one batch of packets from the network interface, and process
    /// them.
    fn execute(&mut self) {
        self.poll();
    }

    /// This method returns an empty vector.
    fn dependencies(&mut self) -> Vec<usize> {
        vec![]
    }
}
