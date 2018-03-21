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

use std::fmt::Display;
use std::str::FromStr;
use std::mem::size_of;
use std::net::Ipv4Addr;
use std::option::Option;

use time::{ precise_time_ns };

use super::common;
use super::config;
use super::master::Master;
use super::rpc::parse_rpc_service;
use super::service::Service;
use super::wireformat;

use super::e2d2::headers::*;
use super::e2d2::interface::*;
use super::e2d2::scheduler::Executable;
use super::e2d2::common::EmptyMetadata;

/// This type represents a requests-dispatcher in Sandstorm. When added to a
/// Netbricks scheduler, this dispatcher polls a network port for RPCs,
/// dispatches them to a service, and sends out responses on the same network
/// port.
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

    /// The IP address of the server. This is required to ensure that the
    /// server does not process packets that were destined to a different
    /// machine.
    network_ip_addr: u32,

    /// The maximum number of packets that the dispatcher can receive from the
    /// network interface in a single burst.
    max_rx_packets: u8,

    /// The maximum number of packets that the dispatched can transmit on the
    /// network interface in a single burst.
    max_tx_packets: u8,

    /// The UDP header that will be appended to every response packet (cached
    /// here to avoid wasting time creating a new one for every response
    /// packet).
    resp_udp_header: UdpHeader,

    /// The IP header that will be appended to every response packet (cached
    /// here to avoid creating a new one for every response packet).
    resp_ip_header: IpHeader,

    /// The MAC header that will be appended to every response packet (cached
    /// here to avoid creating a new one for every response packet).
    resp_mac_header: MacHeader,

    /// The number of response packets that were sent out by the dispatcher in
    /// the last measurement interval.
    responses_sent: u64,

    /// An indicator of the start of the current measurement interval.
    measurement_start_ns: u64,

    /// An indicator of the stop of the previous measurement interval.
    measurement_stop_ns: u64,
}

impl<T> ServerDispatch<T>
where
    T: PacketRx + PacketTx + Display + Clone + 'static,
{
    /// This function creates and returns a requests-dispatcher which can be
    /// added to a Netbricks scheduler. This dispatcher will receive IPv4
    /// Ethernet frames from a network port with a destination IP address of
    /// 192.168.0.2 and UDP port of 0. Responses will be sent out to IP address
    /// 192.168.0.1 and UDP port 0.
    ///
    /// - `net_port`: A network port/interface on which packets will be
    ///               received and transmitted.
    ///
    /// - `return`: A dispatcher of type ServerDispatch capable of receiving
    ///             RPCs, and responding to them.
    pub fn new(config: &config::ServerConfig, net_port: T) -> ServerDispatch<T> {
        let rx_batch_size: u8 = 32;
        let tx_batch_size: u8 = 32;

        // Create a common udp header for response packets.
        let udp_src_port: u16 = config.udp_port;
        let udp_dst_port: u16 = common::CLIENT_UDP_PORT;
        // Currently assuming that all incoming requests are Get() operations
        // for 100 Byte values.
        let udp_length: u16 = common::PACKET_UDP_LEN +
                              size_of::<wireformat::GetResponse>() as u16 +
                              100;
        let udp_checksum: u16 = common::PACKET_UDP_CHECKSUM;

        let mut udp_header: UdpHeader = UdpHeader::new();
        udp_header.set_src_port(udp_src_port);
        udp_header.set_dst_port(udp_dst_port);
        udp_header.set_length(udp_length);
        udp_header.set_checksum(udp_checksum);

        // Create a common ip header for response packets.
        let ip_src_addr: u32 =
            u32::from(Ipv4Addr::from_str(&config.ip_address)
                    .expect("Failed to create server IP address."));
        let ip_dst_addr: u32 =
            u32::from(Ipv4Addr::from_str(common::CLIENT_IP_ADDRESS)
                    .expect("Failed to create client IP address."));
        let ip_ttl: u8 = common::PACKET_IP_TTL;
        let ip_version: u8 = common::PACKET_IP_VER;
        let ip_ihl: u8 = common::PACKET_IP_IHL;
        // Currently assuming that all incoming requests are Get() operations
        // for 100 Byte values.
        let ip_length: u16 = common::PACKET_IP_LEN +
                             size_of::<wireformat::GetResponse>() as u16 +
                             100;

        let mut ip_header: IpHeader = IpHeader::new();
        ip_header.set_src(ip_src_addr);
        ip_header.set_dst(ip_dst_addr);
        ip_header.set_ttl(ip_ttl);
        ip_header.set_version(ip_version);
        ip_header.set_ihl(ip_ihl);
        ip_header.set_length(ip_length);

        // Create a common mac header for response packets.
        let mac_src_addr: MacAddress = config.parse_mac();
        let mac_dst_addr: MacAddress = common::CLIENT_MAC_ADDRESS;
        let mac_etype: u16 = common::PACKET_ETYPE;

        let mut mac_header: MacHeader = MacHeader::new();
        mac_header.src = mac_src_addr;
        mac_header.dst = mac_dst_addr;
        mac_header.set_etype(mac_etype);

        ServerDispatch {
            master_service: Master::new(),
            network_port: net_port.clone(),
            network_ip_addr: ip_src_addr,
            max_rx_packets: rx_batch_size,
            max_tx_packets: tx_batch_size,
            resp_udp_header: udp_header,
            resp_ip_header: ip_header,
            resp_mac_header: mac_header,
            responses_sent: 0,
            measurement_start_ns: precise_time_ns(),
            measurement_stop_ns: 0,
        }
    }

    /// This function attempts to receive a batch of packets from the
    /// dispatcher's network port.
    ///
    /// - `return`: A vector of packets wrapped up in Netbrick's
    ///             Packet<NullHeader, EmptyMetadata> type if there was
    ///             anything received at the network port.
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

    /// This method takes as input a vector of packets and tries to send them
    /// out a network interface.
    ///
    /// - `packets`: A vector of packets to be sent out the network, parsed
    ///              upto their UDP headers.
    fn try_send_packets(&mut self,
                        mut packets: Vec<Packet<UdpHeader, EmptyMetadata>>) {
        // This unsafe block is required to extract the underlying Mbuf's from
        // the passed in batch of packets, and send them out the network port.
        unsafe {
            let mut mbufs = vec![];
            let num_packets = packets.len();

            // Extract Mbuf's from the batch of packets.
            while let Some(packet) = packets.pop() {
                mbufs.push(packet.get_mbuf());
            }

            // Send out the above MBuf's.
            match self.network_port.send(&mut mbufs) {
                Ok(sent) => {
                    if sent < num_packets as u32 {
                        warn!("Was able to send only {} of {} packets.", sent,
                              num_packets);
                    }

                    self.responses_sent += mbufs.len() as u64;
                }

                Err(ref err) => {
                    error!("Error on packet send: {}", err);
                }
            }
        }

        // For every million packets sent out by the dispatcher, print out the
        // amount of time in nano seconds it took to do so.
        let every = 1000000;
        if self.responses_sent >= every {
            self.measurement_stop_ns = precise_time_ns();

            info!("{:.0} K/packets/s",
                  (self.responses_sent as f64 / 1e3) /
                      ((self.measurement_stop_ns - self.measurement_start_ns) as f64 / 1e9));

            self.measurement_start_ns = self.measurement_stop_ns;
            self.responses_sent = 0;
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
    #[allow(unused_assignments)]
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

                valid = common::PACKET_ETYPE.eq(&mac_header.etype());
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
    ///     - It's IP header and payload are not long enough.
    ///
    /// - `packets`: A vector of packets with their MAC headers parsed off
    ///              (type Packet<MacHeader, EmptyMetadata>).
    ///
    /// - `return`: A vector of packets with their IP headers parsed, and
    ///             wrapped up in Netbrick's Packet<MacHeader, EmptyMetadata>
    ///             type.
    #[allow(unused_assignments)]
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
            //      - It is not long enough,
            //      - It's destination Ip address matches that of the server.
            {
                const MIN_LENGTH_IP: u16 = common::PACKET_IP_LEN + 2;
                let ip_header: &IpHeader = packet.get_header();

                valid = (ip_header.version() == 4) &&
                        (ip_header.ttl() > 0) &&
                        (ip_header.length() >= MIN_LENGTH_IP) &&
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
    ///     - It's UDP header plus payload is not long enough.
    ///
    /// - `packets`: A vector of packets with their IP headers parsed.
    ///
    /// - `return`: A vector of packets with their UDP headers parsed. These
    ///             packets are wrapped in Netbrick's
    ///             Packet<UdpHeader, EmptyMetadata> type.
    #[allow(unused_assignments)]
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
            //      - It is not long enough,
            //      - It's destination port matches that of the server.
            {
                const MIN_LENGTH_UDP: u16 = common::PACKET_UDP_LEN + 2;
                let udp_header: &UdpHeader = packet.get_header();

                valid = (udp_header.length() >= MIN_LENGTH_UDP) &&
                        (udp_header.dst_port() == self.resp_udp_header.src_port());
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

    /// This method dispatches requests to the appropriate service. A response
    /// packet is pre-allocated by this method and handed in along with the
    /// request. Once the service returns, this method frees the request packet.
    ///
    /// - `requests`: A vector of packets parsed upto and including their UDP
    ///               headers that will be dispatched to the appropriate
    ///               service.
    ///
    /// - `return`: A vector of packets containing the responses to the requests
    ///             that were passed in to this method.
    fn dispatch_requests(&self,
                         mut requests: Vec<Packet<UdpHeader, EmptyMetadata>>) ->
        Vec<Packet<UdpHeader, EmptyMetadata>>
    {
        // This vector will hold the set of packets that were successfully
        // dispatched to a service.
        let mut dispatched_requests =
            Vec::<Packet<UdpHeader, EmptyMetadata>>::with_capacity(
                self.max_rx_packets as usize);
        // This vector will hold the set of responses that need to be sent
        // back to the client.
        let mut response_packets =
            Vec::<Packet<UdpHeader, EmptyMetadata>>::with_capacity(
                self.max_rx_packets as usize);

        while let Some(mut request) = requests.pop() {
            // Allocate a packet for the response upfront.
            let response: Packet<NullHeader, EmptyMetadata> =
                new_packet()
                    .expect("ERROR: Failed to allocate packet for response!");

            // Populate MAC, IP, and UDP headers on the response packet.
            let response: Packet<MacHeader, EmptyMetadata> =
                response.push_header(&self.resp_mac_header)
                            .expect("ERROR: Failed to add response MAC header");
            let response: Packet<IpHeader, EmptyMetadata> =
                response.push_header(&self.resp_ip_header)
                            .expect("ERROR: Failed to add response IP header");
            let mut response: Packet<UdpHeader, EmptyMetadata> =
                response.push_header(&self.resp_udp_header)
                            .expect("ERROR: Failed to add response UDP header");

            // Handoff the request to the appropriate service.
            match parse_rpc_service(&request) {
                wireformat::Service::MasterService => {
                    let result =
                        self.master_service.dispatch(request, response);
                    request = result.0;
                    response = result.1;
                }

                wireformat::Service::InvalidService => {
                    error!("Server received packet for invalid service!");
                }
            }

            response_packets.push(response);
            dispatched_requests.push(request);
        }

        // Free the set of dispatched packets.
        self.free_packets(dispatched_requests);

        return response_packets;
    }

    /// This method polls the dispatchers network port for any received packets,
    /// dispatches them to the appropriate service, and sends out responses over
    /// the network port.
    #[inline]
    fn poll(&mut self) {
        match self.try_receive_packets() {
            Some(packets) => {
                // Perform basic network processing on the received packets.
                let mut packets = self.parse_mac_headers(packets);
                let mut packets = self.parse_ip_headers(packets);
                let mut packets = self.parse_udp_headers(packets);

                // Dispatch these packets to the appropriate service.
                let mut response_packets = self.dispatch_requests(packets);

                // Send response packets out.
                self.try_send_packets(response_packets);

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
