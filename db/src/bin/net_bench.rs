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

#![feature(use_extern_macros)]

extern crate db;
extern crate libc;
extern crate nix;
extern crate sandstorm;

use std::fmt::Display;
use std::net::Ipv4Addr;
use std::str::FromStr;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

use db::config;
use db::e2d2::allocators::CacheAligned;
use db::e2d2::common::EmptyMetadata;
use db::e2d2::config::{NetbricksConfiguration, PortConfiguration};
use db::e2d2::headers::*;
use db::e2d2::interface::*;
use db::e2d2::scheduler::Executable;
use db::e2d2::scheduler::NetBricksContext as NetbricksContext;
use db::e2d2::scheduler::*;
use db::log::*;
use db::rpc;

use sandstorm::common;

pub struct RpcHandler<T>
where
    T: PacketRx + PacketTx + Display + Clone + 'static,
{
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

    // Response packets returned by completed tasks. Will be picked up and sent out the network by
    // the Dispatch task.
    responses: Vec<Packet<IpHeader, EmptyMetadata>>,

    /// The number of response packets that were sent out by the dispatcher in
    /// the last measurement interval.
    responses_sent: u64,
}

impl<T> RpcHandler<T>
where
    T: PacketRx + PacketTx + Display + Clone + 'static,
{
    pub fn new(net_port: T, config: &config::ServerConfig) -> RpcHandler<T> {
        let rx_batch_size: u8 = 32;

        // Create a common udp header for response packets.
        let udp_src_port: u16 = config.udp_port;
        let udp_dst_port: u16 = common::CLIENT_UDP_PORT;
        let udp_length: u16 = common::PACKET_UDP_LEN;
        let udp_checksum: u16 = common::PACKET_UDP_CHECKSUM;

        let mut udp_header: UdpHeader = UdpHeader::new();
        udp_header.set_src_port(udp_src_port);
        udp_header.set_dst_port(udp_dst_port);
        udp_header.set_length(udp_length);
        udp_header.set_checksum(udp_checksum);

        // Create a common ip header for response packets.
        let ip_src_addr: u32 = u32::from(
            Ipv4Addr::from_str(&config.ip_address).expect("Failed to create server IP address."),
        );
        let ip_dst_addr: u32 = u32::from(
            Ipv4Addr::from_str(&config.client_ip).expect("Failed to create client IP address."),
        );
        let ip_ttl: u8 = common::PACKET_IP_TTL;
        let ip_version: u8 = common::PACKET_IP_VER;
        let ip_ihl: u8 = common::PACKET_IP_IHL;
        let ip_length: u16 = common::PACKET_IP_LEN;

        let mut ip_header: IpHeader = IpHeader::new();
        ip_header.set_src(ip_src_addr);
        ip_header.set_dst(ip_dst_addr);
        ip_header.set_ttl(ip_ttl);
        ip_header.set_version(ip_version);
        ip_header.set_ihl(ip_ihl);
        ip_header.set_length(ip_length);
        ip_header.set_protocol(0x11);

        // Create a common mac header for response packets.
        let mac_src_addr: MacAddress = config.parse_mac();
        let mac_dst_addr: MacAddress = config.parse_client_mac();
        let mac_etype: u16 = common::PACKET_ETYPE;

        let mut mac_header: MacHeader = MacHeader::new();
        mac_header.src = mac_src_addr;
        mac_header.dst = mac_dst_addr;
        mac_header.set_etype(mac_etype);
        RpcHandler {
            network_port: net_port.clone(),
            network_ip_addr: ip_src_addr,
            max_rx_packets: rx_batch_size,
            resp_udp_header: udp_header,
            resp_ip_header: ip_header,
            resp_mac_header: mac_header,
            responses: Vec::with_capacity(rx_batch_size as usize),
            responses_sent: 0,
        }
    }

    pub fn send(&mut self) {
        let responses: Vec<Packet<IpHeader, EmptyMetadata>> = self.responses.drain(..).collect();
        if responses.len() > 0 {
            self.try_send_packets(responses);
        }
    }

    pub fn recv(&mut self) {
        if let Some(packets) = self.try_receive_packets() {
            let mut packets = self.parse_mac_headers(packets);
            let mut packets = self.parse_ip_headers(packets);
            let mut packets = self.parse_udp_headers(packets);
            self.dispatch_requests(packets);
        }
    }

    /// This method takes as input a vector of packets and tries to send them
    /// out a network interface.
    ///
    /// # Arguments
    ///
    /// * `packets`: A vector of packets to be sent out the network, parsed upto their UDP headers.
    fn try_send_packets(&mut self, mut packets: Vec<Packet<IpHeader, EmptyMetadata>>) {
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
                        warn!("Was able to send only {} of {} packets.", sent, num_packets);
                    }

                    self.responses_sent += mbufs.len() as u64;
                }

                Err(ref err) => {
                    error!("Error on packet send: {}", err);
                }
            }
        }
    }

    /// This function attempts to receive a batch of packets from the
    /// dispatcher's network port.
    ///
    /// # Return
    ///
    /// A vector of packets wrapped up in Netbrick's Packet<NullHeader, EmptyMetadata> type if
    /// there was anything received at the network port.
    fn try_receive_packets(&self) -> Option<Vec<Packet<NullHeader, EmptyMetadata>>> {
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
                    let mut recvd_packets = Vec::<Packet<NullHeader, EmptyMetadata>>::with_capacity(
                        self.max_rx_packets as usize,
                    );

                    // Clear out any dangling pointers in mbuf_vector.
                    for _dangling in num_received..self.max_rx_packets as u32 {
                        mbuf_vector.pop();
                    }

                    // Wrap up the received Mbuf's into Packets. The refcount
                    // on the mbuf's were set by DPDK, and do not need to be
                    // bumped up here. Hence, the call to
                    // packet_from_mbuf_no_increment().
                    for mbuf in mbuf_vector.iter_mut() {
                        recvd_packets.push(packet_from_mbuf_no_increment(*mbuf, 0));
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
    /// # Arguments
    ///
    /// * `packets`: A vector of packets wrapped in Netbrick's Packet<> type.
    #[inline]
    fn free_packets<S: EndOffset>(&self, mut packets: Vec<Packet<S, EmptyMetadata>>) {
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
    /// # Arguments
    ///
    /// * `packets`: A vector of packets that were received from DPDK and
    ///              wrapped up in Netbrick's Packet<NullHeader, EmptyMetadata>
    ///              type.
    ///
    /// # Return
    ///
    /// A vector of valid packets with their MAC headers parsed. The packets are of type
    /// `Packet<MacHeader, EmptyMetadata>`.
    #[allow(unused_assignments)]
    fn parse_mac_headers(
        &self,
        mut packets: Vec<Packet<NullHeader, EmptyMetadata>>,
    ) -> Vec<Packet<MacHeader, EmptyMetadata>> {
        // This vector will hold the set of *valid* parsed packets.
        let mut parsed_packets = Vec::with_capacity(self.max_rx_packets as usize);
        // This vector will hold the set of invalid parsed packets.
        let mut ignore_packets = Vec::with_capacity(self.max_rx_packets as usize);

        // Parse the MacHeader on each packet, and check if it is valid.
        while let Some(packet) = packets.pop() {
            let mut valid: bool = true;
            let packet = packet.parse_header::<MacHeader>();

            // The following block borrows the MAC header from the parsed
            // packet, and checks if the ethertype on it matches what the
            // server expects.
            {
                let mac_header: &MacHeader = packet.get_header();
                valid = common::PACKET_ETYPE.eq(&mac_header.etype());
            }

            match valid {
                true => {
                    parsed_packets.push(packet);
                }

                false => {
                    ignore_packets.push(packet);
                }
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
    /// # Arguments
    ///
    /// * `packets`: A vector of packets with their MAC headers parsed off
    ///              (type Packet<MacHeader, EmptyMetadata>).
    ///
    /// # Return
    ///
    /// A vector of packets with their IP headers parsed, and wrapped up in Netbrick's
    /// `Packet<MacHeader, EmptyMetadata>` type.
    #[allow(unused_assignments)]
    fn parse_ip_headers(
        &self,
        mut packets: Vec<Packet<MacHeader, EmptyMetadata>>,
    ) -> Vec<Packet<IpHeader, EmptyMetadata>> {
        // This vector will hold the set of *valid* parsed packets.
        let mut parsed_packets = Vec::with_capacity(self.max_rx_packets as usize);
        // This vector will hold the set of invalid parsed packets.
        let mut ignore_packets = Vec::with_capacity(self.max_rx_packets as usize);

        // Parse the IpHeader on each packet, and check if it is valid.
        while let Some(packet) = packets.pop() {
            let mut valid: bool = true;
            let packet = packet.parse_header::<IpHeader>();

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
                valid = (ip_header.version() == 4)
                    && (ip_header.ttl() > 0)
                    && (ip_header.length() >= MIN_LENGTH_IP)
                    && (ip_header.dst() == self.network_ip_addr);
            }

            match valid {
                true => {
                    parsed_packets.push(packet);
                }

                false => {
                    ignore_packets.push(packet);
                }
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
    /// # Arguments
    ///
    /// * `packets`: A vector of packets with their IP headers parsed.
    ///
    /// # Return
    ///
    /// A vector of packets with their UDP headers parsed. These packets are wrapped in Netbrick's
    /// `Packet<UdpHeader, EmptyMetadata>` type.
    #[allow(unused_assignments)]
    fn parse_udp_headers(
        &self,
        mut packets: Vec<Packet<IpHeader, EmptyMetadata>>,
    ) -> Vec<Packet<UdpHeader, EmptyMetadata>> {
        // This vector will hold the set of *valid* parsed packets.
        let mut parsed_packets = Vec::with_capacity(self.max_rx_packets as usize);
        // This vector will hold the set of invalid parsed packets.
        let mut ignore_packets = Vec::with_capacity(self.max_rx_packets as usize);

        // Parse the UdpHeader on each packet, and check if it is valid.
        while let Some(packet) = packets.pop() {
            let mut valid: bool = true;
            let packet = packet.parse_header::<UdpHeader>();

            // This block borrows the UDP header from the parsed packet, and
            // checks if it is valid. A packet is considered valid if:
            //      - It is not long enough,
            {
                const MIN_LENGTH_UDP: u16 = common::PACKET_UDP_LEN + 2;
                let udp_header: &UdpHeader = packet.get_header();
                valid = udp_header.length() >= MIN_LENGTH_UDP;
            }

            match valid {
                true => {
                    parsed_packets.push(packet);
                }

                false => {
                    ignore_packets.push(packet);
                }
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
    /// # Arguments
    ///
    /// * `requests`: A vector of packets parsed upto and including their UDP
    ///               headers that will be dispatched to the appropriate
    ///               service.
    fn dispatch_requests(&mut self, mut requests: Vec<Packet<UdpHeader, EmptyMetadata>>) {
        // This vector will hold response packets of native requests.
        // It is then appended to scheduler's 'responses' queue
        // so these reponses can be sent out next time dispatch task is run.
        let mut native_responses = Vec::with_capacity(self.max_rx_packets as usize);

        while let Some(request) = requests.pop() {
            // Set the destination ip address on the response IP header.
            let ip = request.deparse_header(common::IP_HDR_LEN);
            self.resp_ip_header.set_dst(ip.get_header().src());

            // Set the destination mac address on the response MAC header.
            let mac = ip.deparse_header(common::MAC_HDR_LEN);
            self.resp_mac_header.set_dst(mac.get_header().src());

            let request = mac.parse_header::<IpHeader>().parse_header::<UdpHeader>();

            // Allocate a packet for the response upfront, and add in MAC, IP, and UDP headers.
            let mut response = new_packet()
                .expect("ERROR: Failed to allocate packet for response!")
                .push_header(&self.resp_mac_header)
                .expect("ERROR: Failed to add response MAC header")
                .push_header(&self.resp_ip_header)
                .expect("ERROR: Failed to add response IP header")
                .push_header(&self.resp_udp_header)
                .expect("ERROR: Failed to add response UDP header");

            // Set the destination port on the response UDP header.
            response
                .get_mut_header()
                .set_dst_port(request.get_header().src_port());

            // TODO:Process and append to reponse queue.

            request.free_packet();
            native_responses.push(rpc::fixup_header_length_fields(response));
        }
        // Enqueue completed native resps on to scheduler's responses queue
        self.responses.append(&mut native_responses);
    }
}

// Executable trait allowing PushbackRecv to be scheduled by Netbricks.
impl<T> Executable for RpcHandler<T>
where
    T: PacketTx + PacketRx + Display + Clone + 'static,
{
    // Called internally by Netbricks.
    fn execute(&mut self) {
        self.send();
        self.recv();
    }

    fn dependencies(&mut self) -> Vec<usize> {
        vec![]
    }
}

/// This function sets up a Sandstorm server's dispatch thread on top
/// of Netbricks.
fn setup_server<S>(
    config: &config::ServerConfig,
    ports: Vec<CacheAligned<PortQueue>>,
    _sibling: CacheAligned<PortQueue>,
    scheduler: &mut S,
    _core: i32,
) where
    S: Scheduler + Sized,
{
    if ports.len() != 1 {
        error!("Server should be configured with exactly 1 port!");
        std::process::exit(1);
    }

    // Add the server to a netbricks pipeline.
    match scheduler.add_task(RpcHandler::new(ports[0].clone(), config)) {
        Ok(_) => {
            info!(
                "Successfully added scheduler for rx {}, tx {}.",
                ports[0].rxq(),
                ports[0].txq()
            );
        }

        Err(ref err) => {
            error!("Error while adding to Netbricks pipeline {}", err);
            std::process::exit(1);
        }
    }
}

/// Returns a struct of type NetbricksConfiguration which can be used to
/// initialize Netbricks with a default set of parameters.
///
/// If used to initialize Netbricks, this struct will run the parent server
/// thread on core 0, and one scheduler on core 1. Packet buffers will be
/// allocated from a 2 GB memory pool, with 64 MB cached at core 1. DPDK will
/// be initialized as a primary process without any additional arguments. A
/// single network interface/port with 1 transmit queue, 1 receive queue, 256
/// receive descriptors, and 256 transmit descriptors will be made available to
/// Netbricks. Loopback, hardware transmit segementation offload, and hardware
/// checksum offload will be disabled on this port.
fn get_default_netbricks_config(config: &config::ServerConfig) -> NetbricksConfiguration {
    // General arguments supplied to netbricks.
    let net_config_name = String::from("server");
    let dpdk_secondary: bool = false;
    let net_primary_core: i32 = 19;
    let net_cores: Vec<i32> = vec![10, 11, 12, 13, 14, 15, 16, 17];
    let net_strict_cores: bool = true;
    let net_pool_size: u32 = 8192 - 1;
    let net_cache_size: u32 = 128;
    let net_dpdk_args: Option<String> = None;

    // Port configuration. Required to configure the physical network interface.
    let net_port_name = config.nic_pci.clone();
    let net_port_rx_queues: Vec<i32> = net_cores.clone();
    let net_port_tx_queues: Vec<i32> = net_cores.clone();
    let net_port_rxd: i32 = 256;
    let net_port_txd: i32 = 256;
    let net_port_loopback: bool = false;
    let net_port_tcp_tso: bool = false;
    let net_port_csum_offload: bool = false;

    let net_port_config = PortConfiguration {
        name: net_port_name,
        rx_queues: net_port_rx_queues,
        tx_queues: net_port_tx_queues,
        rxd: net_port_rxd,
        txd: net_port_txd,
        loopback: net_port_loopback,
        tso: net_port_tcp_tso,
        csum: net_port_csum_offload,
    };

    // The set of ports used by netbricks.
    let net_ports: Vec<PortConfiguration> = vec![net_port_config];

    NetbricksConfiguration {
        name: net_config_name,
        secondary: dpdk_secondary,
        primary_core: net_primary_core,
        cores: net_cores,
        strict: net_strict_cores,
        ports: net_ports,
        pool_size: net_pool_size,
        cache_size: net_cache_size,
        dpdk_args: net_dpdk_args,
    }
}

/// This function configures and initializes Netbricks. In the case of a
/// failure, it causes the program to exit.
///
/// Returns a Netbricks context which can be used to setup and start the
/// server/client.
fn config_and_init_netbricks(config: &config::ServerConfig) -> NetbricksContext {
    let net_config: NetbricksConfiguration = get_default_netbricks_config(config);

    // Initialize Netbricks and return a handle.
    match initialize_system(&net_config) {
        Ok(net_context) => {
            return net_context;
        }

        Err(ref err) => {
            error!("Error during Netbricks init: {}", err);
            // TODO: Drop NetbricksConfiguration?
            std::process::exit(1);
        }
    }
}

fn main() {
    // Basic setup and initialization.
    db::env_logger::init().expect("ERROR: failed to initialize logger!");

    let config = config::ServerConfig::load();
    info!("Starting up Sandstorm server with config {:?}", config);

    // Setup Netbricks.
    let mut net_context: NetbricksContext = config_and_init_netbricks(&config);

    // Setup the server pipeline.
    net_context.start_schedulers();
    net_context.add_pipeline_to_run(Arc::new(
        move |ports, scheduler: &mut StandaloneScheduler, core: i32, sibling| {
            setup_server(&config, ports, sibling, scheduler, core)
        },
    ));

    // Run the server, and give it some time to bootup.
    net_context.execute();
    sleep(Duration::from_millis(1000));

    // Stop the server.
    // net_context.stop();
    // _install.join();
}
