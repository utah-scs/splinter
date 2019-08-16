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

#[cfg(feature = "dispatch")]
use std::cell::RefCell;
use std::fmt::Display;
use std::net::Ipv4Addr;
use std::option::Option;
use std::str::FromStr;
use std::sync::Arc;

use super::config;
#[cfg(feature = "dispatch")]
use super::cyclecounter::CycleCounter;
use super::cycles;
use super::master::Master;
use super::rpc;
use super::rpc::*;
use super::sched::RoundRobin;
use super::service::Service;
use super::task::{Task, TaskPriority, TaskState};
use super::wireformat;

use super::e2d2::common::EmptyMetadata;
use super::e2d2::headers::*;
use super::e2d2::interface::*;

use sandstorm::common;

/// This flag enables or disables fast path for native requests.
/// Later, it will be set from the server.toml file probably.
pub const FAST_PATH: bool = false;

/// This is a thread local variable to count the number of occurrences
/// of cycle counting to average for 1 M events.
#[cfg(feature = "dispatch")]
thread_local!(static COUNTER: RefCell<u64> = RefCell::new(0));

/// This type stores the cycle counter variable for various parts in dispatch stage.
/// poll: Cycle counter for full polling stage.
/// rx_tx: Cycle counter for packets receive and transmit stage.
/// parse: Cycle counter for packet parsing stage.
/// dispatch: Cycle counter for generator and task creation stage.
#[cfg(feature = "dispatch")]
struct DispatchCounters {
    poll: CycleCounter,
    rx_tx: CycleCounter,
    parse: CycleCounter,
    dispatch: CycleCounter,
}

#[cfg(feature = "dispatch")]
impl DispatchCounters {
    /// Creates and return an object of DispatchCounters. This object is used for
    /// couting CPU cycles for various parts in dispatch stage.
    ///
    /// # Return
    ///
    /// New instance of DispatchCounters struct
    fn new() -> DispatchCounters {
        DispatchCounters {
            poll: CycleCounter::new(),
            rx_tx: CycleCounter::new(),
            parse: CycleCounter::new(),
            dispatch: CycleCounter::new(),
        }
    }
}

/// This type represents a requests-dispatcher in Sandstorm. When added to a
/// Netbricks scheduler, this dispatcher polls a network port for RPCs,
/// dispatches them to a service, and sends out responses on the same network
/// port.
pub struct Dispatch<T>
where
    T: PacketRx + PacketTx + Display + Clone + 'static,
{
    /// A ref counted pointer to a master service. The master service
    /// implements the primary interface to the database.
    master_service: Arc<Master>,

    /// A ref counted pointer to the scheduler on which to enqueue tasks,
    /// and from which to receive response packets to be sent back to clients.
    scheduler: Arc<RoundRobin>,

    /// The network port/interface on which this dispatcher receives and
    /// transmits RPC requests and responses on.
    network_port: T,

    /// The receive queue over which this dispatcher steals RPC requests from.
    sibling_port: T,

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

    /// The number of response packets that were sent out by the dispatcher in
    /// the last measurement interval.
    responses_sent: u64,

    /// An indicator of the start of the current measurement interval in cycles.
    measurement_start: u64,

    /// An indicator of the stop of the previous measurement interval in cycles.
    measurement_stop: u64,

    /// The current execution state of the Dispatch task. Can be INITIALIZED, YIELDED, or RUNNING.
    state: TaskState,

    /// The total time for which the Dispatch task has executed on the CPU in cycles.
    time: u64,

    /// The priority of the Dispatch task. Required by the scheduler to determine when to run the
    /// task again.
    priority: TaskPriority,

    /// Unique identifier for a Dispatch task. Currently required for measurement purposes.
    id: i32,

    /// The CPU cycle counter to count the number of cycles per event. Need to use start() and
    /// stop() a code block or function call to profile the events.
    #[cfg(feature = "dispatch")]
    cycle_counter: DispatchCounters,
}

impl<T> Dispatch<T>
where
    T: PacketRx + PacketTx + Display + Clone + 'static,
{
    /// This function creates and returns a requests-dispatcher which can be
    /// added to a Netbricks scheduler.
    ///
    /// # Arguments
    ///
    /// * `config`:   A configuration consisting of the IP address, UDP port etc.
    /// * `net_port`: A network port/interface on which packets will be
    ///               received and transmitted.
    /// * `sib_port`: A network port/interface on which packets will be stolen.
    /// * `master`:   A reference to a Master which will be used to construct tasks from received
    ///               packets.
    /// * `sched`:    A reference to a scheduler on which tasks will be enqueued.
    /// * `id`:       The identifier of the dispatcher.
    ///
    /// # Return
    ///
    /// A dispatcher of type ServerDispatch capable of receiving RPCs, and responding to them.
    pub fn new(
        config: &config::ServerConfig,
        net_port: T,
        sib_port: T,
        master: Arc<Master>,
        sched: Arc<RoundRobin>,
        id: i32,
    ) -> Dispatch<T> {
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

        Dispatch {
            master_service: master,
            scheduler: sched,
            network_port: net_port.clone(),
            sibling_port: sib_port.clone(),
            network_ip_addr: ip_src_addr,
            max_rx_packets: rx_batch_size,
            resp_udp_header: udp_header,
            resp_ip_header: ip_header,
            resp_mac_header: mac_header,
            responses_sent: 0,
            measurement_start: cycles::rdtsc(),
            measurement_stop: 0,
            state: TaskState::INITIALIZED,
            time: 0,
            priority: TaskPriority::DISPATCH,
            id: id,
            #[cfg(feature = "dispatch")]
            cycle_counter: DispatchCounters::new(),
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

    /// This function attempts to steal a batch of packets from the
    /// dispatcher's network port.
    ///
    /// # Return
    ///
    /// A vector of packets wrapped up in Netbrick's Packet<NullHeader, EmptyMetadata> type if
    /// there was anything received at the network port.
    fn try_steal_packets(&self) -> Option<Vec<Packet<NullHeader, EmptyMetadata>>> {
        // Allocate a vector of mutable MBuf pointers into which packets will
        // be received.
        let mut mbuf_vector = Vec::with_capacity(self.max_rx_packets as usize);

        // This unsafe block is needed in order to populate mbuf_vector with a
        // bunch of pointers, and subsequently manipulate these pointers. DPDK
        // will take care of assigning these to actual MBuf's.
        unsafe {
            mbuf_vector.set_len(self.max_rx_packets as usize);

            // Try to receive packets from the sibling.
            match self.sibling_port.recv(&mut mbuf_vector[..]) {
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

        // For every million packets sent out by the dispatcher, print out the
        // amount of time in nano seconds it took to do so.
        let every = 1000000;
        if self.responses_sent >= every {
            self.measurement_stop = cycles::rdtsc();

            debug!(
                "Dispatcher {}: {:.0} K/packets/s",
                self.id,
                (self.responses_sent as f64 / 1e3)
                    / ((self.measurement_stop - self.measurement_start) as f64
                        / (cycles::cycles_per_second() as f64))
            );

            self.measurement_start = self.measurement_stop;
            self.responses_sent = 0;
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
        // This vector will hold the set of packets that were for either an invalid service or
        // operation.
        let mut ignore_packets = Vec::with_capacity(self.max_rx_packets as usize);

        // This vector will hold response packets of native requests.
        // It is then appended to scheduler's 'responses' queue
        // so these reponses can be sent out next time dispatch task is run.
        let mut native_responses = Vec::new();

        while let Some(request) = requests.pop() {
            // Set the destination ip address on the response IP header.
            let ip = request.deparse_header(common::IP_HDR_LEN);
            self.resp_ip_header.set_src(ip.get_header().dst());
            self.resp_ip_header.set_dst(ip.get_header().src());

            // Set the destination mac address on the response MAC header.
            let mac = ip.deparse_header(common::MAC_HDR_LEN);
            self.resp_mac_header.set_src(mac.get_header().dst());
            self.resp_mac_header.set_dst(mac.get_header().src());

            let request = mac.parse_header::<IpHeader>().parse_header::<UdpHeader>();

            // Allocate a packet for the response upfront, and add in MAC, IP, and UDP headers.
            if let Some(mut response) = new_packet() {
                let mut response = response
                    .push_header(&self.resp_mac_header)
                    .expect("ERROR: Failed to add response MAC header")
                    .push_header(&self.resp_ip_header)
                    .expect("ERROR: Failed to add response IP header")
                    .push_header(&self.resp_udp_header)
                    .expect("ERROR: Failed to add response UDP header");

                // Set the destination port on the response UDP header.
                response
                    .get_mut_header()
                    .set_src_port(request.get_header().dst_port());
                response
                    .get_mut_header()
                    .set_dst_port(request.get_header().src_port());

                if parse_rpc_service(&request) == wireformat::Service::MasterService {
                    // The request is for Master, get it's opcode, and call into Master.
                    let opcode = parse_rpc_opcode(&request);
                    if !FAST_PATH {
                        match self.master_service.dispatch(opcode, request, response) {
                            Ok(task) => {
                                self.scheduler.enqueue(task);
                            }

                            Err((req, res)) => {
                                // Master returned an error. The allocated request and response packets
                                // need to be freed up.
                                ignore_packets.push(req);
                                ignore_packets.push(res);
                            }
                        }
                    } else {
                        match opcode {
                            wireformat::OpCode::SandstormInvokeRpc => {
                                // The request is for invoke. Dispatch RPC to its handler.
                                match self.master_service.dispatch_invoke(request, response) {
                                    Ok(task) => {
                                        self.scheduler.enqueue(task);
                                    }

                                    Err((req, res)) => {
                                        // Master returned an error. The allocated request and response packets
                                        // need to be freed up.
                                        ignore_packets.push(req);
                                        ignore_packets.push(res);
                                    }
                                }
                            }

                            wireformat::OpCode::SandstormGetRpc
                            | wireformat::OpCode::SandstormPutRpc
                            | wireformat::OpCode::SandstormMultiGetRpc => {
                                // The request is native. Service it right away.
                                match self
                                    .master_service
                                    .service_native(opcode, request, response)
                                {
                                    Ok((req, res)) => {
                                        // Free request packet.
                                        req.free_packet();

                                        // Push response packet on the local queue of responses that are ready to be sent out.
                                        native_responses.push(rpc::fixup_header_length_fields(res));
                                    }

                                    Err((req, res)) => {
                                        // Master returned an error. The allocated request and response packets
                                        // need to be freed up.
                                        ignore_packets.push(req);
                                        ignore_packets.push(res);
                                    }
                                }
                            }

                            _ => {
                                // The request is unknown.
                                ignore_packets.push(request);
                                ignore_packets.push(response);
                            }
                        }
                    }
                } else {
                    // The request is not for Master. The allocated request and response packets need
                    // to be freed up.
                    ignore_packets.push(request);
                    ignore_packets.push(response);
                }
            } else {
                println!("ERROR: Failed to allocate packet for response");
            }
        }

        // Enqueue completed native resps on to scheduler's responses queue
        self.scheduler.append_resps(&mut native_responses);

        // Free the set of ignored packets.
        self.free_packets(ignore_packets);
    }

    /// This method polls the dispatchers network port for any received packets,
    /// dispatches them to the appropriate service, and sends out responses over
    /// the network port.
    ///
    /// # Return
    ///
    /// The number of packets received.
    #[inline]
    fn poll(&mut self) -> u64 {
        // First, send any pending response packets out.
        #[cfg(feature = "dispatch")]
        self.cycle_counter.rx_tx.start();
        let responses = self.scheduler.responses();
        if responses.len() > 0 {
            self.try_send_packets(responses);
        }

        // Next, try to receive packets from the network.
        if let Some(packets) = self.try_receive_packets() {
            #[cfg(feature = "dispatch")]
            self.cycle_counter.rx_tx.stop(packets.len() as u64);

            // Perform basic network processing on the received packets.
            #[cfg(feature = "dispatch")]
            self.cycle_counter.parse.start();
            let mut packets = self.parse_mac_headers(packets);
            let mut packets = self.parse_ip_headers(packets);
            let mut packets = self.parse_udp_headers(packets);
            let count = packets.len();
            #[cfg(feature = "dispatch")]
            self.cycle_counter.parse.stop(count as u64);

            // Dispatch these packets to the appropriate service.
            #[cfg(feature = "dispatch")]
            self.cycle_counter.dispatch.start();
            self.dispatch_requests(packets);
            #[cfg(feature = "dispatch")]
            self.cycle_counter.dispatch.stop(count as u64);
            count as u64
        } else {
            let mut count = 0;
            // There were no packets at the receive queue. Try to steal some from the sibling.
            if let Some(stolen) = self.try_steal_packets() {
                // Perform basic network processing on the stolen packets.
                let mut stolen = self.parse_mac_headers(stolen);
                let mut stolen = self.parse_ip_headers(stolen);
                let mut stolen = self.parse_udp_headers(stolen);

                // Dispatch these packets to the appropriate service.
                count = stolen.len();
                self.dispatch_requests(stolen);
            }
            count as u64
        }
    }
}

// Implementation of the Task trait for Dispatch. This will allow Dispatch to be scheduled by the
// database.
impl<T> Task for Dispatch<T>
where
    T: PacketRx + PacketTx + Display + Clone + 'static,
{
    /// Refer to the `Task` trait for Documentation.
    fn run(&mut self) -> (TaskState, u64) {
        let start = cycles::rdtsc();

        // Run the dispatch task, polling for received packets and sending out pending responses.
        self.state = TaskState::RUNNING;
        let _count = self.poll();
        self.state = TaskState::YIELDED;

        // Update the time the task spent executing and return.
        let exec = cycles::rdtsc() - start;

        self.time += exec;
        #[cfg(feature = "dispatch")]
        self.cycle_counter.poll.total_cycles(exec, _count);

        #[cfg(feature = "dispatch")]
        COUNTER.with(|count_a| {
            let mut count = count_a.borrow_mut();
            if _count > 0 {
                *count += 1;
            }
            let every = 1000000;
            if *count >= every {
                info!(
                    "Poll {}, RX-TX {}, Parse {}, Dispatch {}",
                    self.cycle_counter.poll.get_average(),
                    self.cycle_counter.rx_tx.get_average(),
                    self.cycle_counter.parse.get_average(),
                    self.cycle_counter.dispatch.get_average()
                );
                *count = 0;
            }
        });
        return (self.state.clone(), exec);
    }

    /// Refer to the `Task` trait for Documentation.
    fn state(&self) -> TaskState {
        self.state.clone()
    }

    /// Refer to the `Task` trait for Documentation.
    fn time(&self) -> u64 {
        self.time.clone()
    }

    /// Refer to the `Task` trait for Documentation.
    fn db_time(&self) -> u64 {
        return 0;
    }

    /// Refer to the `Task` trait for Documentation.
    fn priority(&self) -> TaskPriority {
        self.priority.clone()
    }

    /// Refer to the `Task` trait for Documentation.
    unsafe fn tear(
        &mut self,
    ) -> Option<(
        Packet<UdpHeader, EmptyMetadata>,
        Packet<UdpHeader, EmptyMetadata>,
    )> {
        // The Dispatch task does not return any packets.
        None
    }

    /// Refer to the `Task` trait for Documentation.
    fn set_state(&mut self, state: TaskState) {
        self.state = state;
    }

    /// Refer to the `Task` trait for Documentation.
    fn update_cache(&mut self, _record: &[u8], _keylen: usize) {}

    /// Refer to the `Task` trait for Documentation.
    fn get_id(&self) -> u64 {
        0
    }
}

impl<T> Drop for Dispatch<T>
where
    T: PacketRx + PacketTx + Display + Clone + 'static,
{
    fn drop(&mut self) {
        let responses = self.scheduler.responses();
        if responses.len() > 0 {
            self.try_send_packets(responses);
        }
    }
}
