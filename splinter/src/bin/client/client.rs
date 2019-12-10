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
#![feature(generators, generator_trait)]

extern crate db;
extern crate rand;
extern crate sandstorm;
extern crate spin;
extern crate splinter;
extern crate time;
extern crate zipf;

mod setup;
mod ycsbt;

use std::cell::RefCell;
use std::fmt::Display;
use std::mem;
use std::mem::transmute;
use std::sync::Arc;

use db::config;
use db::cycles;
use db::e2d2::allocators::*;
use db::e2d2::common::EmptyMetadata;
use db::e2d2::headers::UdpHeader;
use db::e2d2::interface::*;
use db::e2d2::scheduler::*;
use db::log::*;
use db::master::Master;
use db::rpc::parse_rpc_opcode;
use db::wireformat::*;

use splinter::sched::TaskManager;
use splinter::workload::Workload;
use splinter::*;

// Flag to indicate that the client has finished sending and receiving the packets.
static mut FINISHED: bool = false;

// The maximum outstanding requests a client can generate; and maximum number of push-back tasks.
const MAX_CREDIT: usize = 32;

///
struct Client<T>
where
    T: PacketTx + PacketRx + Display + Clone + 'static,
{
    // The network stack required to receives RPC response packets from a network port.
    receiver: dispatch::Receiver<T>,

    // Network stack required to actually send RPC requests out the network.
    sender: Arc<dispatch::Sender>,

    //
    workload: Box<Workload>,

    // This parameter decides the type of load the client generates; closed loop or open loop load.
    open_load: bool,

    // If true, RPC requests corresponding to native get() and put() operations are sent out. If
    // false, invoke() based RPC requests are sent out.
    native: bool,

    // The inverse of the rate at which requests are to be generated. Basically, the time interval
    // between two request generations in cycles.
    rate_inv: u64,

    // The time stamp at which the next request must be issued in cycles.
    next: u64,

    //
    table_id: u64,

    // The number of response packets to wait for before printing out statistics.
    responses: u64,

    // Time stamp in cycles at which measurement started. Required to calculate observed
    // throughput of the Sandstorm server.
    start: u64,

    // The total number of responses received so far.
    recvd: u64,

    // The total number of aborted requests.
    aborted: u64,

    // Vector of sampled request latencies. Required to calculate distributions once all responses
    // have been received.
    latencies: Vec<u64>,

    // If true, this receiver will make latency measurements.
    master: bool,

    // Time stamp in cycles at which measurement stopped.
    stop: u64,

    // Total number of requests to be sent out.
    requests: u64,

    // Number of requests that have been sent out so far.
    sent: u64,

    // To keep the mapping between sent and received packets. The client doesn't want to send
    // more than 32(XXX) outstanding packets.
    outstanding: u64,

    // To keep a mapping between each packet and request parameters. This information will be used
    // when the server pushes back the extension.
    manager: RefCell<TaskManager>,

    // The length of the key.
    key_len: usize,

    // The length of the record.
    record_len: usize,

    // Needed for pushback aborted transactions.
    invoke_get_modify: Vec<u8>,
}

impl<T> Client<T>
where
    T: PacketTx + PacketRx + Display + Clone + 'static,
{
    ///
    fn new(
        rx_port: T,
        config: &config::ClientConfig,
        sender: Arc<dispatch::Sender>,
        workload: Box<Workload>,
        master: bool,
        masterservice: Arc<Master>,
        table_id: u64,
    ) -> Client<T> {
        let resps = 34 * 1000 * 1000;
        let order = config.order as u32;
        let payload_len = "ycsbt".as_bytes().len()
            + mem::size_of::<u64>()
            + config.key_len
            + config.key_len
            + mem::size_of::<u32>()
            + mem::size_of::<u8>();
        let mut invoke_get_modify: Vec<u8> = Vec::with_capacity(payload_len);
        invoke_get_modify.extend_from_slice("ycsbt".as_bytes());
        invoke_get_modify
            .extend_from_slice(&unsafe { transmute::<u64, [u8; 8]>(table_id.to_le()) });
        invoke_get_modify.extend_from_slice(&[0; 60]); // Placeholder for 2 keys
        invoke_get_modify.extend_from_slice(&unsafe { transmute::<u32, [u8; 4]>(order.to_le()) });
        invoke_get_modify.extend_from_slice(&[2]);
        invoke_get_modify.resize(payload_len, 0);

        Client {
            receiver: dispatch::Receiver::new(rx_port),
            sender: Arc::clone(&sender),
            workload: workload,
            open_load: config.open_load,
            native: !config.use_invoke,
            rate_inv: cycles::cycles_per_second() / config.req_rate as u64,
            next: 0,
            table_id: table_id,
            responses: resps,
            start: cycles::rdtsc(),
            recvd: 0,
            aborted: 0,
            latencies: Vec::with_capacity(resps as usize),
            master: master,
            stop: 0,
            requests: config.num_reqs as u64,
            sent: 0,
            outstanding: 0,
            manager: RefCell::new(TaskManager::new(Arc::clone(&masterservice))),
            key_len: config.key_len,
            record_len: config.key_len + config.value_len + 9,
            invoke_get_modify: invoke_get_modify,
        }
    }

    #[inline]
    fn send_native(&mut self) {
        let curr = cycles::rdtsc();
        match self.workload.next_optype() {
            OpCode::SandstormGetRpc => {
                let (tenant, key) = self.workload.get_get_request();
                self.sender.send_get(tenant, self.table_id, &key, curr);
                self.outstanding += 1;
                self.sent += 1;
            }

            OpCode::SandstormPutRpc => {
                let (tenant, key, val) = self.workload.get_put_request();
                self.sender
                    .send_put(tenant, self.table_id, &key, &val, curr);
                self.outstanding += 1;
                self.sent += 1;
            }

            OpCode::SandstormMultiGetRpc => {
                let (tenant, n_keys, keys) = self.workload.get_multiget_request();
                self.sender.send_multiget(
                    tenant,
                    self.table_id,
                    self.key_len as u16,
                    n_keys,
                    &keys,
                    curr,
                );
                self.outstanding += 1;
                self.sent += 1;
            }

            _ => {
                info!("Invalid RPC request");
            }
        }
    }

    #[inline]
    fn send_invoke(&mut self) {
        let curr = cycles::rdtsc();
        let name_length = self.workload.name_length();
        let (tenant, request) = self.workload.get_invoke_request();
        self.sender.send_invoke(tenant, name_length, &request, curr);
        self.manager.borrow_mut().create_task(
            curr,
            &request,
            tenant,
            name_length as usize,
            Arc::clone(&self.sender),
        );
        self.outstanding += 1;
        self.sent += 1;
    }

    fn send(&mut self) {
        if self.requests <= self.sent {
            return;
        }

        if self.open_load == true {
            let curr = cycles::rdtsc();
            if curr >= self.next || self.next == 0 {
                if self.native == true {
                    self.send_native();
                } else {
                    // For invoke() mode, limit the pushback task-queue length to 32 to avoid
                    // sharp increase in latency.
                    if self.manager.borrow().get_queue_len() < MAX_CREDIT {
                        self.send_invoke();
                    }
                }
                self.next = self.start + self.sent * self.rate_inv;
            }
        } else {
            while self.outstanding < MAX_CREDIT as u64 {
                if self.native == true {
                    self.send_native();
                } else {
                    // For invoke() mode, limit the pushback task-queue length to 32 to avoid
                    // sharp increase in latency.
                    if self.manager.borrow().get_queue_len() < MAX_CREDIT {
                        self.send_invoke();
                    }
                }
            }
        }
    }

    #[inline]
    fn recv_native(&mut self, _curr: u64, packet: Packet<UdpHeader, EmptyMetadata>) {
        //TODO: Make it generic for each type of client, maybe forward
        // packet to client specific code for some processing.
        match parse_rpc_opcode(&packet) {
            OpCode::SandstormGetRpc => {
                let p = packet.parse_header::<GetResponse>();
                match p.get_header().common_header.status {
                    RpcStatus::StatusOk => {
                        self.workload.process_get_response(&p);
                    }

                    _ => {
                        info!("Invalid native get() response");
                    }
                }
                p.free_packet();
            }

            OpCode::SandstormMultiGetRpc => {
                let p = packet.parse_header::<MultiGetResponse>();
                match p.get_header().common_header.status {
                    RpcStatus::StatusOk => {
                        self.workload.process_multiget_response(&p);
                    }

                    _ => {
                        info!("Invalid native Multiget() response");
                    }
                }
                p.free_packet();
            }

            _ => {
                packet.free_packet();
            }
        }
        self.outstanding -= 1;
    }

    #[inline]
    fn recv_invoke(&mut self, curr: u64, packet: Packet<UdpHeader, EmptyMetadata>) {
        match parse_rpc_opcode(&packet) {
            OpCode::SandstormInvokeRpc => {
                let p = packet.parse_header::<InvokeResponse>();
                let timestamp = p.get_header().common_header.stamp;
                match p.get_header().common_header.status {
                    // If the status is StatusOk then add the stamp to the latencies and
                    // free the packet.
                    RpcStatus::StatusOk => {
                        self.latencies.push(curr - timestamp);
                        self.manager.borrow_mut().delete_task(timestamp);
                        self.recvd += 1;
                        self.outstanding -= 1;
                    }

                    // If the status is StatusPushback then compelete the task, add the
                    // stamp to the latencies, and free the packet.
                    RpcStatus::StatusPushback => {
                        let records = p.get_payload();
                        self.manager.borrow_mut().update_rwset(
                            timestamp,
                            records,
                            self.record_len,
                            self.key_len,
                        );
                        self.outstanding -= 1;
                    }

                    RpcStatus::StatusTxAbort => {
                        self.aborted += 1;
                        self.sender.send_invoke(
                            p.get_header().common_header.tenant,
                            self.workload.name_length(),
                            p.get_payload(),
                            timestamp,
                        );
                    }

                    _ => {
                        info!("Not sure about the response");
                    }
                }
                p.free_packet();
            }

            // The response corresponds to a get() or put() RPC.
            // The opcode on the response identifies the RPC type.
            OpCode::SandstormGetRpc => {
                let p = packet.parse_header::<GetResponse>();
                let timestamp = p.get_header().common_header.stamp;
                self.manager.borrow_mut().update_rwset(
                    timestamp,
                    p.get_payload(),
                    self.record_len,
                    self.key_len,
                );
                p.free_packet();
            }

            _ => {
                packet.free_packet();
            }
        }
    }

    fn recv(&mut self) {
        if self.stop > 0 {
            return;
        }

        if let Some(mut packets) = self.receiver.recv_res() {
            let curr = cycles::rdtsc();
            while let Some(packet) = packets.pop() {
                match parse_rpc_opcode(&packet) {
                    OpCode::SandstormCommitRpc => {
                        let p = packet.parse_header::<CommitResponse>();
                        let timestamp = p.get_header().common_header.stamp;
                        let tenant = p.get_header().common_header.tenant;
                        match p.get_header().common_header.status {
                            RpcStatus::StatusTxAbort => {
                                if self.native == true {
                                    // For native commit response.
                                    if p.get_payload().len() == self.key_len {
                                        self.sender.send_get(
                                            tenant,
                                            self.table_id,
                                            p.get_payload(),
                                            timestamp,
                                        )
                                    } else {
                                        let n_keys: u32 =
                                            (p.get_payload().len() / self.key_len) as u32;
                                        self.sender.send_multiget(
                                            tenant,
                                            self.table_id,
                                            self.key_len as u16,
                                            n_keys,
                                            p.get_payload(),
                                            timestamp,
                                        );
                                    }
                                } else {
                                    // For Pushback commit response.
                                    let name_length = self.workload.name_length();
                                    let mut start_index = 13;
                                    let mut end_index = start_index + 4;
                                    let records = p.get_payload();
                                    for key in records.chunks(self.key_len) {
                                        self.invoke_get_modify[start_index..end_index]
                                            .copy_from_slice(&key[0..4]);
                                        start_index += 30;
                                        end_index += 30;
                                    }

                                    self.sender.send_invoke(
                                        tenant,
                                        name_length,
                                        &self.invoke_get_modify,
                                        timestamp,
                                    );
                                    self.manager.borrow_mut().create_task(
                                        timestamp,
                                        &self.invoke_get_modify,
                                        tenant,
                                        name_length as usize,
                                        Arc::clone(&self.sender),
                                    );
                                }
                                self.aborted += 1;
                                self.outstanding += 1;
                            }

                            RpcStatus::StatusOk => {
                                self.recvd += 1;
                                self.latencies.push(curr - timestamp);
                            }

                            _ => {}
                        }

                        p.free_packet();
                        continue;
                    }

                    _ => {}
                }

                // Process the packet when it is not a commit response.
                if self.native == true {
                    self.recv_native(curr, packet);
                } else {
                    self.recv_invoke(curr, packet);
                }
            }
        }

        // The moment all response packets have been received, set the value of the
        // stop timestamp so that throughput can be estimated later.
        if self.responses <= (self.recvd + self.aborted) {
            self.stop = cycles::rdtsc();
        }
    }
}

// Implementation of the `Drop` trait on PushbackRecv.
impl<T> Drop for Client<T>
where
    T: PacketTx + PacketRx + Display + Clone + 'static,
{
    fn drop(&mut self) {
        if self.stop == 0 {
            self.stop = cycles::rdtsc();
        }
        // Calculate & print the throughput for all client threads.
        println!(
            "Client Throughput {}",
            self.recvd as f64 / cycles::to_seconds(self.stop - self.start)
        );
        // Calculate & print the aborted for all client threads.
        println!(
            "Client Aborted {}",
            self.aborted as f64 / cycles::to_seconds(self.stop - self.start)
        );

        let (tenant, request) = self.workload.get_invoke_request();
        let mut vec = request.to_vec();
        vec[request.len() - 1] = 3;
        self.sender.send_invoke(tenant, 5, &vec, cycles::rdtsc());

        // Calculate & print median & tail latency only on the master thread.
        if self.master {
            self.latencies.sort();

            let m;
            let t = self.latencies[(self.latencies.len() * 99) / 100];
            match self.latencies.len() % 2 {
                0 => {
                    let n = self.latencies.len();
                    m = (self.latencies[n / 2] + self.latencies[(n / 2) + 1]) / 2;
                }

                _ => m = self.latencies[self.latencies.len() / 2],
            }

            println!(
                ">>> {} {}",
                cycles::to_seconds(m) * 1e9,
                cycles::to_seconds(t) * 1e9
            );
        }
    }
}

impl<T> Executable for Client<T>
where
    T: PacketTx + PacketRx + Display + Clone + 'static,
{
    // Called internally by Netbricks.
    fn execute(&mut self) {
        self.send();
        self.recv();

        if self.native == false && self.manager.borrow().get_queue_len() > 0 {
            for _i in 0..MAX_CREDIT {
                self.manager.borrow_mut().execute_task();
            }
        }

        if self.stop > 0 {
            unsafe { FINISHED = true }
            return;
        }
    }

    fn dependencies(&mut self) -> Vec<usize> {
        vec![]
    }
}

fn pick_client(
    table_id: u64,
    config: &config::ClientConfig,
    sender: Arc<dispatch::Sender>,
) -> Box<Workload> {
    match config.workload.as_str() {
        "YCSBT" => Box::new(ycsbt::YCSBT::new(table_id, config, Arc::clone(&sender))),
        _ => Box::new(ycsbt::YCSBT::new(table_id, config, Arc::clone(&sender))),
    }
}

fn setup_send_recv<S>(
    ports: Vec<CacheAligned<PortQueue>>,
    scheduler: &mut S,
    _core: i32,
    master: bool,
    config: &config::ClientConfig,
    masterservice: Arc<Master>,
) where
    S: Scheduler + Sized,
{
    if ports.len() != 1 {
        error!("Client should be configured with exactly 1 port!");
        std::process::exit(1);
    }

    let sender = Arc::new(dispatch::Sender::new(
        config,
        ports[0].clone(),
        config.server_udp_ports,
    ));

    let table_id = 1;
    let workload = pick_client(table_id, config, Arc::clone(&sender));

    // Add the receiver to a netbricks pipeline.
    match scheduler.add_task(Client::new(
        ports[0].clone(),
        config,
        sender,
        workload,
        master,
        masterservice,
        table_id,
    )) {
        Ok(_) => {
            info!(
                "Successfully added Client with rx-tx queue {}.",
                ports[0].rxq()
            );
        }

        Err(ref err) => {
            error!("Error while adding to Netbricks pipeline {}", err);
            std::process::exit(1);
        }
    }
}

fn main() {
    db::env_logger::init().expect("ERROR: failed to initialize logger!");

    let config = config::ClientConfig::load();
    info!("Starting up Sandstorm client with config {:?}", config);

    let masterservice = Arc::new(Master::new());

    // Create tenants with extensions.
    info!("Populating extension for {} tenants", config.num_tenants);
    for tenant in 1..(config.num_tenants + 1) {
        masterservice.load_test(tenant);
    }

    // Setup Netbricks.
    let mut net_context = setup::config_and_init_netbricks(&config);

    // Setup the client pipeline.
    net_context.start_schedulers();

    // The core id's which will run the sender and receiver threads.
    // XXX The following array heavily depend on the set of cores
    // configured in setup.rs
    let senders_receivers = [0, 1, 2, 3, 4, 5, 6, 7];
    assert!(senders_receivers.len() == 8);

    // Setup 8 senders, and receivers.
    for i in 0..8 {
        // First, retrieve a tx-rx queue pair from Netbricks
        let port = net_context
            .rx_queues
            .get(&senders_receivers[i])
            .expect("Failed to retrieve network port!")
            .clone();

        let mut master = false;
        if i == 0 {
            master = true;
        }

        let master_service = Arc::clone(&masterservice);
        // Setup the receive and transmit side.
        net_context
            .add_pipeline_to_core(
                senders_receivers[i],
                Arc::new(
                    move |_ports, sched: &mut StandaloneScheduler, core: i32, _sibling| {
                        setup_send_recv(
                            port.clone(),
                            sched,
                            core,
                            master,
                            &config::ClientConfig::load(),
                            Arc::clone(&master_service),
                        )
                    },
                ),
            ).expect("Failed to initialize receive/transmit side.");
    }

    // Allow the system to bootup fully.
    std::thread::sleep(std::time::Duration::from_secs(1));

    // Run the client.
    net_context.execute();

    // Sleep for an amount of time approximately equal to the estimated execution time, and then
    // shutdown the client.
    unsafe {
        while !FINISHED {
            std::thread::sleep(std::time::Duration::from_secs(2));
        }
    }

    // Stop the client.
    net_context.stop();
}
