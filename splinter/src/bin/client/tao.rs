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

extern crate db;
extern crate rand;
extern crate splinter;
extern crate zipf;

mod setup;

use std::mem::{size_of, transmute};
use std::sync::Arc;

use db::config;
use db::cycles;
use db::e2d2::allocators::CacheAligned;
use db::e2d2::interface::PortQueue;
use db::e2d2::scheduler::*;
use db::log::*;
use db::rpc::parse_rpc_opcode;
use db::wireformat::*;

use rand::distributions::Sample;
use rand::{Rng, SeedableRng, XorShiftRng};

use zipf::ZipfDistribution;

use splinter::*;

// Flag to indicate that the client has finished sending and receiving the packets.
static mut FINISHED: bool = false;

// The max number of outstanding packet allowed at any time per thread.
static MAX_OUTSTANDING: u64 = 32;

/// This type implements the send and receive of a TAO client.
struct TaoSendRecv {
    /// Random number generator required to seed the Zipfian distribution.
    random: XorShiftRng,

    /// Zipfian distribution from which keys are drawn.
    k_dist: ZipfDistribution,

    /// Zipfian distribution from which tenants are drawn.
    t_dist: ZipfDistribution,

    /// Flag indicating whether requests should be native (true) or invocations (false).
    native: bool,

    /// The total number of requests to be sent out.
    requests: u64,

    /// The total number of requests that have been sent so far.
    sent: u64,

    /// The time (in cycles) at which the workload started generating requests.
    start: u64,

    // Time stamp in cycles at which measurement stopped.
    stop: u64,

    /// Network stack that can actually send an RPC over the network.
    sender: dispatch::Sender,

    /// Request buffer for a native obj_get operation. Helps reduce heap allocations.
    no_buff: Vec<u8>,

    /// Request buffer for a native assoc_get operation. Helps reduce heap allocations.
    na_buff: Vec<u8>,

    /// Request buffer for an `obj_get` invoke operation. Again, helps reduce heap allocations.
    io_buff: Vec<u8>,

    /// Request buffer for an `assoc_get` invoke operation. Again, helps reduce heap allocations.
    ia_buff: Vec<u8>,

    /// The percentage of operations that are assoc_gets. The rest are obj_gets.
    assoc_p: usize,

    /// The network stack required to receives RPC response packets from a network port.
    receiver: dispatch::Receiver<CacheAligned<PortQueue>>,

    /// Second receiver stack to receive multiget RPC response packets when operating in
    /// native mode.
    multi_rx: dispatch::Receiver<CacheAligned<PortQueue>>,

    /// The number of response packets to wait for before printing out statistics.
    responses: u64,

    /// The total number of responses received so far.
    recvd: u64,

    /// Vector of sampled request latencies. Required to calculate distributions once all responses
    /// have been received. This vector is for the obj_get RPC.
    o_latencies: Vec<u64>,

    /// Vector of sampled request latencies. Required to calculate distributions once all responses
    /// have been received. This vector is for the assoc_get RPC.
    a_latencies: Vec<u64>,

    /// Pre-allocated vector to hold assoc keys. Required for the native mode.
    assoc_keys: Vec<u8>,

    /// If true and native is false, then obj_gets are sent out as native gets.
    combine: bool,

    // Flag to indicate if the procedure is finished or not.
    finished: bool,

    // To keep the mapping between sent and received packets. The client doesn't want to send
    // more than 32(???) outstanding packets.
    outstanding: u64,
}

// Implementation of methods on TaoSendRecv.
impl TaoSendRecv {
    /// Constructs a `TaoSendRecv` that can be added to a Netbricks pipeline.
    ///
    /// # Arguments
    ///
    /// * `port`:      Network port over which responses will be received.
    /// * `resps`:     The number of responses to be received from the server.
    /// * `native`:    The flag to represent if the client sends/receives native requests/responses.
    /// * `send`:      Network port over which requests will be sent out.
    /// * `dst_ports`: The total number of UDP ports the server is listening on.
    /// * `config`:    Client configuration with Workload related (key and value length etc.) as
    ///                well as network related (Server and Client MAC address etc.) parameters.
    pub fn new(
        port: CacheAligned<PortQueue>,
        resps: u64,
        native: bool,
        send: CacheAligned<PortQueue>,
        dst_ports: u16,
        config: &config::ClientConfig,
    ) -> TaoSendRecv {
        // Allocate a vector for the obj_get invoke() RPC's payload. The payload consists of the
        // name of the extension, the table id (8 bytes) and the key length, an opcode.
        let len = "tao".as_bytes().len() + size_of::<u64>() + 8 + 1;
        let mut io_buff = Vec::with_capacity(len);

        // Pre-populate the extension name, table id, and opcode.
        io_buff.extend_from_slice("tao".as_bytes());
        io_buff.extend_from_slice(&unsafe { transmute::<u64, [u8; 8]>(1u64.to_le()) });
        io_buff.extend_from_slice(&unsafe { transmute::<u64, [u8; 8]>(0u64.to_le()) }); // placeholder for key
        io_buff.extend_from_slice(&[0u8]);
        io_buff.resize(len, 0);

        // Allocate a vector for the assoc_get invoke() RPC's payload. The payload consists of the
        // name of the extension, an opcode, the table id (8 bytes) and the key length.
        let len = "tao".as_bytes().len() + size_of::<u64>() + 18 + 1;
        let mut ia_buff = Vec::with_capacity(len);

        // Pre-populate the extension name, opcode, and table id.
        ia_buff.extend_from_slice("tao".as_bytes());
        ia_buff.extend_from_slice(&unsafe { transmute::<u64, [u8; 8]>(2u64.to_le()) });
        ia_buff.extend_from_slice(&[0; 18]); // placeholder for id1 = 8, assoc_type = 2, id2 = 8.
        ia_buff.extend_from_slice(&[4u8]);
        ia_buff.resize(len, 0);

        // Allocate and init a buffer into which keys for a native obj_get will be generated.
        let mut no_buff = Vec::with_capacity(8);
        no_buff.resize(8, 0);

        // Allocate and init a buffer into which keys for a native assoc_get will be generated.
        let mut na_buff = Vec::with_capacity(10);
        na_buff.resize(10, 0);

        // Pre-populate a vector for assoc keys.
        let mut a_keys = Vec::with_capacity(72);
        a_keys.resize(72, 0);

        TaoSendRecv {
            random: XorShiftRng::from_seed(rand::random::<[u32; 4]>()),
            k_dist: ZipfDistribution::new(config.n_keys, config.skew)
                .expect("Failed to init key generator."),
            t_dist: ZipfDistribution::new(config.num_tenants as usize, config.tenant_skew)
                .expect("Failed to init tenant generator."),
            native: native,
            requests: config.num_reqs as u64,
            sent: 0,
            start: cycles::rdtsc(),
            stop: 0,
            multi_rx: dispatch::Receiver::new(send.clone()),
            sender: dispatch::Sender::new(config, send, dst_ports),
            no_buff: no_buff,
            na_buff: na_buff,
            io_buff: io_buff,
            ia_buff: ia_buff,
            assoc_p: config.assocs_p,
            combine: config.combined,
            receiver: dispatch::Receiver::new(port),
            responses: resps,
            recvd: 0,
            o_latencies: Vec::with_capacity(2 * 1000 * 1000),
            a_latencies: Vec::with_capacity(2 * 1000 * 1000),
            assoc_keys: a_keys,
            finished: false,
            outstanding: 0,
        }
    }

    /// Samples distributions for a tenant id key, and opcode.
    ///
    /// # Return
    /// A 3-tupule consisting of a 4 byte tenant id, 4 byte key, and boolean. If the boolean is
    /// true, the op should be an obj_get.
    #[inline]
    fn sample(&mut self) -> (u32, [u8; 4], bool) {
        let t = self.t_dist.sample(&mut self.random) as u32;

        let k = self.k_dist.sample(&mut self.random) as u32;
        let k: [u8; 4] = unsafe { transmute(k.to_le()) };

        let o = self.random.gen::<u32>() % 100 >= self.assoc_p as u32;

        (t, k, o)
    }

    /// Generates an RPC request out on the network.
    ///
    /// # Arguments
    ///
    /// * `curr`: Timestamp to attach onto the RPC.
    #[inline]
    fn generate(&mut self, curr: u64) {
        let (t, k, o) = self.sample();

        match self.native {
            // Native request.
            true => match o {
                true => {
                    self.no_buff[0..size_of::<u32>()].copy_from_slice(&k);
                    self.sender.send_get(t, 1, &self.no_buff, curr);
                }

                false => {
                    self.na_buff[0..size_of::<u32>()].copy_from_slice(&k);
                    self.sender.send_get(t, 2, &self.na_buff, curr);
                }
            },

            // Invoke request. Add the key to the pre-populated payload.
            false => match o {
                true => match self.combine {
                    true => {
                        self.no_buff[0..size_of::<u32>()].copy_from_slice(&k);
                        self.sender.send_get(t, 1, &self.no_buff, curr);
                    }

                    false => {
                        self.io_buff[11..15].copy_from_slice(&k);
                        self.sender.send_invoke(t, 3, &self.io_buff, curr);
                    }
                },

                false => {
                    self.ia_buff[11..15].copy_from_slice(&k);
                    self.sender.send_invoke(t, 3, &self.ia_buff, curr);
                }
            },
        }
    }

    /// The function which intiate the sending of the requests.
    fn send(&mut self) {
        // Return if there are no more requests to generate.
        if self.requests <= self.sent {
            self.finished = true;
            return;
        }

        // Send when the outstanding packets are less than `MAX_OUTSTANDING`.
        while self.outstanding < MAX_OUTSTANDING {
            // Send out a request at the configured request rate.
            let curr = cycles::rdtsc();

            self.generate(curr);

            self.sent += 1;
            self.outstanding += 1;
        }
    }

    fn assoc_keys(&mut self, list: &[u8]) {
        // Remove Optype, version and key from the payload.
        let list = list.split_at(19).1;

        let mut left: u64 = 0;
        for (idx, e) in list[0..8].iter().enumerate() {
            left |= (*e as u64) << (idx << 3);
        }

        if left > 0 {
            left -= 1;
        }

        let left: [u8; 8] = unsafe { transmute(left) };

        let mut n = 0;
        for id in list.chunks(16) {
            let mut l = n * 18;
            let mut r = l + 8;
            self.assoc_keys[l..r].copy_from_slice(&left);

            l = r + 2;
            r = l + 8;
            self.assoc_keys[l..r].copy_from_slice(&id[0..8]);

            n += 1;
        }
    }

    // Receive packets in response to the requests generated from send() function.
    fn recv(&mut self) {
        // Free incoming packets if all responses have been received.
        if self.responses <= self.recvd && self.finished == true {
            // Free single operation responses.
            if let Some(mut resps) = self.receiver.recv_res() {
                while let Some(packet) = resps.pop() {
                    self.outstanding -= 1;
                    if self.native {
                        let p = packet.parse_header::<GetResponse>();
                        p.free_packet();
                    } else {
                        if self.combine && packet.get_payload().len() <= 50 {
                            let p = packet.parse_header::<GetResponse>();
                            p.free_packet();
                            continue;
                        }
                        let p = packet.parse_header::<InvokeResponse>();
                        p.free_packet();
                    }
                }
            }

            // Free multi-operation responses.
            if self.native {
                if let Some(mut resps) = self.multi_rx.recv_res() {
                    while let Some(packet) = resps.pop() {
                        self.outstanding -= 1;
                        let p = packet.parse_header::<MultiGetResponse>();
                        p.free_packet();
                    }
                }
            }
            return;
        }

        // Check for received packets. If any, then take latency measurements.
        if let Some(mut resps) = self.receiver.recv_res() {
            while let Some(packet) = resps.pop() {
                if self.native {
                    match parse_rpc_opcode(&packet) {
                        OpCode::SandstormCommitRpc => {
                            let p = packet.parse_header::<CommitResponse>();
                            let timestamp = p.get_header().common_header.stamp;
                            match p.get_header().common_header.status {
                                RpcStatus::StatusTxAbort => {
                                    info!("Abort");
                                }

                                RpcStatus::StatusOk => {
                                    self.recvd += 1;
                                    if self.recvd & 0xf == 0 {
                                        self.a_latencies.push(cycles::rdtsc() - timestamp);
                                    }
                                }

                                _ => {}
                            }
                            p.free_packet();
                            continue;
                        }

                        OpCode::SandstormGetRpc => {
                            let p = packet.parse_header::<GetResponse>();

                            // Response to obj_get.
                            if p.get_payload().len() < 67 {
                                self.recvd += 1;
                                self.outstanding -= 1;

                                if self.recvd & 0xf == 0 {
                                    self.o_latencies
                                        .push(cycles::rdtsc() - p.get_header().common_header.stamp);
                                }

                                p.free_packet();
                                continue;
                            }

                            self.assoc_keys(p.get_payload());

                            self.sender.send_multiget(
                                p.get_header().common_header.tenant,
                                2,
                                18,
                                4,
                                &self.assoc_keys,
                                p.get_header().common_header.stamp,
                            );
                            p.free_packet();
                        }

                        OpCode::SandstormMultiGetRpc => {
                            let p = packet.parse_header::<MultiGetResponse>();
                            match p.get_header().common_header.status {
                                RpcStatus::StatusOk => {
                                    self.sender.send_commit(
                                        p.get_header().common_header.tenant,
                                        2,
                                        p.get_payload(),
                                        p.get_header().common_header.stamp,
                                        18,
                                        22,
                                    );
                                }

                                _ => {}
                            }

                            self.outstanding -= 1;
                            p.free_packet();
                        }

                        _ => {
                            packet.free_packet();
                            info!("Something is wrong");
                        }
                    }
                } else {
                    self.recvd += 1;
                    self.outstanding -= 1;

                    if self.combine && packet.get_payload().len() <= 50 {
                        let p = packet.parse_header::<GetResponse>();
                        if self.recvd & 0xf == 0 {
                            self.o_latencies
                                .push(cycles::rdtsc() - p.get_header().common_header.stamp);
                        }
                        p.free_packet();
                        continue;
                    }

                    let p = packet.parse_header::<InvokeResponse>();
                    if self.recvd & 0xf == 0 {
                        if p.get_payload().len() < 67 {
                            self.o_latencies
                                .push(cycles::rdtsc() - p.get_header().common_header.stamp);
                        } else {
                            self.a_latencies
                                .push(cycles::rdtsc() - p.get_header().common_header.stamp);
                        }
                    }
                    p.free_packet();
                }
            }
        }

        // Print out measurements after all responses have been received.
        if self.responses <= self.recvd {
            self.stop = cycles::rdtsc();
            self.finished = true;
        }
    }
}

// Implementation of the Executable trait so that we can use Netbrick's DPDK bindings.
impl Executable for TaoSendRecv {
    fn execute(&mut self) {
        if self.finished == true {
            unsafe { FINISHED = true }
            return;
        }
        self.send();
        self.recv();
    }

    fn dependencies(&mut self) -> Vec<usize> {
        vec![]
    }
}

impl Drop for TaoSendRecv {
    /// Prints out the measured latency distribution and throughput.
    fn drop(&mut self) {
        self.o_latencies.sort();
        let o_median = self.o_latencies[self.o_latencies.len() / 2];
        let o_tail = self.o_latencies[(self.o_latencies.len() * 99) / 100];
        let o_mean = self.o_latencies.iter().sum::<u64>() as f64 / self.o_latencies.len() as f64;

        self.a_latencies.sort();
        let a_median = self.a_latencies[self.a_latencies.len() / 2];
        let a_tail = self.a_latencies[(self.a_latencies.len() * 99) / 100];
        let a_mean = self.a_latencies.iter().sum::<u64>() as f64 / self.a_latencies.len() as f64;

        println!(
            "AMean(ns) {} AMedian(ns): {} ATail(ns) {} OMean(ns) {} OMedian(ns): {} OTail(ns): {} Throughput(Kops/s): {}",
            cycles::to_seconds(a_mean as u64) * 1e9,
            cycles::to_seconds(a_median) * 1e9,
            cycles::to_seconds(a_tail) * 1e9,
            cycles::to_seconds(o_mean as u64) * 1e9,
            cycles::to_seconds(o_median) * 1e9,
            cycles::to_seconds(o_tail) * 1e9,
            self.recvd as f64 / cycles::to_seconds(self.stop - self.start)
        );
    }
}

/// Sets up TaoRecv by adding it to a Netbricks scheduler.
///
/// # Arguments
///
/// * `ports`:     Network port on which packets will be received.
/// * `scheduler`: Netbricks scheduler to which TaoRecv will be added.
/// * `native`:    Boolean indicating whether responses are for native (true) or invoke (false)
///                RPCs.
/// * `send`:      Network port on which packets will be sent.
/// * `config`:    Network related configuration such as the MAC and IP address.
fn setup_send_recv<S>(
    ports: Vec<CacheAligned<PortQueue>>,
    scheduler: &mut S,
    _core: i32,
    native: bool,
    send: Vec<CacheAligned<PortQueue>>,
    config: &config::ClientConfig,
) where
    S: Scheduler + Sized,
{
    if ports.len() != 1 {
        error!("Client should be configured with exactly 1 port!");
        std::process::exit(1);
    }

    // Add the receiver to a netbricks pipeline.
    match scheduler.add_task(TaoSendRecv::new(
        ports[0].clone(),
        32 * 1000 * 1000 as u64,
        native,
        send[0].clone(),
        config.server_udp_ports as u16,
        config,
    )) {
        Ok(_) => {
            info!(
                "Successfully added TaoRecv with tx, rx queue {:?}.",
                (send[0].txq(), ports[0].rxq())
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

    // Setup Netbricks.
    let mut net_context = setup::config_and_init_netbricks(&config);

    // Setup the client pipeline.
    net_context.start_schedulers();

    // The core id's which will run the sender and receiver threads.
    // XXX The following array heavily depend on the set of cores
    // configured in setup.rs
    let senders_receivers = [0, 1, 2, 3, 4, 5, 6, 7];
    assert!(senders_receivers.len() == 8);

    // Required by AggregateRecv.
    let native = !config.use_invoke;

    // Setup 8 senders, and receivers.
    for i in 0..8 {
        // First, retrieve a tx-rx queue pair from Netbricks
        let port = net_context
            .rx_queues
            .get(&senders_receivers[i])
            .expect("Failed to retrieve network port!")
            .clone();

        // Setup the receive side.
        net_context
            .add_pipeline_to_core(
                senders_receivers[i],
                Arc::new(
                    move |send, sched: &mut StandaloneScheduler, core: i32, _sibling| {
                        setup_send_recv(
                            port.clone(),
                            sched,
                            core,
                            native,
                            send,
                            &config::ClientConfig::load(),
                        )
                    },
                ),
            )
            .expect("Failed to initialize receive side.");
    }

    // Run the client.
    net_context.execute();

    // Sleep for an amount of time approximately equal to the estimated execution time, and then
    // shutdown the client.
    unsafe {
        while !FINISHED {
            std::thread::sleep(std::time::Duration::from_secs(2));
        }
    }

    std::thread::sleep(std::time::Duration::from_secs(10));

    // Stop the client.
    net_context.stop();
}
