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

#![feature(use_extern_macros)]

extern crate db;
extern crate rand;
extern crate splinter;
extern crate zipf;

mod setup;

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::mem::{size_of, transmute};
use std::sync::Arc;

use db::config;
use db::cyclecounter::CycleCounter;
use db::cycles;
use db::e2d2::allocators::CacheAligned;
use db::e2d2::interface::PortQueue;
use db::e2d2::scheduler::*;
use db::log::*;
use db::master::Master;
use db::rpc::parse_rpc_opcode;
use db::task::TaskState::*;
use db::wireformat::*;

use rand::distributions::Sample;
use rand::{SeedableRng, XorShiftRng};

use zipf::ZipfDistribution;

use splinter::manager::TaskManager;
use splinter::*;

// Type: 1, KeySize: 30, ValueSize:100
const RECORD_SIZE: usize = 131;

/// This type implements the send half of a client that issues back to back reads to a server and
/// aggregates the returned value into a single 64 bit integer.
struct AggregateSendRecv {
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

    /// Network stack that can actually send an RPC over the network.
    sender: Arc<dispatch::Sender>,

    /// Request buffer for a native get operation. Helps reduce heap allocations.
    n_buff: Vec<u8>,

    /// Request buffer for an invoke operation. Again, helps reduce heap allocations.
    i_buff: Vec<u8>,

    /// The network stack required to receives RPC response packets from a network port.
    receiver: dispatch::Receiver<CacheAligned<PortQueue>>,

    /// The number of response packets to wait for before printing out statistics.
    responses: u64,

    /// The total number of responses received so far.
    recvd: u64,

    /// Vector of sampled request latencies. Required to calculate distributions once all responses
    /// have been received.
    latencies: Vec<u64>,

    /// Number of keys to aggregate across. Required for the native case.
    num: u32,

    /// Order of the final polynomial to be computed.
    ord: u32,

    // To keep the mapping between sent and received packets. The client doesn't want to send
    // more than 32(XXX) outstanding packets.
    outstanding: u64,

    /// A ref counted pointer to a master service. The master service
    /// implements the primary interface to the database.
    master_service: Arc<Master>,

    // To keep a mapping between each packet and request parameters. This information will be used
    // when the server pushes back the extension.
    manager: RefCell<HashMap<u64, TaskManager>>,

    // Run-queue of tasks waiting to execute. Tasks on this queue have either yielded, or have been
    // recently enqueued and never run before.
    waiting: VecDeque<TaskManager>,

    // Number of tasks completed on the client, after server pushback. Wraps around
    // after each 1L such tasks.
    pushback_completed: u64,

    // Counts the number of CPU cycle spent on the task execution when client executes the
    // extensions on its end.
    cycle_counter: CycleCounter,
}

// Implementation of methods on AggregateSend.
impl AggregateSendRecv {
    /// Constructs an `AggregateSend` that can be added to a Netbricks pipeline.
    ///
    /// # Arguments
    ///
    /// * `config`:    Client configuration with Workload related (key and value length etc.) as
    ///                well as network related (Server and Client MAC address etc.) parameters.
    /// * `port`:      Network port over which requests will be sent out.
    /// * `reqs`:      The number of requests to be issued to the server.
    /// * `dst_ports`: The total number of UDP ports the server is listening on.
    /// * `num`:       Number of keys to aggregate across.
    /// * `ord`:       Order of the final polynomial to be computed.
    /// * `resps`:     The number of responses to wait for before calculating statistics.
    /// * `send`:      Network port on which packets will be recv.
    pub fn new(
        config: &config::ClientConfig,
        port: CacheAligned<PortQueue>,
        reqs: u64,
        dst_ports: u16,
        num: u32,
        ord: u32,
        resps: u64,
        send: CacheAligned<PortQueue>,
        masterservice: Arc<Master>,
    ) -> AggregateSendRecv {
        // Allocate a vector for the invoke() RPC's payload. The payload consists of the name of
        // the extension, the table id (8 bytes), the key length, the aggregate size, and the order.
        let len = "aggregate".as_bytes().len()
            + size_of::<u64>()
            + size_of::<u32>()
            + size_of::<u32>()
            + config.key_len;
        let mut i_buff = Vec::with_capacity(len);

        // Pre-populate the extension name and table id.
        i_buff.extend_from_slice("aggregate".as_bytes());
        i_buff.extend_from_slice(&unsafe { transmute::<u64, [u8; 8]>(1u64.to_le()) });
        i_buff.extend_from_slice(&unsafe { transmute::<u32, [u8; 4]>(num.to_le()) });
        i_buff.extend_from_slice(&unsafe { transmute::<u32, [u8; 4]>(ord.to_le()) });
        i_buff.resize(len, 0);

        // Allocate and init a buffer into which keys will be generated.
        let mut n_buff = Vec::with_capacity(config.key_len);
        n_buff.resize(config.key_len, 0);

        AggregateSendRecv {
            random: XorShiftRng::from_seed(rand::random::<[u32; 4]>()),
            k_dist: ZipfDistribution::new(config.n_keys, config.skew)
                .expect("Failed to init key generator."),
            t_dist: ZipfDistribution::new(config.num_tenants as usize, config.tenant_skew)
                .expect("Failed to init tenant generator."),
            native: !config.use_invoke,
            requests: reqs,
            sent: 0,
            start: cycles::rdtsc(),
            receiver: dispatch::Receiver::new(send),
            sender: Arc::new(dispatch::Sender::new(config, port, dst_ports)),
            n_buff: n_buff,
            i_buff: i_buff,
            responses: resps,
            recvd: 0,
            latencies: Vec::with_capacity(2 * 1000 * 1000),
            num: num,
            ord: ord,
            outstanding: 0,
            master_service: Arc::clone(&masterservice),
            manager: RefCell::new(HashMap::new()),
            waiting: VecDeque::new(),
            pushback_completed: 0,
            cycle_counter: CycleCounter::new(),
        }
    }

    /// Samples distributions for a tenant id and key.
    ///
    /// # Return
    /// A 2-tupule consisting of a 4 byte tenant id and 4 byte key.
    #[inline]
    fn sample(&mut self) -> (u32, [u8; 4]) {
        let t = self.t_dist.sample(&mut self.random) as u32;

        let k = self.k_dist.sample(&mut self.random) as u32;
        let k: [u8; 4] = unsafe { transmute(k.to_le()) };

        (t, k)
    }

    /// Generates an RPC request out on the network.
    ///
    /// # Arguments
    ///
    /// * `curr`: Timestamp to attach onto the RPC.
    #[inline]
    fn generate(&mut self, curr: u64) {
        let (t, k) = self.sample();

        match self.native {
            // Native get() request.
            true => {
                self.n_buff[0..size_of::<u32>()].copy_from_slice(&k);
                self.sender.send_get(t, 1, &self.n_buff, curr);
            }

            // Invoke request. Add the key to the pre-populated payload.
            false => {
                self.i_buff[25..29].copy_from_slice(&k);
                self.add_request(&self.i_buff, t, 9, curr);
                self.sender.send_invoke(t, 9, &self.i_buff, curr);
            }
        }
    }

    fn send(&mut self) {
        // Return if there are no more requests to generate.
        if self.requests <= self.sent {
            return;
        }

        // Send out a request at the configured request rate.
        while self.outstanding < 32 {
            let curr = cycles::rdtsc();
            self.generate(curr);
            self.sent += 1;
            self.outstanding += 1;
            if self.waiting.len() >= 100000 {
                let mut batch = 4;
                while batch > 0 {
                    self.execute_task();
                    batch -= 1;
                }
                break;
            }
        }
    }

    /// Aggregates the first byte across a list of values.
    ///
    /// # Arguments
    ///
    /// * `init`: Initial value to be used in the summation.
    /// * `vec`:  The byte array whose contents need to be summed up.
    fn aggregate(&self, init: u64, vec: &[u8]) -> u64 {
        let mut cols = Vec::new();

        // First collect the first byte of each value.
        for row in vec.chunks(100) {
            if row.len() != 100 {
                break;
            }

            cols.push(row[0]);
        }

        // Aggregate the collected set of bytes.
        let mut aggr = cols.iter().fold(init, |sum, e| sum + (*e as u64));

        for _mul in 1..self.ord {
            aggr *= aggr;
        }

        aggr
    }

    /// Prints out the measured latency distribution and throughput.
    fn measurements(&mut self) {
        let stop = cycles::rdtsc();

        self.latencies.sort();
        let median = self.latencies[self.latencies.len() / 2];
        let tail = self.latencies[(self.latencies.len() * 99) / 100];

        info!(
            "Median(ns): {} Tail(ns): {} Throughput: {}",
            cycles::to_seconds(median) * 1e9,
            cycles::to_seconds(tail) * 1e9,
            self.recvd as f64 / cycles::to_seconds(stop - self.start)
        );
    }

    fn recv(&mut self) {
        // Do nothing if all responses have been received.
        if self.responses <= self.recvd {
            return;
        }

        // Check for received packets. If any, then take latency measurements.
        if let Some(mut resps) = self.receiver.recv_res() {
            while let Some(packet) = resps.pop() {
                if self.native {
                    match parse_rpc_opcode(&packet) {
                        OpCode::SandstormGetRpc => {
                            let p = packet.parse_header::<GetResponse>();
                            self.sender.send_multiget(
                                p.get_header().common_header.tenant,
                                1,
                                30,
                                self.num,
                                p.get_payload(),
                                p.get_header().common_header.stamp,
                            );
                            p.free_packet();
                        }

                        OpCode::SandstormMultiGetRpc => {
                            self.recvd += 1;
                            self.outstanding -= 1;

                            let p = packet.parse_header::<MultiGetResponse>();
                            let _s = self.aggregate(0, p.get_payload());
                            if self.recvd & 0xf == 0 {
                                self.latencies
                                    .push(cycles::rdtsc() - p.get_header().common_header.stamp);
                            }
                            p.free_packet();
                        }

                        _ => {
                            packet.free_packet();
                            info!("Something is wrong");
                        }
                    }
                } else {
                    let p = packet.parse_header::<InvokeResponse>();
                    match p.get_header().common_header.status {
                        RpcStatus::StatusOk => {
                            self.recvd += 1;
                            self.outstanding -= 1;
                            if self.recvd & 0xf == 0 {
                                self.latencies
                                    .push(cycles::rdtsc() - p.get_header().common_header.stamp);
                            }
                            self.remove_request(p.get_header().common_header.stamp);
                        }

                        RpcStatus::StatusPushback => {
                            let records = p.get_payload();
                            let (key, record) = records.split_at(369); // 1B for type, 8B for key, and 12 * 30B for value
                            let hdr = &p.get_header();
                            let timestamp = hdr.common_header.stamp;

                            // Create task and run the generator.
                            match self.manager.borrow_mut().remove(&timestamp) {
                                Some(mut manager) => {
                                    manager.create_generator(Arc::clone(&self.sender));
                                    manager.update_rwset(key, 369, 8);
                                    manager.update_rwset(record, RECORD_SIZE, 30);
                                    self.waiting.push_back(manager);
                                }

                                None => {
                                    info!("No manager with {} timestamp", timestamp);
                                }
                            }
                            self.outstanding -= 1;
                        }

                        _ => {}
                    }

                    p.free_packet();
                }
            }
        }

        // Print out measurements after all responses have been received.
        if self.responses <= self.recvd {
            self.measurements();
        }
    }

    fn add_request(&self, req: &[u8], tenant: u32, name_length: u32, id: u64) {
        let req = TaskManager::new(
            Arc::clone(&self.master_service),
            &req,
            tenant,
            name_length,
            id,
        );
        match self.manager.borrow_mut().insert(id, req) {
            Some(_) => {
                info!("Already present in the Hashmap");
            }

            None => {}
        }
    }

    fn remove_request(&self, id: u64) {
        self.manager.borrow_mut().remove(&id);
    }

    fn execute_task(&mut self) {
        // Don't do anything after all responses have been received.
        if self.waiting.len() == 0 {
            return;
        }

        //Execute the pushed-back task.
        let manager = self.waiting.pop_front();
        if let Some(mut manager) = manager {
            let (taskstate, _time) = manager.execute_task();
            if taskstate == YIELDED {
                self.waiting.push_back(manager);
            } else if taskstate == WAITING {
                self.manager.borrow_mut().insert(manager.get_id(), manager);
            } else if taskstate == COMPLETED {
                self.recvd += 1;
                if cfg!(feature = "execution") {
                    self.cycle_counter.total_cycles(_time, 1);
                    self.pushback_completed += 1;
                    if self.pushback_completed == 1000000 {
                        info!(
                            "Completion time per extension {}",
                            self.cycle_counter.get_average()
                        );
                        self.pushback_completed = 0;
                    }
                }
            }
        }
    }
}

// Executable trait allowing AuthRecv to be scheduled by Netbricks.
impl Executable for AggregateSendRecv {
    // Called internally by Netbricks.
    fn execute(&mut self) {
        self.send();
        self.recv();
        self.execute_task();
    }

    fn dependencies(&mut self) -> Vec<usize> {
        vec![]
    }
}

/// Sets up AggregateSend by adding it to a Netbricks scheduler.
///
/// # Arguments
///
/// * `config`:    Network related configuration such as the MAC and IP address.
/// * `ports`:     Network port on which packets will be sent.
/// * `scheduler`: Netbricks scheduler to which AggregateSend will be added.
/// * `num`:       Number of keys aggregations are to be performed across.
/// * `ord`:       Order of the final polynomial to be computed.
fn setup_send_recv<S>(
    config: &config::ClientConfig,
    ports: Vec<CacheAligned<PortQueue>>,
    scheduler: &mut S,
    _core: i32,
    num: u32,
    ord: u32,
    send: Vec<CacheAligned<PortQueue>>,
    masterservice: Arc<Master>,
) where
    S: Scheduler + Sized,
{
    if ports.len() != 1 {
        error!("Client should be configured with exactly 1 port!");
        std::process::exit(1);
    }

    // Add the sender to a netbricks pipeline.
    match scheduler.add_task(AggregateSendRecv::new(
        config,
        ports[0].clone(),
        config.num_reqs as u64,
        config.server_udp_ports as u16,
        num,
        ord,
        32 * 1000 * 1000 as u64,
        send[0].clone(),
        masterservice,
    )) {
        Ok(_) => {
            info!(
                "Successfully added AggregateSend with tx queue {}.",
                ports[0].txq()
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

    // Based on the supplied client configuration, compute the amount of time it will take to send
    // out `num_reqs` requests at a rate of `req_rate` requests per second.
    let exec = config.num_reqs / config.req_rate;

    // Setup Netbricks.
    let mut net_context = setup::config_and_init_netbricks(&config);

    // Setup the client pipeline.
    net_context.start_schedulers();

    // The core id's which will run the sender and receiver threads.
    // XXX The following two arrays heavily depend on the set of cores
    // configured in setup.rs
    let senders_and_receivers = [0, 1, 2, 3, 4, 5, 6, 7];
    assert!(senders_and_receivers.len() == 8);

    // Aggregation size.
    let num = config.num_aggr;
    let ord = config.order;

    // Setup 8 senders and receivers.
    for i in 0..8 {
        // First, retrieve a tx-rx queue pair from Netbricks
        let port = net_context
            .rx_queues
            .get(&senders_and_receivers[i])
            .expect("Failed to retrieve network port!")
            .clone();

        let master_service = Arc::clone(&masterservice);
        // Setup the receive side.
        net_context
            .add_pipeline_to_core(
                senders_and_receivers[i],
                Arc::new(
                    move |send, sched: &mut StandaloneScheduler, core: i32, _sibling| {
                        setup_send_recv(
                            &config::ClientConfig::load(),
                            port.clone(),
                            sched,
                            core,
                            num,
                            ord,
                            send,
                            Arc::clone(&master_service),
                        )
                    },
                ),
            ).expect("Failed to initialize receive side.");
    }

    // Run the client.
    net_context.execute();

    // Sleep for an amount of time approximately equal to the estimated execution time, and then
    // shutdown the client.
    std::thread::sleep(std::time::Duration::from_secs(exec as u64 + 10));

    // Stop the client.
    net_context.stop();
}
