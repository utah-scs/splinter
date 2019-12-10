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

extern crate db;
extern crate rand;
extern crate sandstorm;
extern crate spin;
extern crate splinter;
extern crate time;
extern crate zipf;

mod setup;

use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Display;
use std::mem;
use std::mem::transmute;
use std::sync::Arc;

use self::bytes::Bytes;
use db::config;
use db::cycles;
use db::e2d2::allocators::*;
use db::e2d2::interface::*;
use db::e2d2::scheduler::*;
use db::log::*;
use db::master::Master;
use db::rpc::*;
use db::wireformat::*;

use rand::distributions::{Normal, Sample};
use rand::{Rng, SeedableRng, XorShiftRng};
use splinter::nativestate::PushbackState;
use splinter::proxy::KV;
use splinter::sched::TaskManager;
use splinter::*;
use zipf::ZipfDistribution;

// Flag to indicate that the client has finished sending and receiving the packets.
static mut FINISHED: bool = false;

// Flag to indicate that the client can generate Compute Request based on some distribution.
static mut ORD_DIST: bool = false;
static ORDER: f64 = 2500.0;
static STD_DEV: f64 = 500.0;

// The maximum outstanding requests a client can generate; and maximum number of push-back tasks.
const MAX_CREDIT: usize = 32;

// PUSHBACK benchmark.
// The benchmark is created and parameterized with `new()`. Many threads
// share the same benchmark instance. Each thread can call `abc()` which
// runs the benchmark until another thread calls `stop()`. Each thread
// then returns their runtime and the number of gets and puts they have done.
// This benchmark doesn't care about how get/put are implemented; it takes
// function pointers to get/put on `new()` and just calls those as it runs.
//
// The tests below give an example of how to use it and how to aggregate the results.
pub struct Pushback {
    put_pct: usize,
    rng: Box<dyn Rng>,
    key_rng: Box<ZipfDistribution>,
    tenant_rng: Box<ZipfDistribution>,
    order_rng: Box<Normal>,
    key_buf: Vec<u8>,
    value_buf: Vec<u8>,
}

impl Pushback {
    // Create a new benchmark instance.
    //
    // # Arguments
    //  - key_len: Length of the keys to generate per get/put. Most bytes will be zero, since
    //             the benchmark poplates them from a random 32-bit value.
    //  - value_len: Length of the values to store per put. Always all zero bytes.
    //  - n_keys: Number of keys from which random keys are drawn.
    //  - put_pct: Number between 0 and 100 indicating percent of ops that are sets.
    //  - skew: Zipfian skew parameter. 0.99 is PUSHBACK default.
    //  - n_tenants: The number of tenants from which the tenant id is chosen.
    //  - tenant_skew: The skew in the Zipfian distribution from which tenant id's are drawn.
    // # Return
    //  A new instance of PUSHBACK that threads can call `abc()` on to run.
    fn new(
        key_len: usize,
        value_len: usize,
        n_keys: usize,
        put_pct: usize,
        skew: f64,
        n_tenants: u32,
        tenant_skew: f64,
    ) -> Pushback {
        let seed: [u32; 4] = rand::random::<[u32; 4]>();

        let mut key_buf: Vec<u8> = Vec::with_capacity(key_len);
        key_buf.resize(key_len, 0);
        let mut value_buf: Vec<u8> = Vec::with_capacity(value_len);
        value_buf.resize(value_len, 0);

        Pushback {
            put_pct: put_pct,
            rng: Box::new(XorShiftRng::from_seed(seed)),
            key_rng: Box::new(
                ZipfDistribution::new(n_keys, skew).expect("Couldn't create key RNG."),
            ),
            tenant_rng: Box::new(
                ZipfDistribution::new(n_tenants as usize, tenant_skew)
                    .expect("Couldn't create tenant RNG."),
            ),
            order_rng: Box::new(Normal::new(ORDER, STD_DEV)),
            key_buf: key_buf,
            value_buf: value_buf,
        }
    }

    // Run PUSHBACK A, B, or C (depending on `new()` parameters).
    // The calling thread will not return until `done()` is called on this `Pushback` instance.
    //
    // # Arguments
    //  - get: A function that fetches the data stored under a bytestring key of `self.key_len` bytes.
    //  - set: A function that stores the data stored under a bytestring key of `self.key_len` bytes
    //         with a bytestring value of `self.value_len` bytes.
    // # Return
    //  A three tuple consisting of the duration that this thread ran the benchmark, the
    //  number of gets it performed, and the number of puts it performed.
    pub fn abc<G, P, R>(&mut self, mut get: G, mut put: P) -> R
    where
        G: FnMut(u32, &[u8], u32) -> R,
        P: FnMut(u32, &[u8], &[u8], u32) -> R,
    {
        let is_get = (self.rng.gen::<u32>() % 100) >= self.put_pct as u32;

        // Sample a tenant.
        let t = self.tenant_rng.sample(&mut self.rng) as u32;

        // Sample a key, and convert into a little endian byte array.
        let k = self.key_rng.sample(&mut self.rng) as u32;
        let k: [u8; 4] = unsafe { transmute(k.to_le()) };
        self.key_buf[0..mem::size_of::<u32>()].copy_from_slice(&k);

        let o = self.order_rng.sample(&mut self.rng).abs() as u32;

        if is_get {
            get(t, self.key_buf.as_slice(), o)
        } else {
            put(t, self.key_buf.as_slice(), self.value_buf.as_slice(), o)
        }
    }
}

/// Receives responses to PUSHBACK requests sent out by PushbackSend.
struct PushbackRecvSend<T>
where
    T: PacketTx + PacketRx + Display + Clone + 'static,
{
    // The network stack required to receives RPC response packets from a network port.
    receiver: dispatch::Receiver<T>,

    // The number of response packets to wait for before printing out statistics.
    responses: u64,

    // Time stamp in cycles at which measurement started. Required to calculate observed
    // throughput of the Sandstorm server.
    start: u64,

    // The total number of responses received so far.
    recvd: u64,

    // Vector of sampled request latencies. Required to calculate distributions once all responses
    // have been received.
    latencies: Vec<u64>,

    // If true, this receiver will make latency measurements.
    master: bool,

    // Time stamp in cycles at which measurement stopped.
    stop: u64,

    // The actual PUSHBACK workload. Required to generate keys and values for get() and put() requests.
    workload: RefCell<Pushback>,

    // Network stack required to actually send RPC requests out the network.
    sender: Arc<dispatch::Sender>,

    // Total number of requests to be sent out.
    requests: u64,

    // Number of requests that have been sent out so far.
    sent: u64,

    // If true, RPC requests corresponding to native get() and put() operations are sent out. If
    // false, invoke() based RPC requests are sent out.
    native: bool,

    // Payload for an invoke() based get operation. Required in order to avoid making intermediate
    // copies of the extension name, table id, and key.
    payload_pushback: RefCell<Vec<u8>>,

    // Payload for an invoke() based put operation. Required in order to avoid making intermediate
    // copies of the extension name, table id, key length, key, and value.
    payload_put: RefCell<Vec<u8>>,

    // Flag to indicate if the procedure is finished or not.
    finished: bool,

    // To keep the mapping between sent and received packets. The client doesn't want to send
    // more than 32(XXX) outstanding packets.
    outstanding: u64,

    // To keep a mapping between each packet and request parameters. This information will be used
    // when the server pushes back the extension.
    manager: RefCell<TaskManager>,

    // Keeps track of the state of a multi-operation request. For example, an extension performs
    // four get operations before performing aggregation and all these get operations are dependent
    // on the previous value.
    native_state: RefCell<HashMap<u64, PushbackState>>,

    /// Number of keys to aggregate across. Required for the native case.
    num: u32,

    /// Order of the final polynomial to be computed.
    ord: u32,

    // The length of the key.
    key_len: usize,

    // The length of the record.
    record_len: usize,
}

// Implementation of methods on PushbackRecv.
impl<T> PushbackRecvSend<T>
where
    T: PacketTx + PacketRx + Display + Clone + 'static,
{
    /// Constructs a PushbackRecv.
    ///
    /// # Arguments
    ///
    /// * `port` :  Network port on which responses will be polled for.
    /// * `resps`:  The number of responses to wait for before calculating statistics.
    /// * `master`: Boolean indicating if the receiver should make latency measurements.
    /// * `native`: If true, responses will be considered to correspond to native gets and puts.
    ///
    /// # Return
    ///
    /// A PUSHBACK response receiver that measures the median latency and throughput of a Sandstorm
    /// server.
    fn new(
        rx_port: T,
        resps: u64,
        master: bool,
        config: &config::ClientConfig,
        tx_port: CacheAligned<PortQueue>,
        reqs: u64,
        dst_ports: u16,
        masterservice: Arc<Master>,
        number: u32,
        order: u32,
    ) -> PushbackRecvSend<T> {
        // The payload on an invoke() based get request consists of the extensions name ("pushback"),
        // the table id to perform the lookup on, number of get(), number of CPU cycles and the key to lookup.
        let payload_len = "pushback".as_bytes().len()
            + mem::size_of::<u64>()
            + mem::size_of::<u32>()
            + mem::size_of::<u32>()
            + config.key_len;
        let mut payload_pushback = Vec::with_capacity(payload_len);
        payload_pushback.extend_from_slice("pushback".as_bytes());
        payload_pushback.extend_from_slice(&unsafe { transmute::<u64, [u8; 8]>(1u64.to_le()) });
        payload_pushback.extend_from_slice(&unsafe { transmute::<u32, [u8; 4]>(number.to_le()) });
        payload_pushback.extend_from_slice(&unsafe { transmute::<u32, [u8; 4]>(order.to_le()) });
        payload_pushback.resize(payload_len, 0);

        // The payload on an invoke() based put request consists of the extensions name ("put"),
        // the table id to perform the lookup on, the length of the key to lookup, the key, and the
        // value to be inserted into the database.
        let payload_len = "pushback".as_bytes().len()
            + mem::size_of::<u64>()
            + mem::size_of::<u16>()
            + config.key_len
            + config.value_len;
        let mut payload_put = Vec::with_capacity(payload_len);
        payload_put.extend_from_slice("pushback".as_bytes());
        payload_put.extend_from_slice(&unsafe { transmute::<u64, [u8; 8]>(1u64.to_le()) });
        payload_put.extend_from_slice(&unsafe {
            transmute::<u16, [u8; 2]>((config.key_len as u16).to_le())
        });
        payload_put.resize(payload_len, 0);
        PushbackRecvSend {
            receiver: dispatch::Receiver::new(rx_port),
            responses: resps,
            start: cycles::rdtsc(),
            recvd: 0,
            latencies: Vec::with_capacity(resps as usize),
            master: master,
            stop: 0,
            workload: RefCell::new(Pushback::new(
                config.key_len,
                config.value_len,
                config.n_keys - config.num_aggr as usize,
                0, //config.put_pct,
                config.skew,
                config.num_tenants,
                config.tenant_skew,
            )),
            sender: Arc::new(dispatch::Sender::new(config, tx_port, dst_ports)),
            requests: reqs,
            sent: 0,
            native: !config.use_invoke,
            payload_pushback: RefCell::new(payload_pushback),
            payload_put: RefCell::new(payload_put),
            finished: false,
            outstanding: 0,
            manager: RefCell::new(TaskManager::new(Arc::clone(&masterservice))),
            native_state: RefCell::new(HashMap::with_capacity(32)),
            num: number,
            ord: order,
            key_len: config.key_len,
            record_len: 1 + 8 + config.key_len + config.value_len,
        }
    }

    fn send(&mut self) {
        // Return if there are no more requests to generate.
        if self.requests <= self.sent {
            return;
        }

        while self.outstanding < MAX_CREDIT as u64
            && self.manager.borrow().get_queue_len() < MAX_CREDIT
        {
            // Get the current time stamp so that we can determine if it is time to issue the next RPC.
            let curr = cycles::rdtsc();

            if self.native == true {
                // Configured to issue native RPCs, issue a regular get()/put() operation.
                self.workload.borrow_mut().abc(
                    |tenant, key, _ord| self.sender.send_get(tenant, 1, key, curr),
                    |tenant, key, val, _ord| self.sender.send_put(tenant, 1, key, val, curr),
                );
                self.native_state
                    .borrow_mut()
                    .insert(curr, PushbackState::new(self.num, self.record_len as usize));
                self.outstanding += 1;
            } else {
                // Configured to issue invoke() RPCs.
                let mut p_get = self.payload_pushback.borrow_mut();
                let mut p_put = self.payload_put.borrow_mut();

                // XXX Heavily dependent on how `Pushback` creates a key. Only the first four
                // bytes of the key matter, the rest are zero. The value is always zero.
                self.workload.borrow_mut().abc(
                    |tenant, key, ord| {
                        unsafe {
                            if ORD_DIST {
                                p_get[20..24]
                                    .copy_from_slice(&{ transmute::<u32, [u8; 4]>(ord.to_le()) });
                            }
                        }
                        // First 24 bytes on the payload were already pre-populated with the
                        // extension name (8 bytes), the table id (8 bytes), number of get()
                        // (4 bytes), and number of CPU cycles compute(4 bytes). Just write
                        // in the first 4 bytes of the key.
                        p_get[24..28].copy_from_slice(&key[0..4]);
                        self.manager.borrow_mut().create_task(
                            curr,
                            &p_get,
                            tenant,
                            8,
                            Arc::clone(&self.sender),
                        );
                        self.sender.send_invoke(tenant, 8, &p_get, curr)
                    },
                    |tenant, key, _val, _ord| {
                        // First 18 bytes on the payload were already pre-populated with the
                        // extension name (8 bytes), the table id (8 bytes), and the key length (2
                        // bytes). Just write in the first 4 bytes of the key. The value is anyway
                        // always zero.
                        p_put[18..22].copy_from_slice(&key[0..4]);
                        self.manager.borrow_mut().create_task(
                            curr,
                            &p_put,
                            tenant,
                            8,
                            Arc::clone(&self.sender),
                        );
                        self.sender.send_invoke(tenant, 8, &p_put, curr)
                    },
                );
                self.outstanding += 1;
            }

            // Update the time stamp at which the next request should be generated, assuming that
            // the first request was sent out at self.start.
            self.sent += 1;
        }
    }

    fn recv(&mut self) {
        // Don't do anything after all responses have been received.
        if self.finished == true && self.stop > 0 {
            return;
        }

        // Try to receive packets from the network port.
        // If there are packets, sample the latency of the server.
        if let Some(mut packets) = self.receiver.recv_res() {
            let curr = cycles::rdtsc();
            while let Some(packet) = packets.pop() {
                // Process the commit response and continue.
                match parse_rpc_opcode(&packet) {
                    OpCode::SandstormCommitRpc => {
                        let p = packet.parse_header::<CommitResponse>();
                        let timestamp = p.get_header().common_header.stamp;
                        match p.get_header().common_header.status {
                            RpcStatus::StatusTxAbort => {
                                info!("Abort");
                            }

                            RpcStatus::StatusOk => {
                                self.latencies.push(curr - timestamp);
                            }

                            _ => {}
                        }
                        self.recvd += 1;
                        p.free_packet();
                        if self.native == true {
                            self.native_state.borrow_mut().remove(&timestamp);
                        }
                        continue;
                    }

                    _ => {}
                }

                if self.native == false {
                    match parse_rpc_opcode(&packet) {
                        // The response corresponds to an invoke() RPC.
                        OpCode::SandstormInvokeRpc => {
                            let p = packet.parse_header::<InvokeResponse>();
                            match p.get_header().common_header.status {
                                // If the status is StatusOk then add the stamp to the latencies and
                                // free the packet.
                                RpcStatus::StatusOk => {
                                    self.recvd += 1;
                                    self.latencies
                                        .push(curr - p.get_header().common_header.stamp);
                                    self.outstanding -= 1;
                                    self.manager
                                        .borrow_mut()
                                        .delete_task(p.get_header().common_header.stamp);
                                }

                                // If the status is StatusPushback then compelete the task, add the
                                // stamp to the latencies, and free the packet.
                                RpcStatus::StatusPushback => {
                                    let records = p.get_payload();
                                    let hdr = &p.get_header();
                                    let timestamp = hdr.common_header.stamp;

                                    // Unblock the task and put it in the ready queue.
                                    self.manager.borrow_mut().update_rwset(
                                        timestamp,
                                        records,
                                        self.record_len,
                                        self.key_len,
                                    );
                                    self.outstanding -= 1;
                                }

                                _ => {}
                            }
                            p.free_packet();
                        }

                        // The response corresponds to a get() or put() RPC.
                        // The opcode on the response identifies the RPC type.
                        OpCode::SandstormGetRpc => {
                            let p = packet.parse_header::<GetResponse>();
                            self.latencies
                                .push(curr - p.get_header().common_header.stamp);
                            unsafe {
                                if self
                                    .manager
                                    .borrow()
                                    .contains_key(&p.get_header().common_header.stamp)
                                {
                                    let manager = self
                                        .manager
                                        .borrow_mut()
                                        .remove(&p.get_header().common_header.stamp);
                                    if let Some(mut manager) = manager {
                                        manager.update_rwset(p.get_payload(), RECORD_SIZE, 30);
                                        self.waiting.push_back(manager);
                                    }
                                }
                            }
                            p.free_packet();
                        }

                        OpCode::SandstormPutRpc => {
                            let p = packet.parse_header::<PutResponse>();
                            self.latencies
                                .push(curr - p.get_header().common_header.stamp);
                            p.free_packet();
                        }

                        _ => packet.free_packet(),
                    }
                } else {
                    //The extension is executed locally on the client side.
                    match parse_rpc_opcode(&packet) {
                        OpCode::SandstormGetRpc => {
                            let p = packet.parse_header::<GetResponse>();
                            let timestamp = p.get_header().common_header.stamp;
                            let tenant = p.get_header().common_header.tenant;
                            let val_len = self.record_len - self.key_len - 9;
                            if let Some(ref mut state) =
                                self.native_state.borrow_mut().get_mut(&timestamp)
                            {
                                match p.get_header().common_header.status {
                                    RpcStatus::StatusOk => {
                                        let record = p.get_payload();
                                        state.update_rwset(&record, self.key_len);
                                        if state.op_num == self.num as u8 {
                                            /*let start = cycles::rdtsc();
                                            while cycles::rdtsc() - start < self.ord as u64 {}*/
                                            state.execute_task(self.num, self.ord);
                                            self.sender.send_commit(
                                                tenant,
                                                1,
                                                &state.commit_payload,
                                                timestamp,
                                                self.key_len as u16,
                                                val_len as u16,
                                            );
                                            self.outstanding -= 1;
                                        } else {
                                            match parse_record_optype(record) {
                                                OpType::SandstormRead => {
                                                    // Send the packet with same tenantid, curr etc.
                                                    let record = record.split_at(1).1;
                                                    let (version, entry) = record.split_at(8);
                                                    let (key, val) = entry.split_at(self.key_len);
                                                    // Deserializing is required in actual client;
                                                    // even we are doing the same for Pushback.
                                                    let kv = KV::new(
                                                        Bytes::from(version),
                                                        Bytes::from(key),
                                                        Bytes::from(val),
                                                    );
                                                    self.sender.send_get(
                                                        tenant,
                                                        1,
                                                        &kv.value[0..30],
                                                        timestamp,
                                                    );
                                                    state.op_num += 1;
                                                }
                                                _ => {
                                                    info!("The record is expected to be SandstormRead type");
                                                }
                                            }
                                        }
                                    }

                                    _ => {}
                                }
                            }
                            p.free_packet();
                        }

                        _ => packet.free_packet(),
                    }
                }
            }
        }

        // The moment all response packets have been received, set the value of the
        // stop timestamp so that throughput can be estimated later.
        if self.responses <= self.recvd {
            self.stop = cycles::rdtsc();
            self.finished = true;
        }
    }
}

// Implementation of the `Drop` trait on PushbackRecv.
impl<T> Drop for PushbackRecvSend<T>
where
    T: PacketTx + PacketRx + Display + Clone + 'static,
{
    fn drop(&mut self) {
        if self.stop == 0 {
            self.stop = cycles::rdtsc();
            info!("The client thread received only {} packets", self.recvd);
        }

        // Calculate & print the throughput for all client threads.
        println!(
            "PUSHBACK Throughput {}",
            self.recvd as f64 / cycles::to_seconds(self.stop - self.start)
        );

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

// Executable trait allowing PushbackRecv to be scheduled by Netbricks.
impl<T> Executable for PushbackRecvSend<T>
where
    T: PacketTx + PacketRx + Display + Clone + 'static,
{
    // Called internally by Netbricks.
    fn execute(&mut self) {
        self.send();
        self.recv();
        for _i in 0..MAX_CREDIT {
            self.manager.borrow_mut().execute_task();
        }
        if self.finished == true {
            unsafe { FINISHED = true }
            return;
        }
    }

    fn dependencies(&mut self) -> Vec<usize> {
        vec![]
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

    // Pushback compute size.
    let num = config.num_aggr as u32;
    let ord = config.order as u32;

    // Add the receiver to a netbricks pipeline.
    match scheduler.add_task(PushbackRecvSend::new(
        ports[0].clone(),
        34 * 1000 * 1000 as u64,
        master,
        config,
        ports[0].clone(),
        config.num_reqs as u64,
        config.server_udp_ports as u16,
        masterservice,
        num,
        ord,
    )) {
        Ok(_) => {
            info!(
                "Successfully added PushbackRecvSend with rx-tx queue {}.",
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
    std::thread::sleep(std::time::Duration::from_secs(100));

    // Stop the client.
    net_context.stop();
}

#[cfg(test)]
mod test {
    use std;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::{Duration, Instant};

    #[test]
    fn pushback_abc_basic() {
        let n_threads = 1;
        let mut threads = Vec::with_capacity(n_threads);
        let done = Arc::new(AtomicBool::new(false));

        for _ in 0..n_threads {
            let done = done.clone();
            threads.push(thread::spawn(move || {
                let mut b = super::Pushback::new(10, 100, 1000000, 5, 0.99, 1024, 0.1);
                let mut n_gets = 0u64;
                let mut n_puts = 0u64;
                let start = Instant::now();
                while !done.load(Ordering::Relaxed) {
                    b.abc(
                        |_t, _key, _ord| n_gets += 1,
                        |_t, _key, _value, _ord| n_puts += 1,
                    );
                }
                (start.elapsed(), n_gets, n_puts)
            }));
        }

        thread::sleep(Duration::from_secs(2));
        done.store(true, Ordering::Relaxed);

        // Iterate across all threads. Return a tupule whose first member consists
        // of the highest execution time across all threads, and whose second member
        // is the sum of the number of iterations run on each benchmark thread.
        // Dividing the second member by the first, will yeild the throughput.
        let (duration, n_gets, n_puts) = threads
            .into_iter()
            .map(|t| t.join().expect("ERROR: Thread join failed."))
            .fold(
                (Duration::new(0, 0), 0, 0),
                |(ldur, lgets, lputs), (rdur, rgets, rputs)| {
                    (std::cmp::max(ldur, rdur), lgets + rgets, lputs + rputs)
                },
            );

        let secs = duration.as_secs() as f64 + (duration.subsec_nanos() as f64 / 1e9);
        println!(
            "{} threads: {:.0} gets/s {:.0} puts/s {:.0} ops/s",
            n_threads,
            n_gets as f64 / secs,
            n_puts as f64 / secs,
            (n_gets + n_puts) as f64 / secs
        );
    }

    // Convert a key to u32 assuming little endian.
    fn convert_key(key: &[u8]) -> u32 {
        assert_eq!(4, key.len());
        let k: u32 = 0
            | key[0] as u32
            | (key[1] as u32) << 8
            | (key[2] as u32) << 16
            | (key[3] as u32) << 24;
        k
    }

    #[test]
    fn pushback_abc_histogram() {
        let hist = Arc::new(Mutex::new(HashMap::new()));

        let n_keys = 20;
        let n_threads = 1;

        let mut threads = Vec::with_capacity(n_threads);
        let done = Arc::new(AtomicBool::new(false));
        for _ in 0..n_threads {
            let hist = hist.clone();
            let done = done.clone();
            threads.push(thread::spawn(move || {
                let mut b = super::Pushback::new(4, 100, n_keys, 5, 0.99, 1024, 0.1);
                let mut n_gets = 0u64;
                let mut n_puts = 0u64;
                let start = Instant::now();
                while !done.load(Ordering::Relaxed) {
                    b.abc(
                        |_t, key, _ord| {
                            // get
                            let k = convert_key(key);
                            let mut ht = hist.lock().unwrap();
                            ht.entry(k).or_insert((0, 0)).0 += 1;
                            n_gets += 1
                        },
                        |_t, key, _value, _ord| {
                            // put
                            let k = convert_key(key);
                            let mut ht = hist.lock().unwrap();
                            ht.entry(k).or_insert((0, 0)).1 += 1;
                            n_puts += 1
                        },
                    );
                }
                (start.elapsed(), n_gets, n_puts)
            }));
        }

        thread::sleep(Duration::from_secs(2));
        done.store(true, Ordering::Relaxed);

        // Iterate across all threads. Return a tupule whose first member consists
        // of the highest execution time across all threads, and whose second member
        // is the sum of the number of iterations run on each benchmark thread.
        // Dividing the second member by the first, will yeild the throughput.
        let (duration, n_gets, n_puts) = threads
            .into_iter()
            .map(|t| t.join().expect("ERROR: Thread join failed."))
            .fold(
                (Duration::new(0, 0), 0, 0),
                |(ldur, lgets, lputs), (rdur, rgets, rputs)| {
                    (std::cmp::max(ldur, rdur), lgets + rgets, lputs + rputs)
                },
            );

        let secs = duration.as_secs() as f64 + (duration.subsec_nanos() as f64 / 1e9);
        println!(
            "{} threads: {:.0} gets/s {:.0} puts/s {:.0} ops/s",
            n_threads,
            n_gets as f64 / secs,
            n_puts as f64 / secs,
            (n_gets + n_puts) as f64 / secs
        );

        let ht = hist.lock().unwrap();
        let mut kvs: Vec<_> = ht.iter().collect();
        kvs.sort();
        let v: Vec<_> = kvs
            .iter()
            .map(|&(k, v)| println!("Key {:?}: {:?} gets/puts", k, v))
            .collect();
        println!("Unique key count: {}", v.len());
        assert_eq!(n_keys, v.len());

        let total: i64 = kvs.iter().map(|&(_, &(g, s))| (g + s) as i64).sum();

        let mut sum = 0;
        for &(k, v) in kvs.iter() {
            let &(g, s) = v;
            sum += g + s;
            let percentile = sum as f64 / total as f64;
            println!("Key {:?}: {:?} percentile", k, percentile);
        }
        // For 20 keys median key should be near 4th key, so this checks out.
    }
}
