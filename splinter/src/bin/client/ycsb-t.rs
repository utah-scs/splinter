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

#![feature(generators, generator_trait)]

extern crate bytes;
extern crate db;
extern crate rand;
extern crate sandstorm;
extern crate spin;
extern crate splinter;
extern crate time;
extern crate zipf;

mod setup;

use std::cell::RefCell;
use std::fmt::Display;
use std::mem;
use std::mem::transmute;
use std::ops::{Generator, GeneratorState};
use std::pin::Pin;
use std::slice;
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
use db::rpc::*;
use db::wireformat::*;

use rand::distributions::Sample;
use rand::{Rng, SeedableRng, XorShiftRng};
use splinter::sched::TaskManager;
use splinter::*;
use zipf::ZipfDistribution;

// Flag to indicate that the client has finished sending and receiving the packets.
static mut FINISHED: bool = false;

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
pub struct YCSBT {
    put_pct: usize,
    rng: Box<dyn Rng>,
    key_rng: Box<ZipfDistribution>,
    tenant_rng: Box<ZipfDistribution>,
    key_buf: Vec<u8>,
    multikey_buf: Vec<u8>,
}

impl YCSBT {
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
        _value_len: usize,
        n_keys: usize,
        put_pct: usize,
        skew: f64,
        n_tenants: u32,
        tenant_skew: f64,
    ) -> YCSBT {
        let seed: [u32; 4] = rand::random::<[u32; 4]>();

        let mut key_buf: Vec<u8> = Vec::with_capacity(key_len);
        key_buf.resize(key_len, 0);
        let mut multikey_buf: Vec<u8> = Vec::with_capacity(2 * key_len);
        multikey_buf.resize(2 * key_len, 0);

        YCSBT {
            put_pct: put_pct,
            rng: Box::new(XorShiftRng::from_seed(seed)),
            key_rng: Box::new(
                ZipfDistribution::new(n_keys, skew).expect("Couldn't create key RNG."),
            ),
            tenant_rng: Box::new(
                ZipfDistribution::new(n_tenants as usize, tenant_skew)
                    .expect("Couldn't create tenant RNG."),
            ),
            key_buf: key_buf,
            multikey_buf: multikey_buf,
        }
    }

    // Run PUSHBACK A, B, or C (depending on `new()` parameters).
    // The calling thread will not return until `done()` is called on this `YCSBT` instance.
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
        G: FnMut(u32, &[u8]) -> R,
        P: FnMut(u32, &[u8]) -> R,
    {
        let is_get = (self.rng.gen::<u32>() % 100) >= self.put_pct as u32;

        // Sample a tenant.
        let t = self.tenant_rng.sample(&mut self.rng) as u32;

        // Sample a key, and convert into a little endian byte array.
        let k = self.key_rng.sample(&mut self.rng) as u32;
        let k: [u8; 4] = unsafe { transmute(k.to_le()) };
        self.key_buf[0..4].copy_from_slice(&k);

        if is_get {
            get(t, self.key_buf.as_slice())
        } else {
            self.multikey_buf[0..4].copy_from_slice(&k);
            let k = self.key_rng.sample(&mut self.rng) as u32;
            let k: [u8; 4] = unsafe { transmute(k.to_le()) };
            self.multikey_buf[30..34].copy_from_slice(&k);
            put(t, self.multikey_buf.as_slice())
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

    // The total number of abort responses received so far.
    aborted: u64,

    // Vector of sampled request latencies. Required to calculate distributions once all responses
    // have been received.
    latencies: Vec<u64>,

    // If true, this receiver will make latency measurements.
    master: bool,

    // Time stamp in cycles at which measurement stopped.
    stop: u64,

    // The actual PUSHBACK workload. Required to generate keys and values for get() and put() requests.
    workload: RefCell<YCSBT>,

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
    invoke_get: RefCell<Vec<u8>>,

    // Payload for an invoke() based put operation. Required in order to avoid making intermediate
    // copies of the extension name, table id, key length, key, and value.
    invoke_get_modify: RefCell<Vec<u8>>,

    // Flag to indicate if the procedure is finished or not.
    finished: bool,

    // To keep the mapping between sent and received packets. The client doesn't want to send
    // more than 32(XXX) outstanding packets.
    outstanding: u64,

    // To keep a mapping between each packet and request parameters. This information will be used
    // when the server pushes back the extension.
    manager: RefCell<TaskManager>,

    /// Order of the final polynomial to be computed.
    ord: u32,

    // The length of the key.
    key_len: usize,

    // The length of the record.
    record_len: usize,

    // The table-id used for the requests.
    table_id: u64,

    // The length of the extension name; used for parsing the request payload.
    name_length: u32,
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
        order: u32,
    ) -> PushbackRecvSend<T> {
        let table_id: u64 = 1;
        // The payload on an invoke() based get request consists of the extensions name ("ycsbt"),
        // operation type, the table id to perform the lookup on, and the key to lookup.
        let payload_len = "ycsbt".as_bytes().len()
            + mem::size_of::<u64>()
            + config.key_len
            + mem::size_of::<u32>()
            + mem::size_of::<u8>();
        let mut invoke_get: Vec<u8> = Vec::with_capacity(payload_len);
        invoke_get.extend_from_slice("ycsbt".as_bytes());
        invoke_get.extend_from_slice(&unsafe { transmute::<u64, [u8; 8]>(table_id.to_le()) });
        invoke_get.extend_from_slice(&[0; 30]); // Placeholder for key
        invoke_get.extend_from_slice(&unsafe { transmute::<u32, [u8; 4]>(order.to_le()) });
        invoke_get.extend_from_slice(&[1]);
        invoke_get.resize(payload_len, 0);

        // The payload on an invoke() based get request consists of the extensions name ("ycsbt"),
        // the table id to perform the lookup on, and the two keys to lookup and modify.
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

        PushbackRecvSend {
            receiver: dispatch::Receiver::new(rx_port),
            responses: resps,
            start: cycles::rdtsc(),
            recvd: 0,
            aborted: 0,
            latencies: Vec::with_capacity(resps as usize),
            master: master,
            stop: 0,
            workload: RefCell::new(YCSBT::new(
                config.key_len,
                config.value_len,
                config.n_keys - config.num_aggr as usize,
                config.put_pct,
                config.skew,
                config.num_tenants,
                config.tenant_skew,
            )),
            sender: Arc::new(dispatch::Sender::new(config, tx_port, dst_ports)),
            requests: reqs,
            sent: 0,
            native: !config.use_invoke,
            invoke_get: RefCell::new(invoke_get),
            invoke_get_modify: RefCell::new(invoke_get_modify),
            finished: false,
            outstanding: 0,
            manager: RefCell::new(TaskManager::new(Arc::clone(&masterservice))),
            ord: order,
            key_len: config.key_len,
            record_len: 1 + 8 + config.key_len + config.value_len,
            table_id: 1,
            name_length: "ycsbt".len() as u32,
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
            let curr = cycles::rdtsc();

            if self.native == true {
                // Configured to issue native RPCs, issue a regular get()/put() operation.
                self.workload.borrow_mut().abc(
                    |tenant, key| self.sender.send_get(tenant, self.table_id, key, curr),
                    |tenant, multikey| {
                        self.sender.send_multiget(
                            tenant,
                            self.table_id,
                            self.key_len as u16,
                            2,
                            multikey,
                            curr,
                        )
                    },
                );
                self.outstanding += 1;
            } else {
                // Configured to issue invoke() RPCs.
                let mut p_get = self.invoke_get.borrow_mut();
                let mut p_put = self.invoke_get_modify.borrow_mut();

                self.workload.borrow_mut().abc(
                    |tenant, key| {
                        p_get[13..17].copy_from_slice(&key[0..4]);
                        self.manager.borrow_mut().create_task(
                            curr,
                            &p_get,
                            tenant,
                            self.name_length as usize,
                            Arc::clone(&self.sender),
                        );
                        self.sender
                            .send_invoke(tenant, self.name_length, &p_get, curr)
                    },
                    |tenant, multiget| {
                        p_put[13..17].copy_from_slice(&multiget[0..4]);
                        p_put[43..47].copy_from_slice(&multiget[30..34]);
                        self.manager.borrow_mut().create_task(
                            curr,
                            &p_put,
                            tenant,
                            self.name_length as usize,
                            Arc::clone(&self.sender),
                        );
                        self.sender
                            .send_invoke(tenant, self.name_length, &p_put, curr)
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
                                    let mut invoke_get_modify = self.invoke_get_modify.borrow_mut();
                                    let name_length = self.name_length;
                                    let mut start_index = 13;
                                    let mut end_index = start_index + 4;
                                    let records = p.get_payload();
                                    for key in records.chunks(self.key_len) {
                                        invoke_get_modify[start_index..end_index]
                                            .copy_from_slice(&key[0..4]);
                                        start_index += 30;
                                        end_index += 30;
                                    }

                                    self.sender.send_invoke(
                                        tenant,
                                        name_length,
                                        &invoke_get_modify,
                                        timestamp,
                                    );
                                    self.manager.borrow_mut().create_task(
                                        timestamp,
                                        &invoke_get_modify,
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
            self.finished = true;
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
                        self.process_get_response(&p);
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
                        self.process_multiget_response(&p);
                    }

                    _ => {
                        info!("Invalid native Multiget() response");
                    }
                }
                p.free_packet();
            }

            _ => {
                info!("Invalid native response");
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
                        // No self.outstanding += 1, as this is in response to a previous request.
                        // No subtract and addition.
                        self.aborted += 1;
                        self.sender.send_invoke(
                            p.get_header().common_header.tenant,
                            self.name_length,
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

            _ => {
                info!("Shouldn't for this application");
                packet.free_packet();
            }
        }
    }

    /// Lookup the `Workload` trait for documentation on this method.
    fn process_get_response(&mut self, packet: &Packet<GetResponse, EmptyMetadata>) {
        let header = packet.get_header();
        let record = packet.get_payload();
        self.sender.send_commit(
            header.common_header.tenant,
            self.table_id,
            record,
            header.common_header.stamp,
            30,
            100,
        );
    }

    /// Lookup the `Workload` trait for documentation on this method.
    fn process_multiget_response(&mut self, packet: &Packet<MultiGetResponse, EmptyMetadata>) {
        let mut value1: Vec<u8> = Vec::with_capacity(100);
        let mut value2: Vec<u8> = Vec::with_capacity(100);
        let mut commit_payload = Vec::with_capacity(4 * 139);
        commit_payload.extend_from_slice(packet.get_payload());

        let header = packet.get_header();
        let records = packet.get_payload();
        let (record1, record2) = records.split_at(139);
        let (key1, v1) = record1.split_at(9).1.split_at(30);
        let (key2, v2) = record2.split_at(9).1.split_at(30);
        value1.extend_from_slice(v1);
        value2.extend_from_slice(v2);
        if value1[0] > 0 && value2[0] < 255 {
            value1[0] -= 1;
            value2[0] += 1;
        } else if value2[0] > 0 && value1[0] < 255 {
            value1[0] += 1;
            value2[0] -= 1;
        }

        let ptr = &OpType::SandstormWrite as *const _ as *const u8;
        let optype = unsafe { slice::from_raw_parts(ptr, mem::size_of::<OpType>()) };
        let version: [u8; 8] = unsafe { transmute(0u64.to_le()) };
        commit_payload.extend_from_slice(optype);
        commit_payload.extend_from_slice(&version);
        commit_payload.extend_from_slice(key1);
        commit_payload.extend_from_slice(&value1);

        commit_payload.extend_from_slice(optype);
        commit_payload.extend_from_slice(&version);
        commit_payload.extend_from_slice(key2);
        commit_payload.extend_from_slice(&value2);

        // Execute the compute part.
        let order = self.ord;
        self.execute_task(order);

        self.sender.send_commit(
            header.common_header.tenant,
            self.table_id,
            &commit_payload,
            header.common_header.stamp,
            30,
            100,
        );
    }

    #[allow(unreachable_code)]
    /// This method executes the task.
    ///
    /// # Arguments
    /// *`order`: The amount of compute in each extension.
    pub fn execute_task(&mut self, order: u32) {
        let mut generator = move || {
            // Compute part for this extension
            let start = cycles::rdtsc();
            while cycles::rdtsc() - start < order as u64 {}
            return 0;

            // XXX: This yield is required to get the compiler to compile this closure into a
            // generator. It is unreachable and benign.
            yield 0;
        };

        match Pin::new(&mut generator).resume(()) {
            GeneratorState::Yielded(val) => {
                if val != 0 {
                    panic!("Pushback native execution is buggy");
                }
            }
            GeneratorState::Complete(val) => {
                if val != 0 {
                    panic!("Pushback native execution is buggy");
                }
            }
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

    // YCSBT compute size.
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
            )
            .expect("Failed to initialize receive/transmit side.");
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
