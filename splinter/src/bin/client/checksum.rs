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
extern crate crypto_hash;
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
use std::sync::Arc;

use crypto_hash::{digest, Algorithm};

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

// This shows which model to use, 1 is for MD5, 2 is for SHA-1, 3 is for SHA-256 and 4 is for SHA-512.
static CHECKSUM_ALGO: u8 = 1;

// PUSHBACK benchmark.
// The benchmark is created and parameterized with `new()`. Many threads
// share the same benchmark instance. Each thread can call `abc()` which
// runs the benchmark until another thread calls `stop()`. Each thread
// then returns their runtime and the number of gets and puts they have done.
// This benchmark doesn't care about how get/put are implemented; it takes
// function pointers to get/put on `new()` and just calls those as it runs.
//
// The tests below give an example of how to use it and how to aggregate the results.
pub struct CHECKSUM {
    put_pct: usize,
    rng: Box<dyn Rng>,
    key_rng: Box<ZipfDistribution>,
    tenant_rng: Box<ZipfDistribution>,
    key_buf: Vec<u8>,
    multikey_buf: Vec<u8>,
}

impl CHECKSUM {
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
    ) -> CHECKSUM {
        let seed: [u32; 4] = rand::random::<[u32; 4]>();

        let mut key_buf: Vec<u8> = Vec::with_capacity(key_len);
        key_buf.resize(key_len, 0);
        let mut multikey_buf: Vec<u8> = Vec::with_capacity(2 * key_len);
        multikey_buf.resize(2 * key_len, 0);

        CHECKSUM {
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
    // The calling thread will not return until `done()` is called on this `CHECKSUM` instance.
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

/// Receives responses to PUSHBACK requests sent out by ChecksumSend.
struct ChecksumRecvSend<T>
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
    workload: RefCell<CHECKSUM>,

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

    // Flag to indicate if the procedure is finished or not.
    finished: bool,

    // To keep the mapping between sent and received packets. The client doesn't want to send
    // more than 32(XXX) outstanding packets.
    outstanding: u64,

    // To keep a mapping between each packet and request parameters. This information will be used
    // when the server pushes back the extension.
    manager: RefCell<TaskManager>,

    /// Number of key to use in one operation.
    num: u32,

    // The table-id used for the requests.
    table_id: u64,

    // The length of the extension name; used for parsing the request payload.
    name_length: u32,
}

// Implementation of methods on ChecksumRecv.
impl<T> ChecksumRecvSend<T>
where
    T: PacketTx + PacketRx + Display + Clone + 'static,
{
    /// Constructs a ChecksumRecv.
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
        num: u32,
    ) -> ChecksumRecvSend<T> {
        let table_id: u64 = 1;
        // The payload on an invoke() based get request consists of the extensions name ("checksum"),
        // the table id to perform the lookup on, number of key in operation, and the key to lookup, and operation type.
        let payload_len = "checksum".as_bytes().len()
            + mem::size_of::<u64>()
            + mem::size_of::<u32>()
            + config.key_len
            + mem::size_of::<u8>();
        let mut invoke_get: Vec<u8> = Vec::with_capacity(payload_len);
        invoke_get.extend_from_slice("checksum".as_bytes());
        invoke_get.extend_from_slice(&unsafe { transmute::<u64, [u8; 8]>(table_id.to_le()) });
        invoke_get.extend_from_slice(&unsafe { transmute::<u32, [u8; 4]>(num.to_le()) });
        invoke_get.extend_from_slice(&[0; 8]); // Placeholder for key
        invoke_get.extend_from_slice(&[CHECKSUM_ALGO]);
        invoke_get.resize(payload_len, 0);

        ChecksumRecvSend {
            receiver: dispatch::Receiver::new(rx_port),
            responses: resps,
            start: cycles::rdtsc(),
            recvd: 0,
            aborted: 0,
            latencies: Vec::with_capacity(resps as usize),
            master: master,
            stop: 0,
            workload: RefCell::new(CHECKSUM::new(
                config.key_len,
                config.value_len,
                config.n_keys as usize,
                0,
                config.skew,
                config.num_tenants,
                config.tenant_skew,
            )),
            sender: Arc::new(dispatch::Sender::new(config, tx_port, dst_ports)),
            requests: reqs,
            sent: 0,
            native: !config.use_invoke,
            invoke_get: RefCell::new(invoke_get),
            finished: false,
            outstanding: 0,
            manager: RefCell::new(TaskManager::new(Arc::clone(&masterservice))),
            num: num,
            table_id: table_id,
            name_length: "checksum".as_bytes().len() as u32,
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
                    |_tenant, _multikey| {
                        info!("The client shouldn't generate this request");
                    },
                );
                self.outstanding += 1;
            } else {
                // Configured to issue invoke() RPCs.
                let mut p_get = self.invoke_get.borrow_mut();

                self.workload.borrow_mut().abc(
                    |tenant, key| {
                        p_get[20..24].copy_from_slice(&key[0..4]);
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
                    |_tenant, _multiget| {
                        info!("The client shouldn't generate this request");
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
                                self.aborted += 1;
                                info!("Abort");
                            }

                            RpcStatus::StatusOk => {
                                self.recvd += 1;
                                self.latencies.push(curr - timestamp);
                            }

                            _ => {}
                        }
                        self.outstanding -= 1;
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
                        let record = p.get_payload();
                        let keys = record.split_at(17).1;
                        let keys = keys.split_at(self.num as usize * 30).0;
                        self.sender.send_multiget(
                            p.get_header().common_header.tenant,
                            self.table_id,
                            30,
                            self.num,
                            keys,
                            p.get_header().common_header.stamp,
                        );
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
                        self.manager
                            .borrow_mut()
                            .update_rwset(timestamp, records, 139, 30);
                    }

                    RpcStatus::StatusTxAbort => {
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
                info!("This response type is not valid for this application");
                packet.free_packet();
            }
        }
    }

    /// Lookup the `Workload` trait for documentation on this method.
    fn process_multiget_response(&mut self, packet: &Packet<MultiGetResponse, EmptyMetadata>) {
        let mut commit_payload = Vec::with_capacity(self.num as usize * 139);
        commit_payload.extend_from_slice(packet.get_payload());

        let header = packet.get_header();
        let records = packet.get_payload();

        // Execute the compute part.
        let num = self.num;
        self.execute_task(records, num);

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
    pub fn execute_task(&mut self, records: &[u8], _num: u32) {
        let mut generator = move || {
            let mut aggr: u64 = 0;
            for record in records.chunks(139) {
                let vals = record.split_at(39).1;
                if vals.len() == 100 {
                    if CHECKSUM_ALGO == 1 {
                        let result = digest(Algorithm::MD5, vals);
                        aggr += result[0] as u64;
                    }
                    if CHECKSUM_ALGO == 2 {
                        let result = digest(Algorithm::SHA1, vals);
                        aggr += result[0] as u64;
                    }
                    if CHECKSUM_ALGO == 3 {
                        let result = digest(Algorithm::SHA256, vals);
                        aggr += result[0] as u64;
                    }
                    if CHECKSUM_ALGO == 4 {
                        let result = digest(Algorithm::SHA512, vals);
                        aggr += result[0] as u64;
                    }
                    if CHECKSUM_ALGO <= 0 && CHECKSUM_ALGO > 4 {
                        return 1;
                    }
                } else {
                    return 2;
                }
            }
            if aggr != 0 {
                aggr -= aggr;
            }

            return aggr;

            // XXX: This yield is required to get the compiler to compile this closure into a
            // generator. It is unreachable and benign.
            yield 0;
        };

        match Pin::new(&mut generator).resume(()) {
            GeneratorState::Yielded(val) => {
                if val != 0 {
                    panic!("Checksum native execution is buggy");
                }
            }
            GeneratorState::Complete(val) => {
                if val != 0 {
                    panic!("Checksum native execution is buggy");
                }
            }
        }
    }
}

// Implementation of the `Drop` trait on ChecksumRecv.
impl<T> Drop for ChecksumRecvSend<T>
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

// Executable trait allowing ChecksumRecv to be scheduled by Netbricks.
impl<T> Executable for ChecksumRecvSend<T>
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

    // CHECKSUM compute size.
    let num = config.num_aggr as u32;

    // Add the receiver to a netbricks pipeline.
    match scheduler.add_task(ChecksumRecvSend::new(
        ports[0].clone(),
        34 * 1000 * 1000 as u64,
        master,
        config,
        ports[0].clone(),
        config.num_reqs as u64,
        config.server_udp_ports as u16,
        masterservice,
        num,
    )) {
        Ok(_) => {
            info!(
                "Successfully added ChecksumRecvSend with rx-tx queue {}.",
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
    std::thread::sleep(std::time::Duration::from_secs(10));

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
                let mut b = super::CHECKSUM::new(10, 100, 1000000, 5, 0.99, 1024, 0.1);
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
                let mut b = super::CHECKSUM::new(4, 100, n_keys, 5, 0.99, 1024, 0.1);
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
