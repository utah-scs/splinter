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
extern crate time;
extern crate splinter;
extern crate zipf;

mod setup;

use std::cell::RefCell;
use std::fmt::Display;
use std::mem;
use std::mem::transmute;
use std::sync::Arc;

use db::config;
use db::cycles;
use db::e2d2::allocators::*;
use db::e2d2::interface::*;
use db::e2d2::scheduler::*;
use db::log::*;
use db::rpc::*;
use db::wireformat::*;

use rand::distributions::Sample;
use rand::{Rng, SeedableRng, XorShiftRng};
use zipf::ZipfDistribution;

use splinter::*;

// YCSB A, B, and C benchmark.
// The benchmark is created and parameterized with `new()`. Many threads
// share the same benchmark instance. Each thread can call `abc()` which
// runs the benchmark until another thread calls `stop()`. Each thread
// then returns their runtime and the number of gets and puts they have done.
// This benchmark doesn't care about how get/put are implemented; it takes
// function pointers to get/put on `new()` and just calls those as it runs.
//
// The tests below give an example of how to use it and how to aggregate the results.
pub struct Ycsb {
    put_pct: usize,
    rng: Box<Rng>,
    key_rng: Box<ZipfDistribution>,
    tenant_rng: Box<ZipfDistribution>,
    key_buf: Vec<u8>,
    value_buf: Vec<u8>,
}

impl Ycsb {
    // Create a new benchmark instance.
    //
    // # Arguments
    //  - key_len: Length of the keys to generate per get/put. Most bytes will be zero, since
    //             the benchmark poplates them from a random 32-bit value.
    //  - value_len: Length of the values to store per put. Always all zero bytes.
    //  - n_keys: Number of keys from which random keys are drawn.
    //  - put_pct: Number between 0 and 100 indicating percent of ops that are sets.
    //  - skew: Zipfian skew parameter. 0.99 is YCSB default.
    //  - n_tenants: The number of tenants from which the tenant id is chosen.
    //  - tenant_skew: The skew in the Zipfian distribution from which tenant id's are drawn.
    // # Return
    //  A new instance of YCSB that threads can call `abc()` on to run.
    fn new(
        key_len: usize,
        value_len: usize,
        n_keys: usize,
        put_pct: usize,
        skew: f64,
        n_tenants: u32,
        tenant_skew: f64,
    ) -> Ycsb {
        let seed: [u32; 4] = rand::random::<[u32; 4]>();

        let mut key_buf: Vec<u8> = Vec::with_capacity(key_len);
        key_buf.resize(key_len, 0);
        let mut value_buf: Vec<u8> = Vec::with_capacity(value_len);
        value_buf.resize(value_len, 0);

        Ycsb {
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
            value_buf: value_buf,
        }
    }

    // Run YCSB A, B, or C (depending on `new()` parameters).
    // The calling thread will not return until `done()` is called on this `Ycsb` instance.
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
        P: FnMut(u32, &[u8], &[u8]) -> R,
    {
        let is_get = (self.rng.gen::<u32>() % 100) >= self.put_pct as u32;

        // Sample a tenant.
        let t = self.tenant_rng.sample(&mut self.rng) as u32;

        // Sample a key, and convert into a little endian byte array.
        let k = self.key_rng.sample(&mut self.rng) as u32;
        let k: [u8; 4] = unsafe { transmute(k.to_le()) };
        self.key_buf[0..mem::size_of::<u32>()].copy_from_slice(&k);

        if is_get {
            get(t, self.key_buf.as_slice())
        } else {
            put(t, self.key_buf.as_slice(), self.value_buf.as_slice())
        }
    }
}

/// Sends out YCSB based RPC requests to a Sandstorm server.
struct YcsbSend {
    // The actual YCSB workload. Required to generate keys and values for get() and put() requests.
    workload: RefCell<Ycsb>,

    // Network stack required to actually send RPC requests out the network.
    sender: dispatch::Sender,

    // Total number of requests to be sent out.
    requests: u64,

    // Number of requests that have been sent out so far.
    sent: u64,

    // The inverse of the rate at which requests are to be generated. Basically, the time interval
    // between two request generations in cycles.
    rate_inv: u64,

    // The time stamp at which the workload started generating requests in cycles.
    start: u64,

    // The time stamp at which the next request must be issued in cycles.
    next: u64,

    // If true, RPC requests corresponding to native get() and put() operations are sent out. If
    // false, invoke() based RPC requests are sent out.
    native: bool,

    // Payload for an invoke() based get operation. Required in order to avoid making intermediate
    // copies of the extension name, table id, and key.
    payload_get: RefCell<Vec<u8>>,

    // Payload for an invoke() based put operation. Required in order to avoid making intermediate
    // copies of the extension name, table id, key length, key, and value.
    payload_put: RefCell<Vec<u8>>,
}

// Implementation of methods on YcsbSend.
impl YcsbSend {
    /// Constructs a YcsbSend.
    ///
    /// # Arguments
    ///
    /// * `config`:    Client configuration with YCSB related (key and value length etc.) as well as
    ///                Network related (Server and Client MAC address etc.) parameters.
    /// * `port`:      Network port over which requests will be sent out.
    /// * `reqs`:      The number of requests to be issued to the server.
    /// * `dst_ports`: The total number of UDP ports the server is listening on.
    ///
    /// # Return
    ///
    /// A YCSB request generator.
    fn new(
        config: &config::ClientConfig,
        port: CacheAligned<PortQueue>,
        reqs: u64,
        dst_ports: u16,
    ) -> YcsbSend {
        // The payload on an invoke() based get request consists of the extensions name ("get"),
        // the table id to perform the lookup on, and the key to lookup.
        let payload_len = "get".as_bytes().len() + mem::size_of::<u64>() + config.key_len;
        let mut payload_get = Vec::with_capacity(payload_len);
        payload_get.extend_from_slice("get".as_bytes());
        payload_get.extend_from_slice(&unsafe { transmute::<u64, [u8; 8]>(1u64.to_le()) });
        payload_get.resize(payload_len, 0);

        // The payload on an invoke() based put request consists of the extensions name ("put"),
        // the table id to perform the lookup on, the length of the key to lookup, the key, and the
        // value to be inserted into the database.
        let payload_len = "put".as_bytes().len()
            + mem::size_of::<u64>()
            + mem::size_of::<u16>()
            + config.key_len
            + config.value_len;
        let mut payload_put = Vec::with_capacity(payload_len);
        payload_put.extend_from_slice("put".as_bytes());
        payload_put.extend_from_slice(&unsafe { transmute::<u64, [u8; 8]>(1u64.to_le()) });
        payload_put.extend_from_slice(&unsafe {
            transmute::<u16, [u8; 2]>((config.key_len as u16).to_le())
        });
        payload_put.resize(payload_len, 0);

        YcsbSend {
            workload: RefCell::new(Ycsb::new(
                config.key_len,
                config.value_len,
                config.n_keys,
                config.put_pct,
                config.skew,
                config.num_tenants,
                config.tenant_skew,
            )),
            sender: dispatch::Sender::new(config, port, dst_ports),
            requests: reqs,
            sent: 0,
            rate_inv: cycles::cycles_per_second() / config.req_rate as u64,
            start: cycles::rdtsc(),
            next: 0,
            native: !config.use_invoke,
            payload_get: RefCell::new(payload_get),
            payload_put: RefCell::new(payload_put),
        }
    }
}

// The Executable trait allowing YcsbSend to be scheduled by Netbricks.
impl Executable for YcsbSend {
    // Called internally by Netbricks.
    fn execute(&mut self) {
        // Return if there are no more requests to generate.
        if self.requests <= self.sent {
            return;
        }

        // Get the current time stamp so that we can determine if it is time to issue the next RPC.
        let curr = cycles::rdtsc();

        // If it is either time to send out a request, or if a request has never been sent out,
        // then, do so.
        if curr >= self.next || self.next == 0 {
            if self.native == true {
                // Configured to issue native RPCs, issue a regular get()/put() operation.
                self.workload.borrow_mut().abc(
                    |tenant, key| self.sender.send_get(tenant, 1, key, curr),
                    |tenant, key, val| self.sender.send_put(tenant, 1, key, val, curr),
                );
            } else {
                // Configured to issue invoke() RPCs.
                let mut p_get = self.payload_get.borrow_mut();
                let mut p_put = self.payload_put.borrow_mut();

                // XXX Heavily dependent on how `Ycsb` creates a key. Only the first four
                // bytes of the key matter, the rest are zero. The value is always zero.
                self.workload.borrow_mut().abc(
                    |tenant, key| {
                        // First 11 bytes on the payload were already pre-populated with the
                        // extension name (3 bytes), and the table id (8 bytes). Just write in the
                        // first 4 bytes of the key.
                        p_get[11..15].copy_from_slice(&key[0..4]);
                        self.sender.send_invoke(tenant, 3, &p_get, curr)
                    },
                    |tenant, key, _val| {
                        // First 13 bytes on the payload were already pre-populated with the
                        // extension name (3 bytes), the table id (8 bytes), and the key length (2
                        // bytes). Just write in the first 4 bytes of the key. The value is anyway
                        // always zero.
                        p_put[13..17].copy_from_slice(&key[0..4]);
                        self.sender.send_invoke(tenant, 3, &p_put, curr)
                    },
                );
            }

            // Update the time stamp at which the next request should be generated, assuming that
            // the first request was sent out at self.start.
            self.sent += 1;
            self.next = self.start + self.sent * self.rate_inv;
        }
    }

    fn dependencies(&mut self) -> Vec<usize> {
        vec![]
    }
}

/// Receives responses to YCSB requests sent out by YcsbSend.
struct YcsbRecv<T>
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

    // If true, then responses will be considered to correspond to native gets and puts.
    native: bool,

    // Time stamp in cycles at which measurement stopped.
    stop: u64,
}

// Implementation of methods on YcsbRecv.
impl<T> YcsbRecv<T>
where
    T: PacketTx + PacketRx + Display + Clone + 'static,
{
    /// Constructs a YcsbRecv.
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
    /// A YCSB response receiver that measures the median latency and throughput of a Sandstorm
    /// server.
    fn new(port: T, resps: u64, master: bool, native: bool) -> YcsbRecv<T> {
        YcsbRecv {
            receiver: dispatch::Receiver::new(port),
            responses: resps,
            start: cycles::rdtsc(),
            recvd: 0,
            latencies: Vec::with_capacity(resps as usize),
            master: master,
            native: native,
            stop: 0,
        }
    }
}

// Implementation of the `Drop` trait on YcsbRecv.
impl<T> Drop for YcsbRecv<T>
where
    T: PacketTx + PacketRx + Display + Clone + 'static,
{
    fn drop(&mut self) {
        // Calculate & print the throughput for all client threads.
        println!(
            "YCSB Throughput {}",
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

// Executable trait allowing YcsbRecv to be scheduled by Netbricks.
impl<T> Executable for YcsbRecv<T>
where
    T: PacketTx + PacketRx + Display + Clone + 'static,
{
    // Called internally by Netbricks.
    fn execute(&mut self) {
        // Don't do anything after all responses have been received.
        if self.responses <= self.recvd {
            return;
        }

        // Try to receive packets from the network port.
        // If there are packets, sample the latency of the server.
        if let Some(mut packets) = self.receiver.recv_res() {
            while let Some(packet) = packets.pop() {
                self.recvd += 1;

                // Measure latency on the master client after the first 2 million requests.
                // The start timestamp is present on the RPC response header.
                if self.recvd > 2 * 1000 * 1000 && self.master {
                    let curr = cycles::rdtsc();

                    match self.native {
                        // The response corresponds to an invoke() RPC.
                        false => {
                            let p = packet.parse_header::<InvokeResponse>();
                            self.latencies
                                .push(curr - p.get_header().common_header.stamp);
                            p.free_packet();
                        }

                        // The response corresponds to a get() or put() RPC.
                        // The opcode on the response identifies the RPC type.
                        true => match parse_rpc_opcode(&packet) {
                            OpCode::SandstormGetRpc => {
                                let p = packet.parse_header::<GetResponse>();
                                self.latencies
                                    .push(curr - p.get_header().common_header.stamp);
                                p.free_packet();
                            }

                            OpCode::SandstormPutRpc => {
                                let p = packet.parse_header::<PutResponse>();
                                self.latencies
                                    .push(curr - p.get_header().common_header.stamp);
                                p.free_packet();
                            }

                            _ => packet.free_packet(),
                        },
                    }
                } else {
                    packet.free_packet();
                }
            }
        }

        // The moment all response packets have been received, set the value of the
        // stop timestamp so that throughput can be estimated later.
        if self.responses <= self.recvd {
            self.stop = cycles::rdtsc();
        }
    }

    fn dependencies(&mut self) -> Vec<usize> {
        vec![]
    }
}

/// Sets up YcsbSend by adding it to a Netbricks scheduler.
///
/// # Arguments
///
/// * `config`:    Network related configuration such as the MAC and IP address.
/// * `ports`:     Network port on which packets will be sent.
/// * `scheduler`: Netbricks scheduler to which YcsbSend will be added.
fn setup_send<S>(
    config: &config::ClientConfig,
    ports: Vec<CacheAligned<PortQueue>>,
    scheduler: &mut S,
    _core: i32,
) where
    S: Scheduler + Sized,
{
    if ports.len() != 1 {
        error!("Client should be configured with exactly 1 port!");
        std::process::exit(1);
    }

    // Add the sender to a netbricks pipeline.
    match scheduler.add_task(YcsbSend::new(
        config,
        ports[0].clone(),
        config.num_reqs as u64,
        config.server_udp_ports as u16,
    )) {
        Ok(_) => {
            info!(
                "Successfully added YcsbSend with tx queue {}.",
                ports[0].txq()
            );
        }

        Err(ref err) => {
            error!("Error while adding to Netbricks pipeline {}", err);
            std::process::exit(1);
        }
    }
}

/// Sets up YcsbRecv by adding it to a Netbricks scheduler.
///
/// # Arguments
///
/// * `ports`:     Network port on which packets will be sent.
/// * `scheduler`: Netbricks scheduler to which YcsbRecv will be added.
/// * `master`:    If true, the added YcsbRecv will make latency measurements.
/// * `native`:    If true, the added YcsbRecv will assume that responses correspond to gets
///                and puts.
fn setup_recv<S>(
    ports: Vec<CacheAligned<PortQueue>>,
    scheduler: &mut S,
    _core: i32,
    master: bool,
    native: bool,
) where
    S: Scheduler + Sized,
{
    if ports.len() != 1 {
        error!("Client should be configured with exactly 1 port!");
        std::process::exit(1);
    }

    // Add the receiver to a netbricks pipeline.
    match scheduler.add_task(YcsbRecv::new(
        ports[0].clone(),
        34 * 1000 * 1000 as u64,
        master,
        native,
    )) {
        Ok(_) => {
            info!(
                "Successfully added YcsbRecv with rx queue {}.",
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
    let senders = [0, 2, 4, 6];
    let receive = [1, 3, 5, 7];
    assert!((senders.len() == 4) && (receive.len() == 4));

    // Setup 4 senders, and 4 receivers.
    for i in 0..4 {
        // First, retrieve a tx-rx queue pair from Netbricks
        let port = net_context
            .rx_queues
            .get(&senders[i])
            .expect("Failed to retrieve network port!")
            .clone();

        let mut master = false;
        if i == 0 {
            master = true;
        }

        let native = !config.use_invoke;

        // Setup the receive side.
        net_context
            .add_pipeline_to_core(
                receive[i],
                Arc::new(
                    move |_ports, sched: &mut StandaloneScheduler, core: i32, _sibling| {
                        setup_recv(port.clone(), sched, core, master, native)
                    },
                ),
            ).expect("Failed to initialize receive side.");

        // Setup the send side.
        net_context
            .add_pipeline_to_core(
                senders[i],
                Arc::new(
                    move |ports, sched: &mut StandaloneScheduler, core: i32, _sibling| {
                        setup_send(&config::ClientConfig::load(), ports, sched, core)
                    },
                ),
            ).expect("Failed to initialize send side.");
    }

    // Allow the system to bootup fully.
    std::thread::sleep(std::time::Duration::from_secs(1));

    // Run the client.
    net_context.execute();

    // Sleep for an amount of time approximately equal to the estimated execution time, and then
    // shutdown the client.
    std::thread::sleep(std::time::Duration::from_secs(exec as u64 + 11));

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
    fn ycsb_abc_basic() {
        let n_threads = 1;
        let mut threads = Vec::with_capacity(n_threads);
        let done = Arc::new(AtomicBool::new(false));

        for _ in 0..n_threads {
            let done = done.clone();
            threads.push(thread::spawn(move || {
                let mut b = super::Ycsb::new(10, 100, 1000000, 5, 0.99, 1024, 0.1);
                let mut n_gets = 0u64;
                let mut n_puts = 0u64;
                let start = Instant::now();
                while !done.load(Ordering::Relaxed) {
                    b.abc(|_t, _key| n_gets += 1, |_t, _key, _value| n_puts += 1);
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
    fn ycsb_abc_histogram() {
        let hist = Arc::new(Mutex::new(HashMap::new()));

        let n_keys = 20;
        let n_threads = 1;

        let mut threads = Vec::with_capacity(n_threads);
        let done = Arc::new(AtomicBool::new(false));
        for _ in 0..n_threads {
            let hist = hist.clone();
            let done = done.clone();
            threads.push(thread::spawn(move || {
                let mut b = super::Ycsb::new(4, 100, n_keys, 5, 0.99, 1024, 0.1);
                let mut n_gets = 0u64;
                let mut n_puts = 0u64;
                let start = Instant::now();
                while !done.load(Ordering::Relaxed) {
                    b.abc(
                        |_t, key| {
                            // get
                            let k = convert_key(key);
                            let mut ht = hist.lock().unwrap();
                            ht.entry(k).or_insert((0, 0)).0 += 1;
                            n_gets += 1
                        },
                        |_t, key, _value| {
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
