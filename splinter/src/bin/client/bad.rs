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
extern crate time;
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
use db::wireformat::*;

use rand::distributions::Sample;
use rand::{Rng, SeedableRng, XorShiftRng};
use zipf::ZipfDistribution;

use splinter::*;

// Bad benchmark.
// The benchmark is created and parameterized with `new()`. Many threads
// share the same benchmark instance. Each thread can call `abc()` which
// runs the benchmark until another thread calls `stop()`. Each thread
// then returns their runtime and the number of gets and puts they have done.
// This benchmark doesn't care about how get/put are implemented; it takes
// function pointers to get/put on `new()` and just calls those as it runs.
//
// The tests below give an example of how to use it and how to aggregate the results.
pub struct Bad {
    bad_ptm: usize,
    rng: Box<Rng>,
    key_rng: Box<ZipfDistribution>,
    tenant_rng: Box<ZipfDistribution>,
    key_buf: Vec<u8>,
}

impl Bad {
    // Create a new benchmark instance.
    //
    // # Arguments
    //  - key_len: Length of the keys to generate per get/put. Most bytes will be zero, since
    //             the benchmark poplates them from a random 32-bit value.
    //  - n_keys: Number of keys from which random keys are drawn.
    //  - bad_ptm: Number between 0 and 100 indicating number of ops per 10 million that
    //             misbehave.
    //  - skew: Zipfian skew parameter. 0.99 is default.
    //  - n_tenants: The number of tenants from which the tenant id is chosen.
    //  - tenant_skew: The skew in the Zipfian distribution from which tenant id's are drawn.
    // # Return
    //  A new instance of Bad that threads can call `abc()` on to run.
    fn new(
        key_len: usize,
        n_keys: usize,
        bad_ptm: usize,
        skew: f64,
        n_tenants: u32,
        tenant_skew: f64,
    ) -> Bad {
        let seed: [u32; 4] = rand::random::<[u32; 4]>();

        let mut key_buf: Vec<u8> = Vec::with_capacity(key_len);
        key_buf.resize(key_len, 0);

        Bad {
            bad_ptm: bad_ptm,
            rng: Box::new(XorShiftRng::from_seed(seed)),
            key_rng: Box::new(
                ZipfDistribution::new(n_keys, skew).expect("Couldn't create key RNG."),
            ),
            tenant_rng: Box::new(
                ZipfDistribution::new(n_tenants as usize, tenant_skew)
                    .expect("Couldn't create tenant RNG."),
            ),
            key_buf: key_buf,
        }
    }

    // Run Bad (depending on `new()` parameters).
    //
    // # Arguments
    //  - get: A function that fetches the data stored under a bytestring key of `self.key_len` bytes.
    //  - set: A function that fetches multiple copies of the data stored under a bytestring key of
    //         `self.key_len` bytes
    //
    // # Return
    //  A three tuple consisting of the duration that this thread ran the benchmark, the
    //  number of gets it performed, and the number of puts it performed.
    pub fn abc<G, P, R>(&mut self, mut get: G, mut bad: P) -> R
    where
        G: FnMut(u32, &[u8]) -> R,
        P: FnMut(u32, &[u8]) -> R,
    {
        let is_get = (self.rng.gen::<u32>() % 10000000) >= self.bad_ptm as u32;

        // Sample a tenant.
        let t = self.tenant_rng.sample(&mut self.rng) as u32;

        // Sample a key, and convert into a little endian byte array.
        let k = self.key_rng.sample(&mut self.rng) as u32;
        let k: [u8; 4] = unsafe { transmute(k.to_le()) };
        self.key_buf[0..mem::size_of::<u32>()].copy_from_slice(&k);

        if is_get {
            get(t, self.key_buf.as_slice())
        } else {
            bad(t, self.key_buf.as_slice())
        }
    }
}

/// Sends out RPC requests to a Sandstorm server.
struct BadSend {
    // The actual workload. Required to generate keys for get() and bad() requests.
    workload: RefCell<Bad>,

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

    // Payload for an invoke() based get operation. Required in order to avoid making intermediate
    // copies of the extension name, table id, and key.
    payload_get: RefCell<Vec<u8>>,

    // Payload for an invoke() based bad operation. Required in order to avoid making intermediate
    // copies of the extension name, table id, and key.
    payload_bad: RefCell<Vec<u8>>,
}

// Implementation of methods on BadSend.
impl BadSend {
    /// Constructs a BadSend.
    ///
    /// # Arguments
    ///
    /// * `config`:    Client configuration with Bad related (key etc.) as well as
    ///                Network related (Server and Client MAC address etc.) parameters.
    /// * `port`:      Network port over which requests will be sent out.
    /// * `reqs`:      The number of requests to be issued to the server.
    /// * `dst_ports`: The total number of UDP ports the server is listening on.
    ///
    /// # Return
    ///
    /// A Bad request generator.
    fn new(
        config: &config::ClientConfig,
        port: CacheAligned<PortQueue>,
        reqs: u64,
        dst_ports: u16,
    ) -> BadSend {
        // The payload on an invoke() based get request consists of the extensions name ("get"),
        // the table id to perform the lookup on, and the key to lookup.
        let payload_len = "get".as_bytes().len() + mem::size_of::<u64>() + config.key_len;
        let mut payload_get = Vec::with_capacity(payload_len);
        payload_get.extend_from_slice("get".as_bytes());
        payload_get.extend_from_slice(&unsafe { transmute::<u64, [u8; 8]>(1u64.to_le()) });
        payload_get.resize(payload_len, 0);

        // The payload on an invoke() based bad request consists of the extensions name ("bad"),
        // the table id to perform the lookup on, and the key to lookup.
        let payload_len = "bad".as_bytes().len() + mem::size_of::<u64>() + config.key_len;
        let mut payload_bad = Vec::with_capacity(payload_len);
        payload_bad.extend_from_slice("bad".as_bytes());
        payload_bad.extend_from_slice(&unsafe { transmute::<u64, [u8; 8]>(1u64.to_le()) });
        payload_bad.resize(payload_len, 0);

        BadSend {
            workload: RefCell::new(Bad::new(
                config.key_len,
                config.n_keys,
                config.bad_ptm,
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
            payload_get: RefCell::new(payload_get),
            payload_bad: RefCell::new(payload_bad),
        }
    }
}

// The Executable trait allowing BadSend to be scheduled by Netbricks.
impl Executable for BadSend {
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
            // Configured to issue invoke() RPCs.
            let mut p_get = self.payload_get.borrow_mut();
            let mut p_bad = self.payload_bad.borrow_mut();

            // XXX Heavily dependent on how `Bad` creates a key. Only the first four
            // bytes of the key matter, the rest are zero. The value is always zero.
            self.workload.borrow_mut().abc(
                |tenant, key| {
                    // First 11 bytes on the payload were already pre-populated with the
                    // extension name (3 bytes), and the table id (8 bytes). Just write in the
                    // first 4 bytes of the key.
                    p_get[11..15].copy_from_slice(&key[0..4]);
                    self.sender.send_invoke(tenant, 3, &p_get, curr)
                },
                |tenant, key| {
                    // First 11 bytes on the payload were already pre-populated with the
                    // extension name (3 bytes), and the table id (8 bytes). Just write in the
                    // first 4 bytes of the key.
                    p_bad[11..15].copy_from_slice(&key[0..4]);
                    self.sender.send_invoke(tenant, 3, &p_bad, curr)
                },
            );

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

/// Receives responses to requests sent out by BadSend.
struct BadRecv<T>
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
}

// Implementation of methods on BadRecv.
impl<T> BadRecv<T>
where
    T: PacketTx + PacketRx + Display + Clone + 'static,
{
    /// Constructs a BadRecv.
    ///
    /// # Arguments
    ///
    /// * `port` :  Network port on which responses will be polled for.
    /// * `resps`:  The number of responses to wait for before calculating statistics.
    /// * `master`: Boolean indicating if the receiver should make latency measurements.
    ///
    /// # Return
    ///
    /// A response receiver that measures the median latency and throughput of a Sandstorm
    /// server.
    fn new(port: T, resps: u64, master: bool) -> BadRecv<T> {
        BadRecv {
            receiver: dispatch::Receiver::new(port),
            responses: resps,
            start: cycles::rdtsc(),
            recvd: 0,
            latencies: Vec::with_capacity(resps as usize),
            master: master,
            stop: 0,
        }
    }
}

// Implementation of the `Drop` trait on BadRecv.
impl<T> Drop for BadRecv<T>
where
    T: PacketTx + PacketRx + Display + Clone + 'static,
{
    fn drop(&mut self) {
        // Calculate & print the throughput for all client threads.
        println!(
            "BAD Throughput {}",
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

// Executable trait allowing BadRecv to be scheduled by Netbricks.
impl<T> Executable for BadRecv<T>
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

                    let p = packet.parse_header::<InvokeResponse>();
                    self.latencies
                        .push(curr - p.get_header().common_header.stamp);
                    p.free_packet();
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

/// Sets up BadSend by adding it to a Netbricks scheduler.
///
/// # Arguments
///
/// * `config`:    Network related configuration such as the MAC and IP address.
/// * `ports`:     Network port on which packets will be sent.
/// * `scheduler`: Netbricks scheduler to which BadSend will be added.
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
    match scheduler.add_task(BadSend::new(
        config,
        ports[0].clone(),
        config.num_reqs as u64,
        config.server_udp_ports as u16,
    )) {
        Ok(_) => {
            info!(
                "Successfully added BadSend with tx queue {}.",
                ports[0].txq()
            );
        }

        Err(ref err) => {
            error!("Error while adding to Netbricks pipeline {}", err);
            std::process::exit(1);
        }
    }
}

/// Sets up BadRecv by adding it to a Netbricks scheduler.
///
/// # Arguments
///
/// * `ports`:     Network port on which packets will be sent.
/// * `scheduler`: Netbricks scheduler to which BadRecv will be added.
/// * `master`:    If true, the added BadRecv will make latency measurements.
fn setup_recv<S>(ports: Vec<CacheAligned<PortQueue>>, scheduler: &mut S, _core: i32, master: bool)
where
    S: Scheduler + Sized,
{
    if ports.len() != 1 {
        error!("Client should be configured with exactly 1 port!");
        std::process::exit(1);
    }

    // Add the receiver to a netbricks pipeline.
    match scheduler.add_task(BadRecv::new(
        ports[0].clone(),
        34 * 1000 * 1000 as u64,
        master,
    )) {
        Ok(_) => {
            info!(
                "Successfully added BadRecv with rx queue {}.",
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

        // Setup the receive side.
        net_context
            .add_pipeline_to_core(
                receive[i],
                Arc::new(
                    move |_ports, sched: &mut StandaloneScheduler, core: i32, _sibling| {
                        setup_recv(port.clone(), sched, core, master)
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
