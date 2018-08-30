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
extern crate zipf;

mod dispatch;
mod setup;

use std::sync::Arc;
use std::mem::{size_of, transmute};

use db::config;
use db::cycles;
use db::log::*;
use db::e2d2::scheduler::*;
use db::e2d2::allocators::CacheAligned;
use db::e2d2::interface::PortQueue;
use db::wireformat::{GetResponse, InvokeResponse};

use rand::distributions::Sample;
use rand::{SeedableRng, XorShiftRng};

use zipf::ZipfDistribution;

/// This type implements the send half of a client that issues back to back reads to a server and
/// aggregates the returned value into a single 64 bit integer.
struct AggregateSend {
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

    /// The time interval between two requests (in cycles).
    delay: u64,

    /// The time (in cycles) at which the workload started generating requests.
    start: u64,

    /// The time (in cycles) at which the next request should be issued/generated.
    next: u64,

    /// Network stack that can actually send an RPC over the network.
    sender: dispatch::Sender,

    /// Request buffer for a native get operation. Helps reduce heap allocations.
    n_buff: Vec<u8>,

    /// Request buffer for an invoke operation. Again, helps reduce heap allocations.
    i_buff: Vec<u8>,
}

// Implementation of methods on AggregateSend.
impl AggregateSend {
    /// Constructs an `AggregateSend` that can be added to a Netbricks pipeline.
    ///
    /// # Arguments
    ///
    /// * `config`:    Client configuration with Workload related (key and value length etc.) as
    ///                well as network related (Server and Client MAC address etc.) parameters.
    /// * `port`:      Network port over which requests will be sent out.
    /// * `reqs`:      The number of requests to be issued to the server.
    /// * `dst_ports`: The total number of UDP ports the server is listening on.
    pub fn new(
        config: &config::ClientConfig,
        port: CacheAligned<PortQueue>,
        reqs: u64,
        dst_ports: u16,
    ) -> AggregateSend {
        // Allocate a vector for the invoke() RPC's payload. The payload consists of the name of
        // the extension, the table id (8 bytes), and the key length.
        let len = "aggregate".as_bytes().len() + size_of::<u64>() + config.key_len;
        let mut i_buff = Vec::with_capacity(len);

        // Pre-populate the extension name and table id.
        i_buff.extend_from_slice("aggregate".as_bytes());
        i_buff.extend_from_slice(&unsafe { transmute::<u64, [u8; 8]>(1u64.to_le()) });
        i_buff.resize(len, 0);

        // Allocate and init a buffer into which keys will be generated.
        let mut n_buff = Vec::with_capacity(config.key_len);
        n_buff.resize(config.key_len, 0);

        AggregateSend {
            random: XorShiftRng::from_seed(rand::random::<[u32; 4]>()),
            k_dist: ZipfDistribution::new(config.n_keys, config.skew)
                .expect("Failed to init key generator."),
            t_dist: ZipfDistribution::new(config.num_tenants as usize, config.tenant_skew)
                .expect("Failed to init tenant generator."),
            native: !config.use_invoke,
            requests: reqs,
            sent: 0,
            delay: cycles::cycles_per_second() / config.req_rate as u64,
            start: cycles::rdtsc(),
            next: 0,
            sender: dispatch::Sender::new(config, port, dst_ports),
            n_buff: n_buff,
            i_buff: i_buff,
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
                self.i_buff[17..21].copy_from_slice(&k);
                self.sender.send_invoke(t, 9, &self.i_buff, curr);
            }
        }
    }
}

// Implementation of the Executable trait so that we can use Netbrick's DPDK bindings.
impl Executable for AggregateSend {
    fn execute(&mut self) {
        // Return if there are no more requests to generate.
        if self.requests <= self.sent {
            return;
        }

        // Send out a request at the configured request rate.
        let curr = cycles::rdtsc();
        if curr >= self.next || self.next == 0 {
            self.generate(curr);

            self.sent += 1;
            self.next = self.start + self.sent * self.delay;
        }
    }

    fn dependencies(&mut self) -> Vec<usize> {
        vec![]
    }
}

/// This type implements the receive half of a client that issues back to back reads to a
/// server and aggregates the returned value into a single 64 bit integer.
struct AggregateRecv {
    /// The network stack required to receives RPC response packets from a network port.
    receiver: dispatch::Receiver<CacheAligned<PortQueue>>,

    /// Flag indicating whether requests sent out were native (true) or invocations (false).
    native: bool,

    /// The number of response packets to wait for before printing out statistics.
    responses: u64,

    /// Time stamp in cycles at which measurement started. Required to calculate observed
    /// throughput of the Sandstorm server.
    start: u64,

    /// The total number of responses received so far.
    recvd: u64,

    /// Vector of sampled request latencies. Required to calculate distributions once all responses
    /// have been received.
    latencies: Vec<u64>,
}

// Implementation of methods on AggregateRecv.
impl AggregateRecv {
    /// Returns an `AggregateRecv`.
    ///
    /// # Arguments
    ///
    /// * `port` :  Network port on which responses will be polled for.
    /// * `resps`:  The number of responses to wait for before calculating statistics.
    /// * `native`: Boolean indicating whether responses are for native (true) RPCs or
    ///             invoke (false) RPCs
    ///
    /// # Return
    ///
    /// A receiver that measures the median latency and throughput of a Sandstorm server.
    fn new(port: CacheAligned<PortQueue>, resps: u64, native: bool) -> AggregateRecv {
        AggregateRecv {
            receiver: dispatch::Receiver::new(port),
            native: native,
            responses: resps,
            start: cycles::rdtsc(),
            recvd: 0,
            latencies: Vec::with_capacity(2 * 1000 * 1000),
        }
    }

    /// Sums up the contents of a byte array.
    ///
    /// # Arguments
    ///
    /// * `init`: Initial value to be used in the summation.
    /// * `vec`:  The byte array whose contents need to be summed up.
    fn aggregate(init: u64, vec: &[u8]) -> u64 {
        vec.iter().fold(init, |sum, e| sum + (*e as u64))
    }

    /// Prints out the measured latency distribution and throughput.
    fn measurements(&mut self) {
        let stop = cycles::rdtsc();

        self.latencies.sort();
        let median = self.latencies[self.latencies.len() / 2];
        let tail = self.latencies[(self.latencies.len() * 99) / 100];

        info!(
            "Median(ns): {} Tail(ns): {} Throughput(Kops/s): {}",
            cycles::to_seconds(median) * 1e9,
            cycles::to_seconds(tail) * 1e9,
            self.recvd as f64 / cycles::to_seconds(stop - self.start)
        );
    }
}

// Implementation of the Executable trait so that we can use Netbrick's DPDK bindings.
impl Executable for AggregateRecv {
    fn execute(&mut self) {
        // Do nothing if all responses have been received.
        if self.responses <= self.recvd {
            return;
        }

        // Check for received packets. If any, then take latency measurements.
        if let Some(mut resps) = self.receiver.recv_res() {
            while let Some(packet) = resps.pop() {
                self.recvd += 1;

                if self.native {
                    // If the response is for a native request, then first aggregate and then
                    // take a latency measurement (if required).
                    let p = packet.parse_header::<GetResponse>();
                    let s = Self::aggregate(0, p.get_payload());
                    if self.recvd & 0xf == 0 {
                        self.latencies
                            .push(cycles::rdtsc() - p.get_header().common_header.stamp);
                    }
                } else {
                    let p = packet.parse_header::<InvokeResponse>();
                    if self.recvd & 0xf == 0 {
                        self.latencies
                            .push(cycles::rdtsc() - p.get_header().common_header.stamp);
                    }
                }
            }
        }

        // Print out measurements after all responses have been received.
        if self.responses <= self.recvd {
            self.measurements();
        }
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
    match scheduler.add_task(AggregateSend::new(
        config,
        ports[0].clone(),
        config.num_reqs as u64,
        config.server_udp_ports as u16,
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

/// Sets up AggregateRecv by adding it to a Netbricks scheduler.
///
/// # Arguments
///
/// * `ports`:     Network port on which packets will be sent.
/// * `scheduler`: Netbricks scheduler to which AggregateRecv will be added.
/// * `native`:    Boolean indicating whether responses are for native (true) or invoke (false)
///                RPCs.
fn setup_recv<S>(ports: Vec<CacheAligned<PortQueue>>, scheduler: &mut S, _core: i32, native: bool)
where
    S: Scheduler + Sized,
{
    if ports.len() != 1 {
        error!("Client should be configured with exactly 1 port!");
        std::process::exit(1);
    }

    // Add the receiver to a netbricks pipeline.
    match scheduler.add_task(AggregateRecv::new(
        ports[0].clone(),
        32 * 1000 * 1000 as u64,
        native,
    )) {
        Ok(_) => {
            info!(
                "Successfully added AggregateRecv with rx queue {}.",
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

    // Required by AggregateRecv.
    let native = config.use_invoke;

    // Setup 4 senders and 4 receivers.
    for i in 0..4 {
        // First, retrieve a tx-rx queue pair from Netbricks
        let port = net_context
            .rx_queues
            .get(&senders[i])
            .expect("Failed to retrieve network port!")
            .clone();

        // Setup the receive side.
        net_context
            .add_pipeline_to_core(
                receive[i],
                Arc::new(
                    move |_ports, sched: &mut StandaloneScheduler, core: i32, _sibling| {
                        setup_recv(port.clone(), sched, core, native)
                    },
                ),
            )
            .expect("Failed to initialize receive side.");

        // Setup the send side.
        net_context
            .add_pipeline_to_core(
                senders[i],
                Arc::new(
                    move |ports, sched: &mut StandaloneScheduler, core: i32, _sibling| {
                        setup_send(&config::ClientConfig::load(), ports, sched, core)
                    },
                ),
            )
            .expect("Failed to initialize send side.");
    }

    // Run the client.
    net_context.execute();

    // Sleep for an amount of time approximately equal to the estimated execution time, and then
    // shutdown the client.
    std::thread::sleep(std::time::Duration::from_secs(exec as u64 + 10));

    // Stop the client.
    net_context.stop();
}
