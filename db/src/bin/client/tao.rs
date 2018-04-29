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
extern crate zipf;

mod setup;
mod dispatch;

use std::sync::Arc;
use std::fmt::Display;
use std::mem::transmute;

use db::cycles;
use db::config;
use db::log::*;
use db::e2d2::interface::*;
use db::e2d2::scheduler::*;
use db::e2d2::allocators::*;


/// Sends out Tao based RPC requests to a Sandstorm server.
struct TaoSend {
    // Network stack required to actually send RPC requests out the network.
    sender: dispatch::Sender,
}

// Implementation of methods on TaoSend.
impl TaoSend {
    /// Constructs a TaoSend.
    ///
    /// # Arguments
    ///
    /// * `config`:  Client configuration with Tao related (key and value length etc.) as well as
    ///              Network related (Server and Client MAC address etc.) parameters.
    /// * `port`:    Network port over which requests will be sent out.
    /// * `reqs`:    The number of requests to be issued to the server.
    /// * `udp_dst`: The UDP destination port to send packets to.
    ///
    /// # Return
    ///
    /// A Tao request generator.
    fn new(config: &config::ClientConfig, port: CacheAligned<PortQueue>) -> TaoSend {
        TaoSend {
            sender: dispatch::Sender::new(config, port, 0),
        }
    }
}

// The Executable trait allowing TaoSend to be scheduled by Netbricks.
impl Executable for TaoSend {
    // Called internally by Netbricks.
    fn execute(&mut self) {

        // Get the current time stamp so that we can determine if it is time to issue the next RPC.
        let curr = cycles::rdtsc();

        let mut payload = Vec::new();
        //Object add = |opcode = 1|table_id = 8|obj_type = 2|value = n > 0|
        let table_id: [u8; 8] = unsafe { transmute(200000u64.to_le()) };
        let obj_type: [u8; 2] = unsafe { transmute(1u16.to_le()) };
        let value: [u8; 8] = unsafe { transmute(2u64.to_le()) };
        let opcode: [u8; 1] = unsafe { transmute(0u8.to_le()) };
        payload.extend_from_slice("tao".as_bytes());
        payload.extend_from_slice(&opcode);
        payload.extend_from_slice(&table_id);
        payload.extend_from_slice(&obj_type);
        payload.extend_from_slice(&value);
        self.sender.send_invoke(1, 3, &payload, curr)
    }

    fn dependencies(&mut self) -> Vec<usize> {
        vec![]
    }
}

/// Receives responses to Tao requests sent out by TaoSend.
struct TaoRecv<T>
where
    T: PacketTx + PacketRx + Display + Clone + 'static,
{
    // The network stack required to receives RPC response packets from a network port.
    receiver: dispatch::Receiver<T>
}

// Implementation of methods on TaoRecv.
impl<T> TaoRecv<T>
where
    T: PacketTx + PacketRx + Display + Clone + 'static,
{
    /// Constructs a TaoRecv.
    ///
    /// # Arguments
    ///
    /// * `port` : Network port on which responses will be polled for.
    ///
    /// # Return
    ///
    /// A Tao response receiver that measures the median latency and throughput of a Sandstorm
    /// server.
    fn new(port: T, resps: u64) -> TaoRecv<T> {
        TaoRecv {
            receiver: dispatch::Receiver::new(port),
        }
    }
}

// Executable trait allowing TaoRecv to be scheduled by Netbricks.
impl<T> Executable for TaoRecv<T>
where
    T: PacketTx + PacketRx + Display + Clone + 'static,
{
    // Called internally by Netbricks.
    fn execute(&mut self) {
        // If there are response packets at the network port, print out their contents,
        // and free them.
        if let Some(mut packets) = self.receiver.recv_res() {
            while let Some(packet) = packets.pop() {
                println!("Response: {:?}", packet.get_payload());
                packet.free_packet();
            }
        }
    }

    fn dependencies(&mut self) -> Vec<usize> {
        vec![]
    }
}

/// Sets up TaoSend by adding it to a Netbricks scheduler.
///
/// # Arguments
///
/// * `config`:    Network related configuration such as the MAC and IP address.
/// * `ports`:     Network port on which packets will be sent.
/// * `scheduler`: Netbricks scheduler to which TaoSend will be added.
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
    match scheduler.add_task(TaoSend::new(config, ports[0].clone())) {
        Ok(_) => {
            info!("Successfully added TaoSend to a Netbricks pipeline.");
        }

        Err(ref err) => {
            error!("Error while adding TaoSend to Netbricks pipeline {}", err);
            std::process::exit(1);
        }
    }
}

/// Sets up TaoRecv by adding it to a Netbricks scheduler.
///
/// # Arguments
///
/// * `ports`:     Network port on which packets will be sent.
/// * `scheduler`: Netbricks scheduler to which TaoRecv will be added.
fn setup_recv<S>(ports: Vec<CacheAligned<PortQueue>>, scheduler: &mut S, _core: i32)
where
    S: Scheduler + Sized,
{
    if ports.len() != 1 {
        error!("Client should be configured with exactly 1 port!");
        std::process::exit(1);
    }

    // Add the receiver to a netbricks pipeline.
    match scheduler.add_task(TaoRecv::new(ports[0].clone(), 16 * 1000 * 1000 as u64)) {
        Ok(_) => {
            info!("Successfully added TaoRecv to a Netbricks pipeline.");
        }

        Err(ref err) => {
            error!("Error while adding TaoRecv to Netbricks pipeline {}", err);
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

    // Retrieve one port-queue from Netbricks, and setup the Receive side.
    let port = net_context
        .rx_queues
        .get(&0)
        .expect("Failed to retrieve network port!")
        .clone();

    // Setup the receive side on core 2.
    net_context
        .add_pipeline_to_core(
            2,
            Arc::new(move |_ports, sched: &mut StandaloneScheduler, core: i32| {
                setup_recv(port.clone(), sched, core)
            }),
        )
        .expect("Failed to initialize receive side.");

    // Setup the send side on core 0.
    net_context
        .add_pipeline_to_core(
            0,
            Arc::new(move |ports, sched: &mut StandaloneScheduler, core: i32| {
                setup_send(&config, ports, sched, core)
            }),
        )
        .expect("Failed to initialize send side.");

    // Run the client.
    net_context.execute();

    // Sleep for an amount of time approximately equal to the estimated execution time, and then
    // shutdown the client.
    std::thread::sleep(std::time::Duration::from_secs(exec as u64 + 10));

    // Stop the client.
    net_context.stop();
}

#[cfg(test)]
mod test {
    use std;
    use std::thread;
    use std::mem::transmute;
    use std::time::{Duration, Instant};
    use std::sync::{Arc, Mutex};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn Tao_abc_basic() {
        let n_threads = 1;
        let mut threads = Vec::with_capacity(n_threads);
        let done = Arc::new(AtomicBool::new(false));

        for _ in 0..n_threads {
            let done = done.clone();
            threads.push(thread::spawn(move || {
                let mut b = super::Tao::new(10, 100, 1000000, 5, 0.99);
                let mut n_gets = 0u64;
                let mut n_puts = 0u64;
                let start = Instant::now();
                while !done.load(Ordering::Relaxed) {
                    b.abc(|_key| n_gets += 1, |_key, _value| n_puts += 1);
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
        let k: u32 = 0 | key[0] as u32 | (key[1] as u32) << 8 | (key[2] as u32) << 16
            | (key[3] as u32) << 24;
        k
    }

    #[test]
    fn Tao_abc_histogram() {
        let hist = Arc::new(Mutex::new(HashMap::new()));

        let n_keys = 20;
        let n_threads = 1;

        let mut threads = Vec::with_capacity(n_threads);
        let done = Arc::new(AtomicBool::new(false));
        for _ in 0..n_threads {
            let hist = hist.clone();
            let done = done.clone();
            threads.push(thread::spawn(move || {
                let mut b = super::Tao::new(4, 100, n_keys, 5, 0.99);
                let mut n_gets = 0u64;
                let mut n_puts = 0u64;
                let start = Instant::now();
                while !done.load(Ordering::Relaxed) {
                    b.abc(
                        |key| {
                            // get
                            let k = convert_key(key);
                            let mut ht = hist.lock().unwrap();
                            ht.entry(k).or_insert((0, 0)).0 += 1;
                            n_gets += 1
                        },
                        |key, _value| {
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
        let v: Vec<_> = kvs.iter()
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
