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
#![feature(integer_atomics)]
#![feature(duration_from_micros)]

extern crate db;

mod setup;
mod dispatch;

use std::sync::Arc;
use std::fmt::Display;
use std::mem::transmute;

use db::config;
use db::log::*;
use db::e2d2::interface::*;
use db::e2d2::scheduler::*;

/// Send side logic for a simple client that issues put() and get() requests.
struct SanitySend<T>
where
    T: PacketTx + PacketRx + Display + Clone + 'static,
{
    // RPC request generator required to send RPC requests to a Sandstorm server.
    sender: dispatch::Sender<T>,

    // Number of put() requests remaining to be issued. Once all of these have been issued,
    // SanitySend will flip over to get() requests.
    puts: u64,

    // Number of get() requests remaining to be issued. Once all of these have been issued,
    // SanitySend will stop issuing RPC requests.
    gets: u64,

    // If true, native get() and put() requests are sent out. If false, invoke based requests are
    // sent out.
    native: bool,
}

// Implementation of methods on SanitySend.
impl<T> SanitySend<T>
where
    T: PacketTx + PacketRx + Display + Clone + 'static,
{
    /// Constructs a SanitySend.
    ///
    /// # Arguments
    ///
    /// * `config`: Network related configuration such as the MAC and IP address.
    /// * `port`:   Network port on which packets will be sent.
    ///
    /// # Return
    ///
    /// A SanitySend that can issue simple get() and put() RPCs to a remote Sandstorm server.
    fn new(config: &config::ClientConfig, port: T) -> SanitySend<T> {
        SanitySend {
            sender: dispatch::Sender::new(config, port),
            puts: 1 * 1000,
            gets: 1 * 1000,
            native: !config.use_invoke,
        }
    }
}

// Executable trait allowing SanitySend to be scheduled on Netbricks.
impl<T> Executable for SanitySend<T>
where
    T: PacketTx + PacketRx + Display + Clone + 'static,
{
    /// Called by a Netbricks scheduler.
    fn execute(&mut self) {
        // Throttle. Sleep for 1 micro-second before issuing a request.
        std::thread::sleep(std::time::Duration::from_micros(1));

        // If there are pending puts, issue one and return.
        if self.puts > 0 {
            // Determine the key and value to be inserted into the database.
            let temp = self.puts;
            let temp: [u8; 8] = unsafe { transmute(temp.to_le()) };

            // Send out either a put() or invoke().
            if self.native == true {
                self.sender.send_put(100, 100, &temp, &temp, self.puts);
            } else {
                let mut payload = Vec::new();
                let table: [u8; 8] = unsafe { transmute(100u64.to_le()) };
                payload.extend_from_slice("put".as_bytes()); // Name
                payload.extend_from_slice(&table); // Table Id
                payload.extend_from_slice(&[8, 0]); // Key Length
                payload.extend_from_slice(&temp); // Key
                payload.extend_from_slice(&temp); // Value
                self.sender.send_invoke(100, 3, &payload, self.puts);
            }

            self.puts -= 1;
            return;
        }

        // If there are no pending puts but there are pending gets, then issue one and return.
        if self.gets > 0 {
            let temp = self.gets;
            let temp: [u8; 8] = unsafe { transmute(temp.to_le()) };

            // Send out either a get() or invoke().
            if self.native == true {
                self.sender.send_get(100, 100, &temp, self.gets);
            } else {
                let mut payload = Vec::new();
                let table: [u8; 8] = unsafe { transmute(100u64.to_le()) };
                payload.extend_from_slice("get".as_bytes()); // Name
                payload.extend_from_slice(&table); // Table Id
                payload.extend_from_slice(&temp); // Key
                self.sender.send_invoke(100, 3, &payload, self.gets);
            }

            self.gets -= 1;
            return;
        }
    }

    fn dependencies(&mut self) -> Vec<usize> {
        vec![]
    }
}

/// Receive side logic for a client that issues simple get() and put() RPC requests.
struct SanityRecv<T>
where
    T: PacketTx + PacketRx + Display + Clone + 'static,
{
    // Receiver of responses to RPC requests issued by SanitySend.
    receiver: dispatch::Receiver<T>,
}

// Implementation of methods on SanityRecv.
impl<T> SanityRecv<T>
where
    T: PacketTx + PacketRx + Display + Clone + 'static,
{
    /// Constructs a SanityRecv.
    ///
    /// # Arguments
    ///
    /// * `port`: Network port over which responses will be received. Required by the receiver.
    ///
    /// # Return
    ///
    /// A SanityRecv capable of receiving responses to RPC requests generated by SanitySend.
    fn new(port: T) -> SanityRecv<T> {
        SanityRecv {
            receiver: dispatch::Receiver::new(port),
        }
    }
}

// Executable trait allowing SanityRecv to be scheduled by Netbricks.
impl<T> Executable for SanityRecv<T>
where
    T: PacketTx + PacketRx + Display + Clone + 'static,
{
    // Called internally by a Netbricks scheduler.
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

/// Sets up SanitySend by adding it to a Netbricks scheduler.
///
/// # Arguments
///
/// * `config`:    Network related configuration such as the MAC and IP address.
/// * `ports`:     Network port on which packets will be sent.
/// * `scheduler`: Netbricks scheduler to which SanitySend will be added.
fn setup_send<T, S>(config: &config::ClientConfig, ports: Vec<T>, scheduler: &mut S)
where
    T: PacketTx + PacketRx + Display + Clone + 'static,
    S: Scheduler + Sized,
{
    if ports.len() != 1 {
        error!("Client should be configured with exactly 1 port!");
        std::process::exit(1);
    }

    // Add the sender to a netbricks pipeline.
    match scheduler.add_task(SanitySend::new(config, ports[0].clone())) {
        Ok(_) => {
            info!("Successfully added SanitySend to a Netbricks pipeline.");
        }

        Err(ref err) => {
            error!("Error while adding to Netbricks pipeline {}", err);
            std::process::exit(1);
        }
    }
}

/// Sets up SanityRecv by adding it to a Netbricks scheduler.
///
/// # Arguments
///
/// * `ports`:     Network port on which packets will be sent.
/// * `scheduler`: Netbricks scheduler to which SanityRecv will be added.
fn setup_recv<T, S>(ports: Vec<T>, scheduler: &mut S)
where
    T: PacketTx + PacketRx + Display + Clone + 'static,
    S: Scheduler + Sized,
{
    if ports.len() != 1 {
        error!("Client should be configured with exactly 1 port!");
        std::process::exit(1);
    }

    // Add the receiver to a netbricks pipeline.
    match scheduler.add_task(SanityRecv::new(ports[0].clone())) {
        Ok(_) => {
            info!("Successfully added SanityRecv to a Netbricks pipeline.");
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
    let mut net_context = setup::config_and_init_netbricks();

    // Setup the client pipeline.
    net_context.start_schedulers();

    // Retrieve one port-queue from Netbricks, and setup the Send side.
    let port = net_context
        .rx_queues
        .get(&2)
        .expect("Failed to retrieve network port!")
        .clone();

    // Setup the send side on core 2.
    net_context
        .add_pipeline_to_core(
            2,
            Arc::new(move |_ports, sched: &mut StandaloneScheduler| {
                setup_send(&config, port.clone(), sched)
            }),
        )
        .expect("Failed to initialize send side.");

    // Retrieve one port-queue from Netbricks, and setup the Receive side.
    let port = net_context
        .rx_queues
        .get(&2)
        .expect("Failed to retrieve network port!")
        .clone();

    // Setup the receive side on core 4.
    net_context
        .add_pipeline_to_core(
            4,
            Arc::new(move |_ports, sched: &mut StandaloneScheduler| {
                setup_recv(port.clone(), sched)
            }),
        )
        .expect("Failed to initialize receive side.");

    // Run the client.
    net_context.execute();

    loop {}

    // Stop the client.
    // net_context.stop();
}
