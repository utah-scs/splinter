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
extern crate splinter;

mod setup;

use std::fmt::Display;
use std::fs::File;
use std::io::{Read, Write};
use std::mem::{size_of, transmute};
use std::net::{Shutdown, TcpStream};
use std::sync::Arc;

use db::config;
use db::e2d2::allocators::*;
use db::e2d2::interface::*;
use db::e2d2::scheduler::*;
use db::log::*;
use db::wireformat::InstallRequest;

use splinter::*;

/// Send side logic for a simple client that issues put() and get() requests.
struct SanitySend {
    // RPC request generator required to send RPC requests to a Sandstorm server.
    sender: dispatch::Sender,

    // Number of put() requests remaining to be issued. Once all of these have been issued,
    // SanitySend will flip over to get() requests.
    puts: u64,

    // Number of get() requests remaining to be issued. Once all of these have been issued,
    // SanitySend will stop issuing RPC requests.
    gets: u64,

    // If true, native get() and put() requests are sent out. If false, invoke based requests are
    // sent out.
    native: bool,

    // Server network endpoint listening for install() RPCs.
    install_addr: String,
}

// Implementation of methods on SanitySend.
impl SanitySend {
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
    fn new(config: &config::ClientConfig, port: CacheAligned<PortQueue>) -> SanitySend {
        SanitySend {
            sender: dispatch::Sender::new(config, port, 1),
            puts: 1 * 1000,
            gets: 1 * 1000,
            native: !config.use_invoke,
            install_addr: config.install_addr.clone(),
        }
    }
}

// Executable trait allowing SanitySend to be scheduled on Netbricks.
impl Executable for SanitySend {
    /// Called by a Netbricks scheduler.
    fn execute(&mut self) {
        // Throttle. Sleep for 1 micro-second before issuing a request.
        std::thread::sleep(std::time::Duration::from_micros(1000));

        // If using invokes, then first install a get and put extension.
        let install: bool = !self.native && (self.puts == 1000) && (self.gets == 1000);

        if install {
            // First, open the get() extension and read it into a buffer.
            let mut buf: Vec<u8> = Vec::new();
            let mut get = File::open("../ext/get/target/release/libget.so")
                .expect("Failed to open .so for install.");
            let _ = get.read_to_end(&mut buf);

            // Next, construct the RPC (header and payload).
            let hdr = InstallRequest::new(100, 4, buf.len() as u32, 0);
            let hdr: [u8; size_of::<InstallRequest>()] = unsafe { transmute(hdr) };
            let mut req: Vec<u8> = Vec::new();
            req.extend_from_slice(&hdr);
            req.extend_from_slice("iget".as_bytes());
            req.append(&mut buf);

            // Send the RPC to the server.
            let mut stream = TcpStream::connect(self.install_addr.clone())
                .expect("Failed to connect to server for install.");
            stream
                .write_all(&req)
                .expect("Failed to send install to server.");
            stream
                .flush()
                .expect("Failed to flush install RPC on server connection.");
            stream
                .shutdown(Shutdown::Write)
                .expect("Failed to stop writes on stream.");

            // Wait for a response from the server.
            let mut res: Vec<u8> = Vec::new();
            stream
                .read_to_end(&mut res)
                .expect("Failed to read install response from server.");
        }

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
                payload.extend_from_slice("iget".as_bytes()); // Name
                payload.extend_from_slice(&table); // Table Id
                payload.extend_from_slice(&temp); // Key
                self.sender.send_invoke(100, 4, &payload, self.gets);
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
fn setup_recv<T, S>(ports: Vec<T>, scheduler: &mut S, _core: i32)
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
    let mut net_context = setup::config_and_init_netbricks(&config);

    // Setup the client pipeline.
    net_context.start_schedulers();

    // Retrieve one port-queue from Netbricks, and setup the Send side.
    let port = net_context
        .rx_queues
        .get(&0)
        .expect("Failed to retrieve network port!")
        .clone();

    // Setup the send side on core 0.
    net_context
        .add_pipeline_to_core(
            0,
            Arc::new(
                move |_ports, sched: &mut StandaloneScheduler, core: i32, _sibling| {
                    setup_send(&config, port.clone(), sched, core)
                },
            ),
        )
        .expect("Failed to initialize send side.");

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
            Arc::new(
                move |_ports, sched: &mut StandaloneScheduler, core: i32, _sibling| {
                    setup_recv(port.clone(), sched, core)
                },
            ),
        )
        .expect("Failed to initialize receive side.");

    // Run the client.
    net_context.execute();

    loop {}

    // Stop the client.
    // net_context.stop();
}
