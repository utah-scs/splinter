/* Copyright (c) 2017 University of Utah
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

use std::sync::Arc;

use db::log::*;

use db::e2d2::scheduler::*;
use db::e2d2::interface::*;
use db::e2d2::scheduler::Executable;
use db::e2d2::allocators::CacheAligned;
use db::e2d2::scheduler::NetBricksContext as NetbricksContext;
use db::e2d2::config::{NetbricksConfiguration, PortConfiguration};

use db::config;
use db::master::Master;
use db::sched::RoundRobin;
use db::dispatch::Dispatch;

/// A simple wrapper around the scheduler, allowing it to be added to a Netbricks pipeline.
struct Server {
    scheduler: Arc<RoundRobin>,
}

// Implementation of methods on Server.
impl Server {
    /// Creates a Server.
    ///
    /// # Arguments
    ///
    /// * `sched`: A scheduler of tasks in the database. This scheduler must already have a
    ///            `Dispatch` task enqueued on it.
    ///
    /// # Return
    ///
    /// A Server that can be added to a Netbricks pipeline.
    pub fn new(sched: Arc<RoundRobin>) -> Server {
        Server { scheduler: sched }
    }
}

// Implementation of the executable trait, allowing Server to be passed into Netbricks.
impl Executable for Server {
    /// This function is called internally by Netbricks to "execute" the server.
    fn execute(&mut self) {
        // Never return back to Netbricks.
        loop {
            self.scheduler.poll();
        }
    }

    /// No clue about what this guy is meant to do.
    fn dependencies(&mut self) -> Vec<usize> {
        vec![]
    }
}

/// This function sets up a Sandstorm server's dispatch thread on top
/// of Netbricks.
fn setup_server<S>(
    config: &config::ServerConfig,
    ports: Vec<CacheAligned<PortQueue>>,
    scheduler: &mut S,
    master: &Arc<Master>,
) where
    S: Scheduler + Sized,
{
    if ports.len() != 1 {
        error!("Server should be configured with exactly 1 port!");
        std::process::exit(1);
    }

    // Create a scheduler and a dispatcher for the server.
    let sched = Arc::new(RoundRobin::new());
    let dispatch = Dispatch::new(
        config,
        ports[0].clone(),
        Arc::clone(master),
        Arc::clone(&sched),
        ports[0].rxq(),
    );
    sched.enqueue(Box::new(dispatch));

    // Add the server to a netbricks pipeline.
    match scheduler.add_task(Server::new(sched)) {
        Ok(_) => {
            info!(
                "Successfully added scheduler with rx,tx queues {:?}.",
                (ports[0].rxq(), ports[0].txq())
            );
        }

        Err(ref err) => {
            error!("Error while adding to Netbricks pipeline {}", err);
            std::process::exit(1);
        }
    }
}

/// Returns a struct of type NetbricksConfiguration which can be used to
/// initialize Netbricks with a default set of parameters.
///
/// If used to initialize Netbricks, this struct will run the parent server
/// thread on core 0, and one scheduler on core 1. Packet buffers will be
/// allocated from a 2 GB memory pool, with 64 MB cached at core 1. DPDK will
/// be initialized as a primary process without any additional arguments. A
/// single network interface/port with 1 transmit queue, 1 receive queue, 256
/// receive descriptors, and 256 transmit descriptors will be made available to
/// Netbricks. Loopback, hardware transmit segementation offload, and hardware
/// checksum offload will be disabled on this port.
fn get_default_netbricks_config(config: &config::ServerConfig) -> NetbricksConfiguration {
    // General arguments supplied to netbricks.
    let net_config_name = String::from("server");
    let dpdk_secondary: bool = false;
    let net_primary_core: i32 = 31;
    let net_cores: Vec<i32> = vec![0, 2, 4, 6, 8, 10, 12, 14];
    let net_strict_cores: bool = true;
    let net_pool_size: u32 = 8192 - 1;
    let net_cache_size: u32 = 128;
    let net_dpdk_args: Option<String> = None;

    // Port configuration. Required to configure the physical network interface.
    let net_port_name = config.nic_pci.clone();
    let net_port_rx_queues: Vec<i32> = net_cores.clone();
    let net_port_tx_queues: Vec<i32> = net_cores.clone();
    let net_port_rxd: i32 = 256;
    let net_port_txd: i32 = 256;
    let net_port_loopback: bool = false;
    let net_port_tcp_tso: bool = false;
    let net_port_csum_offload: bool = false;

    let net_port_config = PortConfiguration {
        name: net_port_name,
        rx_queues: net_port_rx_queues,
        tx_queues: net_port_tx_queues,
        rxd: net_port_rxd,
        txd: net_port_txd,
        loopback: net_port_loopback,
        tso: net_port_tcp_tso,
        csum: net_port_csum_offload,
    };

    // The set of ports used by netbricks.
    let net_ports: Vec<PortConfiguration> = vec![net_port_config];

    NetbricksConfiguration {
        name: net_config_name,
        secondary: dpdk_secondary,
        primary_core: net_primary_core,
        cores: net_cores,
        strict: net_strict_cores,
        ports: net_ports,
        pool_size: net_pool_size,
        cache_size: net_cache_size,
        dpdk_args: net_dpdk_args,
    }
}

/// This function configures and initializes Netbricks. In the case of a
/// failure, it causes the program to exit.
///
/// Returns a Netbricks context which can be used to setup and start the
/// server/client.
fn config_and_init_netbricks(config: &config::ServerConfig) -> NetbricksContext {
    let net_config: NetbricksConfiguration = get_default_netbricks_config(config);

    // Initialize Netbricks and return a handle.
    match initialize_system(&net_config) {
        Ok(net_context) => {
            return net_context;
        }

        Err(ref err) => {
            error!("Error during Netbricks init: {}", err);
            // TODO: Drop NetbricksConfiguration?
            std::process::exit(1);
        }
    }
}

fn main() {
    // Basic setup and initialization.
    db::env_logger::init().expect("ERROR: failed to initialize logger!");

    let config = config::ServerConfig::load();
    info!("Starting up Sandstorm server with config {:?}", config);

    // Setup Netbricks.
    let mut net_context: NetbricksContext = config_and_init_netbricks(&config);

    let master = Arc::new(Master::new());
    info!("Populating test data table and extensions...");

    // Create tenants with data and extensions for YCSB.
    for tenant in 0..config.num_tenants {
        master.fill_test(tenant, 1, 10 * 1000 * 1000);
        master.load_test(tenant);
    }

    // Create tenants with data and extensions for Sanity 
    master.load_test(100);
    master.fill_test(100, 100, 0);

    info!("Finished populating data and extensions");

    // Setup the server pipeline.
    net_context.start_schedulers();
    net_context.add_pipeline_to_run(Arc::new(
        move |ports, scheduler: &mut StandaloneScheduler| {
            setup_server(&config, ports, scheduler, &master)
        },
    ));

    // Run the server.
    net_context.execute();

    loop {}

    // Stop the server.
    // net_context.stop();
}
