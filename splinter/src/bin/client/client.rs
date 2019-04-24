fn main() {
    db::env_logger::init().expect("ERROR: failed to initialize logger!");

    let config = config::ClientConfig::load();
    info!("Starting up Sandstorm client with config {:?}", config);

    let masterservice = Arc::new(Master::new());

    // Create tenants with extensions.
    info!(
        "Overridden key-length {}, Value length {}",
        KEY_LENGTH, VAL_LENGTH
    );
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