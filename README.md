# Sandstorm
A multi-tenant key-value store into which untrusted extensions can be pushed
down at run-time

## Running Sandstorm on CloudLab
To run Sandstorm on [CloudLab](https://www.cloudlab.us/login.php), do the
following

1. Instantiate an experiment using the `sandstorm-cluster` profile under the
   `sandstorm` project (you will need to join the project first). When asked,
   allocate two machines for the experiment.
2. Once the experiment has begun, clone this repository on both the allocated
   machines.
3. Next, run `scripts/setup.py --full` from the root directory on both machines.
   This script installs Rust, DPDK, and binds a 10 GbE network card to DPDK.
   It saves the PCI and MAC addresses of the bound NIC to a file called `nic_info`.
   Run - `source $HOME/.cargo/env` once the script is done.
4. Create a toml file (`db/server.toml`) for the server on one machine, and a
   toml file (`db/client.toml`) for the client on the other machine. There are
   example files under `db`.
5. Update the toml files with the correct MAC and PCI addresses (generated
   during Step 3).
6. To run the server, run `make`, followed by `sudo scripts/run-server' from the
   root directory.
7. To run the ycsb client, run `make`, followed by `sudo scripts/run-ycsb` from
   the root directory.
