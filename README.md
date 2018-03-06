# Sandstorm
A multi-tenant key-value store into which untrusted extensions can be pushed
down at run-time

## Running Sandstorm on CloudLab
To run Sandstorm on [CloudLab](https://www.cloudlab.us/login.php), do the
following

1. Instantiate an experiment using the `sandstorm-test` profile under the
   `Sandstorm` project (you will need to join the project first). When asked,
   allocate two machines for the experiment.
2. Once the experiment has begun, clone this repository on both the allocated
   machines.
3. Install Rust and DPDK by running `scripts/setup.py`. **This script needs to
   be run twice. The first run installs stable Rust and dumps a few instructions
   on the screen. After following those instructions, the second run switches
   over to nightly Rust, and compiles DPDK.**
4. Load the `uio` and the `igb_uio.ko` kernel modules. `uio` can be loaded using
   `modprobe`, and `igb_uio.ko` can be loaded using `lsmod` and found under
   `net/3rdparty/dpdk/build/kmod`.
5. Next, run the `dpdk-devbind.py` script under `net/3rdparty/dpdk/usertools.py`
   on both machines to bind a NIC to the DPDK driver. Running 
   `dpdk-devbind.py --status-dev=net` will display the different NICs available.
   Pass the PCI-ID of the 10 GigE NIC marked as `*Active*` to
   `dpdk-devbind.py --force --bind=igb_uio` in-order to bind it to DPDK.
6. Find the MAC address of the bound NIC by running `testpmd` under
   `net/3rdparty/dpdk/build/app`. This will verify that the NIC can be detected
   by DPDK, dump it's MAC address, and run a simple forwarding test over it.
7. Update `db/src/common.rs` with the client and server's MAC address depending
   on which machine you plan on using as a client, and which machine you plan on
   using as a server. Also, update the source MAC address with the client's MAC,
   and target MAC address with the server's MAC in `db/src/bin/client.rs`.
8. Run `make` on both machines to compile Sandstorm. Update `LD_LIBRARY_PATH`
   with `net/target/native`.
9. The server and client can now be run using the binaries called `server` and
   `client` under `db/target/release`.
