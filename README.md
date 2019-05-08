# Splinter
A multi-tenant key-value store into which untrusted extensions can be pushed
down at run-time

## Running Splinter on CloudLab
To run Sandstorm on [CloudLab](https://www.cloudlab.us/login.php), do the
following

1. Instantiate an experiment using the `splinter-cluster` profile under the
    `sandstorm` project (you will need to join the project first). When asked,
    allocate two machines for the experiment.
2. Next, run
    `python3 scripts/everything/main.py --setup --build <cloudlab-username> <any-host-in-cluster> <command/extension>`
    From the root directory of the project on any machine outside or inside the cluster.
    This script is capable of: 
      - Installing Rust
      - Installing DPDK
      - Setting up NIC and MAC addresses
      - Compiling Server and Client Rust
      - Pushing and recompiling local commits
      - Running extensions/clients
      - logging output to `logs/latest/`
    If you want to know more about the features of this script,
    use `python3 scripts/everything/main.py -h`
  
## How to run server and client for each extension
To update the configuration parameters, change `db/server.toml` on the server side and `splinter/client.toml` on the client side.

To run the extension on the client-side, use `use_invoke = false` and for the server-side use `use_invoke=true`. And to run the extension on both on the server-side and client-side, keep `use_invoke = true` and add `pushback` feature in `db/Cargo.toml` on the server-side.

Change `num_tenants` and `num_keys` on both the sides. The server uses these parameters to populate the tables and extension for different number of extensions and the client uses these parameters to generate the load.

### Aggregate Extension
`key_size = 8`

`value_size = 30`

`num_aggr = X`	// The number of record accessed for per aggregate operation.

`order = X`	// The amound of computation(multiplication) for per aggregate operation.

### Analysis Extension
`key_size = 30`

`value_size = 108`

Also add the feature `ml_model` in `db/Cargo.toml`.

### Auth Extension
`key_size = 30`

`value_size = 72`

### Pushback Extension
`key_size = 30`

`value_size = 100`

`num_aggr = X` // The number of records accessed per operation.

`order = X` // The amount of compute per operation in terms of CPU cycles.

