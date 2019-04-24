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
What are the parameters for each client, What is the key and value sizes, etc.
### Aggregate

### Analysis

### Auth

### Pushback

### TAO
