#!/usr/bin/env bash
set -exu

if [ -f $HOME/.cargo/env ]; then
    source $HOME/.cargo/env
fi

# Make sure rust is up-to-date
if [ ! -x "$(command -v rustup)" ] ; then
    curl https://sh.rustup.rs -sSf | sh -s -- -y
fi

source $HOME/.cargo/env
rustup default nightly
rustup update

bash ./net/3rdparty/get-dpdk.sh

cd net/native
make
cd ../..
mkdir -p net/target/native
cp net/native/libzcsi.so net/target/native/libzcsi.so

PCI=$(python ./net/3rdparty/dpdk/usertools/dpdk-devbind.py --status-dev=net | grep ens1f1 | grep Active | tail -1 | awk '{ print $1 }')
MAC=$(ethtool -P ens1f1 | awk '{ print $3 }')

echo "pci: $PCI" > nic_info
echo "mac: $MAC" >> nic_info

