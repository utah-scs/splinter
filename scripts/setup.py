#!/usr/bin/python
#
# Copyright (c) 2018 University of Utah
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

import os
import sys
import subprocess

colors = {
    "bold": '\033[1m',
    "end" : '\033[0m',
}

"""This function prints a passed in string in the specified color.
"""
def printColor(color, string):
    print colors[color] + string + colors["end"]

"""This function first compiles DPDK using Netbricks scripts on CloudLab's d430.
   It then binds an active 10 GigE NIC to the compiled igb_uio driver.
"""
def setupDpdk():
    printColor("bold", "=============== Compiling DPDK =======================")
    subprocess.check_call("./net/3rdparty/get-dpdk.sh", shell=True)

    print ""
    printColor("bold", "=============== Binding NIC to DPDK ==================")
    # First, find the PCI-ID of the 10 GigE NIC.
    cmd = "./net/3rdparty/dpdk/usertools/dpdk-devbind.py --status-dev=net |" + \
            " grep 10GbE | grep Active | awk '{ print $1 }'"
    pci = subprocess.check_output(cmd, shell=True)

    # Next, load the necessary kernel modules.
    cmd = "sudo insmod ./net/3rdparty/dpdk/build/kmod/igb_uio.ko"
    subprocess.check_call("sudo modprobe uio", shell=True)
    subprocess.check_call(cmd, shell=True)

    # Then, bind the NIC to igb_uio.
    cmd = "sudo ./net/3rdparty/dpdk/usertools/dpdk-devbind.py --force -b " + \
            "igb_uio " + str(pci)
    subprocess.check_call(cmd, shell=True)

    return

"""This function updates Rust to the nightly build.
"""
def setRustNightly():
    printColor("bold", "=============== Setting Rust to Nightly ==============")
    subprocess.check_call("rustup install nightly", shell=True)
    subprocess.check_call("rustup default nightly", shell=True)
    return

"""This function installs the stable version of Rust.
"""
def installRust():
    printColor("bold", "=============== Installing Rust ======================")
    subprocess.check_call("curl https://sh.rustup.rs -sSf | sh", shell=True)

    print "\n"
    printColor("bold", "Run \'source $HOME/.cargo/env\', and then" + \
               " re-run this script.")
    return

if __name__ == "__main__":
    # First, check if stable rust needs to be installed. If it already exists,
    # then set rustc to the nightly version, else, install it.
    if os.path.isfile(".installed_rust"):
        setRustNightly()

        # Next, setup DPDK.
        setupDpdk()
    else:
        installRust()
        # Future invocations need not install stable rust again.
        subprocess.check_call("touch .installed_rust", shell=True)

    sys.exit(0)
