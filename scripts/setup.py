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
import argparse
import subprocess

"""Dictionary of different colors that can be printed to the screen.
"""
colors = {
    "bold": '\033[1m',
    "end" : '\033[0m',
}

"""This function prints a passed in string in the specified color.
"""
def printColor(color, string):
    print colors[color] + string + colors["end"]

"""This function fixes dependencies inside the db and ext/test crates.
"""
def setupCargo():
    printColor("bold", "=============== Fixing Deps ==========================")
    fix = "cargo generate-lockfile; " + \
          "cargo update -p spin:0.4.9 --precise 0.4.7; " + \
          "cargo update -p serde:1.0.72 --precise 1.0.37; " + \
          "cargo update -p serde_derive:1.0.72 --precise 1.0.37; " + \
          "cargo update -p env_logger:0.5.12 --precise 0.5.3; "

    # Fix dependencies inside db.
    cmd = "cd db; " + fix + "cd ../"
    subprocess.check_call(cmd, shell=True)

    # Fix dependencies inside ext/test.
    cmd = "cd ext/test; " + fix + "cd ../../"
    subprocess.check_call(cmd, shell=True)

"""This function first compiles DPDK using Netbricks scripts on CloudLab's d430.
   It then binds an active 10 GigE NIC to the compiled igb_uio driver.
"""
def setupDpdk():
    printColor("bold", "=============== Compiling DPDK =======================")
    subprocess.check_call("./net/3rdparty/get-dpdk.sh", shell=True)

    print ""
    printColor("bold", "=============== Binding NIC to DPDK ==================")
    # First, find the PCI-ID of the first active 10 GigE NIC.
    cmd = "./net/3rdparty/dpdk/usertools/dpdk-devbind.py --status-dev=net |" + \
            " grep 10GbE | grep Active | tail -1 | awk '{ print $1 }'"
    pci = subprocess.check_output(cmd, shell=True)

    # Next, load the necessary kernel modules.
    cmd = "sudo insmod ./net/3rdparty/dpdk/build/kmod/igb_uio.ko"
    subprocess.check_call("sudo modprobe uio", shell=True)
    subprocess.check_call(cmd, shell=True)

    # Print out the PCI and MAC address of the NIC.
    cmd = "ls /sys/bus/pci/devices/" + str(pci).rstrip() + "/net/"
    net = subprocess.check_output(cmd, shell=True)
    cmd = "ethtool -P " + str(net).rstrip() + " | awk '{ print $3 }'"
    mac = subprocess.check_output(cmd, shell=True)
    printColor("bold", "NIC PCI ADDRESS: " + str(pci).rstrip())
    printColor("bold", "NIC MAC ADDRESS: " + str(mac).rstrip())

    # Write out the PCI and MAC addresses to a file.
    subprocess.check_output("rm -Rf ./nic_info", shell=True)
    subprocess.check_output("echo \"pci: " + str(pci).rstrip() + \
                            "\" >> ./nic_info", shell=True)
    subprocess.check_output("echo \"mac: " + str(mac).rstrip() + \
                            "\" >> ./nic_info", shell=True)

    # Then, bind the NIC to igb_uio.
    cmd = "sudo ./net/3rdparty/dpdk/usertools/dpdk-devbind.py --force -b " + \
            "igb_uio " + str(pci)
    subprocess.check_call(cmd, shell=True)

    return

"""This function sets up the vim editor.
"""
def setupDevEnvt():
    printColor("bold", "=============== Setting up Dev Environment ===========")
    subprocess.check_call("cp ./misc/dev/vimrc-sample ~/.vimrc", shell=True)
    subprocess.check_call("cp -r ./misc/dev/vim ~/.vim", shell=True)
    subprocess.check_call("vim +PlugClean +PlugInstall +qall", shell=True)

"""This function installs the nightly version of Rust.
"""
def installRust():
    printColor("bold", "=============== Installing Rust ======================")
    subprocess.check_call("curl -s https://static.rust-lang.org/rustup.sh | " +\
                          "sh -s -- --channel=nightly --date=2018-04-11",
                          shell=True)
    return

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=\
                                     'Setup a machine for Sandstorm')
    parser.add_argument('--full', action='store_true',
                        help='If true, performs a full setup on the box.')
    parser.add_argument('--installRust', action='store_true',
                        help='If true, installs rust.')
    parser.add_argument('--setupDevEnv', action='store_true',
                        help='If true, sets up development tools (vim etc).')
    parser.add_argument('--installDpdk', action='store_true',
                        help='If true, builds and installs DPDK.')
    parser.add_argument('--fixCargoDep', action='store_true',
                        help='If true, fixes all cargo dependencies.')
    args = parser.parse_args()

    # First, install Rust.
    if args.full or args.installRust:
        installRust()

    # Then, setup the development environment.
    if args.full or args.setupDevEnv:
        setupDevEnvt()

    # Next, setup DPDK.
    if args.full or args.installDpdk:
        setupDpdk()

    # Finally, fix dependencies.
    if args.full or args.fixCargoDep:
        setupCargo()

    sys.exit(0)
