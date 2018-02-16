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

"""This function compiles DPDK using Netbricks scripts on CloudLab's d430.
"""
def setupDpdk():
    print "=============== Compiling DPDK ========================"
    subprocess.check_call("./net/3rdparty/get-dpdk.sh", shell=True)
    return

"""This function updates Rust to the nightly build.
"""
def setRustNightly():
    print "=============== Setting Rust to Nightly ==============="
    subprocess.check_call("rustup install nightly", shell=True)
    subprocess.check_call("rustup default nightly", shell=True)
    return

"""This function installs the stable version of Rust.
"""
def installRust():
    print "=============== Installing Rust ======================="
    subprocess.check_call("curl https://sh.rustup.rs -sSf | sh", shell=True)

    print "\n" + "\033[1m" + "Run \'source $HOME/.cargo/env\', and then" +\
          " re-run this script." + "\033[0m"
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
