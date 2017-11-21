#!/usr/bin/python

# Copyright (c) 2017 University of Utah
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
import gzip
import glob
import argparse
import subprocess

def getProcessorConfig():
    """Return the cpu configuration of the test machine.

    @return: A tupule of integers consisting of the number of sockets, number
             of hardware cores per socket, and number of hardware threads on
             the test machine.
    """
    # Get the number of sockets.
    cmd = 'lscpu | grep \"Socket(s):\" | awk \'{ print $2 }\''
    sockets = int(subprocess.check_output(cmd, shell=True))

    # Get the number of cores per socket.
    cmd = 'lscpu | grep \"Core(s) per socket:\" | awk \'{ print $4 }\''
    coresPerSocket = int(subprocess.check_output(cmd, shell=True))

    # Get the number of hardware threads.
    cmd = 'lscpu | grep \"CPU(s):\" | grep -v "NUMA" | awk \'{ print $2 }\''
    hwThreads = int(subprocess.check_output(cmd, shell=True))

    return sockets, coresPerSocket, hwThreads

def compileBenchmark():
    """Compile the Context Switch Benchmark.
    """
    print('========== Compiling Context Switch Benchmark ==========')
    subprocess.check_call('make')
    return

def runBenchmark(cmdLineArgs, sockets, coresPerSocket, hwThreads):
    """Run the Context Switch Benchmark.

    @param cmdLineArgs: Command line arguments that were parsed in by argparse.

    @param sockets: The number of sockets on the test machine.
    @type  sockets: C{int}

    @param coresPerSocket: The number of cores on each socket.
    @type  coresPerSocket: C{int}

    @param hwThreads: The number of hardware threads on the test machine.
    @type  hwThreads: C{int}
    """
    print('\n========== Running Context Switch Benchmark ============')

    # Create an argument string for the benchmark.
    benchCmd = './ContextSwitchBench' + \
               ' --machineType ' + str(cmdLineArgs.machineType) + \
               ' --myCoreId ' + str(cmdLineArgs.myCoreId) + \
               ' --activeCores ' + str(cmdLineArgs.activeCores) + \
               ' --numSockets ' + str(sockets) + \
               ' --numCoresPerSocket ' + str(coresPerSocket) + \
               ' --numHWThreads ' + str(hwThreads)

    subprocess.check_call(benchCmd, shell=True)
    return

def consolidateOutput(machineType):
    """Read in the output of ContextSwitchBench, and generate a single output
       file.

    @param machineType: The machine this experiment is being run on.
    @type  machineType: C{str}
    """
    print('\n========== Consolidating Benchmark Output ==============')

    filePrefix = 'samples.' + machineType

    header = '# ExptId: Identifies when the experiment was run.\n' + \
             '# TS: The start time stamp of the sample.\n' + \
             '# Cycles: The number of cycles it took for the process\n' + \
             '#         to context switch out and back in.\n' + \
             '# CyclesPerSec: The number of cpu cycles per second on\n' + \
             '#               the machine this experiment was run on.\n' + \
             '# Machine: The cloudlab machine the experiment was run on.\n' + \
             '# ActiveCores: The number of cores running the benchmark.\n' + \
             '# Cores: The number of hardware cores on the machine.\n' + \
             '# Sockets: The number of hardware sockets on the machine.\n' + \
             '# ExptId TS Cycles CyclesPerSec Machine ActiveCores Cores' + \
             ' Sockets\n'

    with gzip.open(filePrefix + '.data.gz', 'a') as outputFile:
        # First, write a header.
        outputFile.write(header)

        # Write all output into a single .gz file.
        for samples in glob.glob(filePrefix + '*.data'):
            with open(samples, 'r') as inputFile:
                outputFile.writelines(inputFile.readlines())
            os.remove(samples)

    return

if __name__ == "__main__":
    # Parse command line arguments.
    parser = argparse.ArgumentParser(description=\
                                     'Configure and run ContextSwitchBench')
    parser.add_argument('--machineType', required=True,
                        help='The processor architecture of the test machine.')
    parser.add_argument('--myCoreId', type=int, default=0,
                        help='The hw thread to pin the managing processes to.')
    parser.add_argument('--activeCores', type=int, default=1,
                        help='The number of cores to collect measurements on.')
    parser.add_argument('--numRuns', type=int, default=1,
                        help='The number of experimental runs to perform.')
    args = parser.parse_args()

    # Get this machines processor configuration.
    sockets, coresPerSocket, hwThreads = getProcessorConfig()

    # Compile the benchmark.
    compileBenchmark()

    # Do multiple runs of the benchmark.
    for runNum in range(0, args.numRuns):
        # Run the benchmark.
        runBenchmark(args, sockets, coresPerSocket, hwThreads)

        # Consolidate output.
        consolidateOutput(args.machineType)

    sys.exit(0)
