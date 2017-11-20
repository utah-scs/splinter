/* Copyright (c) 2017 University of Utah
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <algorithm>

#include <sched.h>
#include <errno.h>
#include <getopt.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/wait.h>

#include "ContextSwitchBench.h"

int
main(int argc, char** argv)
{
    int argCode;
    int optIndex = 0;
    int activeCores = 0;
    int numHWThreads = 0;
    int numSockets = 1;
    int numCoresPerSocket = 0;
    int myCoreId = 0;
    string machineType = "laptop-broadwell";

    // Supported command line arguments
    static struct option long_options[] = {
        // The number of hardware cores to use.
        {"activeCores", required_argument, nullptr, 'a'},
        // The number of hardware threads available on the machine.
        {"numHWThreads", required_argument, nullptr, 'n'},
        // The hardware core to pin the parent benchmark process on.
        {"myCoreId", required_argument, nullptr, 'c'},
        // The machine being used for the experiment.
        {"machineType", required_argument, nullptr, 't'},
        // The number of sockets available on the machine.
        {"numSockets", required_argument, nullptr, 's'},
        // The number of hardware cores per socket.
        {"numCoresPerSocket", required_argument, nullptr, 'p'},
        {nullptr, 0, nullptr, 0}
    };

    // Read in command line arguments (if any).
    while ((argCode = getopt_long(argc, argv, "a:n:c:t:s:p:", long_options,
            &optIndex)) != -1) {
        switch (argCode) {
        case 'a':
            activeCores = atoi(optarg);
            break;

        case 'n':
            numHWThreads = atoi(optarg);
            break;

        case 'c':
            myCoreId = atoi(optarg);
            break;

        case 't':
            machineType = string(optarg);
            break;

        case 's':
            numSockets = atoi(optarg);
            break;

        case 'p':
            numCoresPerSocket = atoi(optarg);
            break;

        default:
            printf("Received invalid argument \'%c\'.\nExiting.\n", argCode);
            return (-EINVAL);
        }
    }

    if (numHWThreads <= 0) {
        printf("Please provide a positive non-zero value for numHWThreads.\n");
        printf("Exiting.\n");
        return (-EINVAL);
    }

    if (activeCores < 0) {
        printf("Please provide a positive value for activeCores.\n");
        printf("Exiting.\n");
        return (-EINVAL);
    }

    if (numSockets > 1 && numCoresPerSocket <= 0) {
        printf("Please provide a positive non-zero value for");
        printf(" numCoresPerSocket.\nExiting.\n");
        return (-EINVAL);
    }

    // Identify cores for pinning.
    vector<int> coreList = ContextSwitchBench::getHWCores(numHWThreads,
            numCoresPerSocket);

    if (activeCores > coreList.size() - 1) {
        printf("activeCores exceeds the total number of available hw cores.\n");
        printf("Please provide a value lesser than %lu.\n", coreList.size());
        printf("Exiting.\n");
        return (-EINVAL);
    }

    // Pin this thread to prevent it from interfering with the measurements.
    if (ContextSwitchBench::pinThreadToCore(coreList[myCoreId]) < 0) {
        printf("Failed to pin parent benchmark process.\nExiting.\n");
        return (-EINVAL);
    } else {
        printf("Parent benchmark process pinned to hw thread %d.\n",
                coreList[myCoreId]);
    }

    // Initialize cycle counters.
    Cycles::init();

    // Run the benchmark.
    if (activeCores > 0) {
        ContextSwitchBench bench(coreList, coreList.size(), numSockets,
                myCoreId, machineType, activeCores);
        bench.run();
    } else {
        for (int i = 1; i < coreList.size(); i++) {
            ContextSwitchBench bench(coreList, coreList.size(), numSockets,
                    myCoreId, machineType, i);
            bench.run();
        }
    }

    return (0);
}

/* PUBLIC METHODS ON ContextSwitchBench. */

/**
 * This function runs the experiment. A pair of processes is created for
 * each active core. Each process pair runs the benchmark. This function
 * returns once every pair has finished taking measurements.
 */
void
ContextSwitchBench::run()
{
    // Lock all the mutexes. This is required as part of the setup.
    for (int i = 0; i < numActiveCores; i++) {
        startMutex.lock(i);
        doneMutex.lock(i);
    }

    // Create all pairs of processes.
    for (int i = 0, core = 0; i < numActiveCores && core < numCores;
            i++, core++) {
        if (core == myCoreId) {
            core++;
        }

        createProcessPair(coreList[core], i);
        printf("Created process pair %d. Pinned to hw thread %d.\n", i,
                coreList[core]);
    }

    // Wait for the child processes to terminate.
    for (int i = 0; i < numActiveCores; i++) {
        int status;
        pid_t childPid;

        childPid = wait(&status);
        if (WIFEXITED(status) == false) {
            printf("Child (pid %d) did not exit normally (exit status %d).\n",
                    childPid, WEXITSTATUS(status));
        }
    }
}

/**
 * This function returns a list mapping every physical core in the system
 * to exactly one of it's hardware threads. This is useful to figure out
 * which hardware thread id to use when pinning a process to a physical
 * core.
 *
 * \param numHWThreads
 *     The number of hardware threads in the system.
 *
 * \param numCoresPerSocket
 *     The number of hardware cores per physical socket in the system.
 *
 * \return coreList
 *     A list mapping each physical core to exactly one of it's hardware
 *     threads.
 */
vector<int>
ContextSwitchBench::getHWCores(int numHWThreads, int numCoresPerSocket)
{
    vector<int> coreList(numHWThreads, -1);
    string filePrefix = "/sys/devices/system/cpu/cpu";
    string coreFileSuffix = "/topology/core_id";
    string sockFileSuffix = "/topology/physical_package_id";

    // For each hardware thread in the system, identify the core it sits on.
    // If the core has not been seen previously, update coreList[core] with
    // the hardware thread id.
    for (int i = 0; i < numHWThreads; i++) {
        string coreIdFile = filePrefix + to_string(i) + coreFileSuffix;
        string sockIdFile = filePrefix + to_string(i) + sockFileSuffix;
        ifstream inCoreFile(coreIdFile);
        ifstream inSockFile(sockIdFile);
        if (inCoreFile.is_open()) {
            string coreId;
            getline(inCoreFile, coreId);

            if(!inSockFile.is_open()) {
                continue;
            }

            string sockId;
            getline(inSockFile, sockId);

            if (coreList[stoi(coreId) + (stoi(sockId) * numCoresPerSocket)] ==
                -1) {
                coreList[stoi(coreId) + (stoi(sockId) * numCoresPerSocket)] = i;
            }

            inCoreFile.close();
            inSockFile.close();
        }
    }

    // Cleanup.
    coreList.erase(remove(coreList.begin(), coreList.end(), -1),
            coreList.end());

    return coreList;
}

/**
 * This function pins the calling thread/process to a core.
 *
 * \param coreId
 *     The core the caller will be pinned to.
 *
 * \return
 *     Returns 0 if the caller was successfully pinned to coreId.
 *     Returns -1 if pinning failed.
 */
int
ContextSwitchBench::pinThreadToCore(int coreId)
{
    cpu_set_t cpuset;
    cpu_set_t cpusetVerify;

    // First, set this thread's affinity.
    CPU_ZERO(&cpuset);
    CPU_SET(coreId, &cpuset);
    if (sched_setaffinity(0, sizeof(cpuset), &cpuset) != 0) {
        return (-1);
    }

    // Next, read this thread's affinity.
    CPU_ZERO(&cpusetVerify);
    if (sched_getaffinity(0, sizeof(cpusetVerify), &cpusetVerify) != 0) {
        return (-1);
    }

    // Make sure the set and read affinities match.
    if (CPU_EQUAL(&cpuset, &cpusetVerify) == 0) {
        return (-1);
    }

    return 0;
}

/**
 * This function writes an output header to a file.
 *
 * \param file
 *     The file that the header must be written to.
 */
void
ContextSwitchBench::dumpHeader(FILE *file)
{
    fprintf(file, "# ExptId: Identifies when the experiment was run.\n");
    fprintf(file, "# TS: The start time stamp of the sample.\n");
    fprintf(file, "# Cycles: The number of cycles it took for the process\n");
    fprintf(file, "#         to context switch out and back in.\n");
    fprintf(file, "# CyclesPerSec: The number of cpu cycles per second on\n");
    fprintf(file, "#               the machine this experiment was run on.\n");
    fprintf(file, "# Machine: The cloudlab machine the experiment was run ");
    fprintf(file, "on.\n");
    fprintf(file, "# ActiveCores: The number of cores running the ");
    fprintf(file, "benchmark.\n");
    fprintf(file, "# Cores: The number of hardware cores on the machine.\n");
    fprintf(file, "# Sockets: The number of hardware sockets on the machine.\n");

    fprintf(file, "ExptId TS Cycles CyclesPerSec Machine ActiveCores Cores");
    fprintf(file, " Sockets\n");
}

/**
 * This function writes out a set of samples to an output file.
 *
 * \param samples
 *     A reference to the set of samples that need to be written out.
 *
 * \param file
 *     The file the samples need to be written to.
 */
void
ContextSwitchBench::dumpSamples(vector<Sample>& samples, FILE* file)
{
    for (auto& sample : samples) {
        fprintf(file, "%lu %lu %lu %f %s %d %d %d\n",
            exptId.tv_sec,
            sample.startTS,
            sample.endTS - sample.startTS,
            Cycles::perSecond(),
            machineType.c_str(),
            numActiveCores,
            numCores,
            numSockets);
    }
}

/* PRIVATE METHODS ON ContextSwitchBench */

/**
 * This function runs the actual benchmark.
 *
 * Using the startMutex and doneMutex, the processes in a given pair induce
 * context switches in each other. Only one process makes measurements. Once
 * numSamplesPerCore measurements have been made, the experiment is over.
 *
 * Each measurement is the time taken by a process to context switch out and
 * back in i.e twice the actual process context switch time.
 */
void
ContextSwitchBench::processEntry(int hwThread, int pairId, int idInPair)
{
    if (pinThreadToCore(hwThread) < 0) {
        printf("Failed to pin child to core %d.\n Exiting.\n", hwThread);
        exit(EXIT_FAILURE);
    }

    if (idInPair == 0) { // Master process that takes measurements.
        vector<Sample> samples(numSamplesPerCore);
        FILE* outputFile;
        string outputFileName = "samples." + machineType + ".pair" +
                to_string(pairId) + "." + to_string(numActiveCores) +
                "_active_cores.data";

        // Warmup of a 1000 samples.
        for (int i = 0; i < 1000; i++) {
            startMutex.unlock(pairId);
            doneMutex.lock(pairId);
        }

        // Each iteration of this loop will result in two context switches on
        // the core. One from the master to the slave, and the other from the
        // slave to the master.
        for (int i = 0; i < numSamplesPerCore; i++) {
            samples[i].startTS = Cycles::rdtsc();

            startMutex.unlock(pairId);
            doneMutex.lock(pairId);

            samples[i].endTS = Cycles::rdtsc();
        }

        outputFile = fopen(outputFileName.c_str(), "w");
        dumpSamples(samples, outputFile);
        fclose(outputFile);

        printf("Pair %d collected %d samples.\n", pairId, numSamplesPerCore);
    } else { // Slave process.
        while (true) {
            startMutex.lock(pairId);
            doneMutex.unlock(pairId);
        }
    }

    exit(EXIT_SUCCESS);
}

/**
 * This function creates a pair of processes, informing them of the hardware
 * thread they need to run on.
 */
void
ContextSwitchBench::createProcessPair(int hwThread, int pairId)
{
    for (int i = 0; i < 2; i++) {
        pid_t pid = fork();

        if (pid == -1) {
            printf("Failed to create child process.\n Exiting.\n");
            exit(EXIT_FAILURE);
        }

        if (pid == 0) {
            processEntry(hwThread, pairId, i);
        }
    }
}
