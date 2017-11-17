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

#include <vector>

#include <sys/time.h>

#include "Mutex.h"
#include "Cycles.h"

using namespace std;

#ifndef CONTEXT_SWITCH_BENCH_H
#define CONTEXT_SWITCH_BENCH_H

/**
 * A microbenchmark to measure process context switch time.
 *
 * This class benchmarks the time taken to context switch out of a process
 * and back in.
 *
 * A pair of processes is created and pinned to a hardware core. This pair
 * enters into a tight loop involving two mutexes - a ``start" mutex and a
 * ``done" mutex. The mutexes help induce context switches which are measured
 * using cycle counters.
 */
class ContextSwitchBench {
  public:
    /**
     * Constructor for ContextSwitchBench.
     */
    ContextSwitchBench(vector<int> coreList, int numCores, int numSockets,
            int myCoreId, string machineType="laptop-broadwell",
            int numActiveCores=1, int numSamplesPerCore=1e6)
        : coreList(coreList)
        , numCores(numCores)
        , numSockets(numSockets)
        , myCoreId(myCoreId)
        , numActiveCores(numActiveCores)
        , numSamplesPerCore(numSamplesPerCore)
        , startMutex(numActiveCores)
        , doneMutex(numActiveCores)
        , exptId()
        , machineType(machineType)
    {
        // Use the current time for the experiment id.
        gettimeofday(&exptId, nullptr);
    }

    /**
     * Destructor for ContextSwitchBench.
     */
    ~ContextSwitchBench()
    {
        ;
    }

    void run();
    static vector<int> getHWCores(int numHWThreads, int numCoresPerSocket);
    static int pinThreadToCore(int coreId);

    /**
     * A simple type to track the start and end time stamp of a
     * measurement sample.
     */
    struct Sample {
        /**
         * Constructor for Sample.
         */
        Sample()
            : startTS(0)
            , endTS(0)
        {}

        // The time stamp at which the measurement started.
        uint64_t startTS;

        // The time stamp at which the measurement was complete.
        uint64_t endTS;
    };

    void dumpHeader(FILE *file);
    void dumpSamples(vector<Sample>& samples, FILE *file);

  private:
    void createProcessPair(int hwThread, int pairId);
    void processEntry(int hwThread, int pairId, int idInPair);

    // A list mapping hardware cores to hardware threads. Used to pin child
    // processes to cores.
    vector<int> coreList;

    // The number of physical cores on this machine. Required as part of the
    // output file.
    int numCores;

    // The number of physical sockets on this machine. Required as part of the
    // output file.
    int numSockets;

    // The logical core the parent process is pinned to. Child processes will
    // not be pinned to this core.
    int myCoreId;

    // The number of cores actively involved in the experiment. Each active
    // core runs a pair of child processes.
    int numActiveCores;

    // The number of samples collected on each active core.
    int numSamplesPerCore;

    // First of a pair of mutexes required to measure context switch time.
    Mutex startMutex;

    // Second of a pair of mutexes required to measure context switch time.
    Mutex doneMutex;

    // A unique identifier for the experiment.
    struct timeval exptId;

    // The type of CloudLab machine the experiment is being run on.
    string machineType;
};

#endif // CONTEXT_SWITCH_BENCH_H
