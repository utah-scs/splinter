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

#include <atomic>

#include <unistd.h>
#include <strings.h>

#include <sys/mman.h>
#include <sys/syscall.h>

#include <linux/futex.h>

#ifndef MUTEX_H
#define MUTEX_H

using namespace std;

/**
 * A Mutex class.
 *
 * This class provides a set of mutexes that can be shared by a group of
 * processes forked off from a parent process. Mutexes can be locked and
 * unlocked individually and independently.
 *
 * This class has one issue. If a process that has locked a mutex tries to
 * lock it again, it will get blocked. As a result, this class is fit only
 * for microbenchmarking and should not be used in any real application.
 */
class Mutex {
  public:
    /**
     * Constructor for Mutex. Creates a shared mapping consisting of one
     * mutex for each pair of processes that will use this object.
     */
    Mutex(int maxProcesses)
        : maxProcesses(maxProcesses)
        , sharedMapping()
        , sharedBytes(sizeof(int) * maxProcesses)
        , sharedAtomics()
    {
        sharedMapping = mmap(nullptr, sharedBytes, PROT_READ | PROT_WRITE,
                MAP_SHARED | MAP_ANONYMOUS, -1, 0);
        bzero(sharedMapping, sharedBytes);

        sharedAtomics =
                reinterpret_cast<decltype(sharedAtomics)>(sharedMapping);
    }

    /**
     * Destructor for Mutex.
     */
    ~Mutex()
    {
        munmap(sharedMapping, sharedBytes);
    }

    /**
     * This function locks a mutex. It does so by setting the value of the
     * mutex to 1. If the mutex has already been locked, the calling process
     * is put to sleep and the value of the mutex is set to 2. The process
     * will be woken up once some process unlocks the mutex by setting it's
     * value to 0.
     *
     * \param mutexId
     *     The mutex to be locked.
     */
    void
    lock(int mutexId)
    {
        if (mutexId >= maxProcesses) {
            printf("Process tried to lock an unallocated mutex.\n");
            printf("Exiting.\n");
            exit(EXIT_FAILURE);
        }

        // A value of zero means that the mutex is unlocked.
        auto& mutex = sharedAtomics[mutexId];
        int expectedValue = 0;

        // CHECK 1.
        // If the value of the mutex is equal to zero, then set it to 1 and
        // return. This effectively acquires the mutex, and marks it as locked.
        if (mutex.compare_exchange_strong(expectedValue, 1) == true) {
            return;
        } // END OF CHECK 1.

        // Reach here if the mutex has been locked by a different process.
        while (true) {
            // CHECK 2.
            // This check consists of two conditions:
            // 1. expectedValue == 2 if there already exists another process
            //    blocked on the mutex. In this case, the current process is
            //    blocked. This condition is also satisfied if CHECK 4 below
            //    failed, meaning that the mutex is still locked.
            // 2. The second condition is satisfied if the value of the mutex
            //    changed after the last CAS attempt. This can happen if either
            //    the mutex was unlocked (value of mutex set to zero), or there
            //    is another process concurrently executing this loop (value of
            //    mutex changed from 1 to 2). If this condition is not
            //    satisfied, it means that the mutex is still locked, and the
            //    value of mutex will be set to 2.
            if (expectedValue == 2 ||
                mutex.compare_exchange_strong(expectedValue, 2) == false) {

                // CHECK 3.
                // If the previos CAS attempt in CHECK 2 or CHECK 4 indicate
                // that the mutex is still locked, then block the process.
                if (expectedValue != 0) {
                    futex_wait(reinterpret_cast<void*>(&mutex), 2);
                } // END OF CHECK 3.

            } // END OF CHECK 2.

            // CHECK 4.
            // If the mutex has been unlocked by a different process at this
            // point (i.e it's value is zero), set it to 2 and return. This
            // effectively acquires the mutex, and marks it as locked.
            //
            // If the mutex is still locked, this check sets expectedValue to
            // 2 causing CHECK 2 above to succeed and this process to be
            // blocked.
            expectedValue = 0;
            if (mutex.compare_exchange_strong(expectedValue, 2) == true) {
                return;
            } // END OF CHECK 4.
        }
    }

    /**
     * This function unlocks a mutex. The mutex is unlocked by setting it's
     * value to 0. Atmost one process waiting on the mutex is then woken up.
     *
     * \param mutexId
     *     The mutex to be unlocked.
     */
    void
    unlock(int mutexId)
    {
        if (mutexId >= maxProcesses) {
            printf("Process tried to unlock an unallocated mutex.\n");
            printf("Exiting.\n");
            exit(EXIT_FAILURE);
        }

        auto& mutex = sharedAtomics[mutexId];

        mutex = 0;
        futex_wake(reinterpret_cast<void*>(&mutex), 1);
    }

  private:
    /**
     * This function uses the futex api to put a process waiting to acquire
     * a mutex to sleep.
     */
    void
    futex_wait(void* addr, int value)
    {
        syscall(SYS_futex, addr, FUTEX_WAIT, value, nullptr, nullptr, 0);
    }

    /**
     * This function uses the futex api to wakeup a process that had blocked
     * while trying to lock a mutex.
     */
    void
    futex_wake(void* addr, int numProcesses)
    {
        syscall(SYS_futex, addr, FUTEX_WAKE, numProcesses, nullptr, nullptr, 0);
    }

    // The number of process pairs using this object. One mutex will be created
    // per process pair.
    int maxProcesses;

    // The memory underlying the set of mutexes. This memory is mapped in to
    // every process using this object.
    void* sharedMapping;

    // The space in bytes occupied by the set of mutexes.
    int sharedBytes;

    // A reference to sharedMapping for ease of access by processes.
    atomic<int>* sharedAtomics;
};

#endif // MUTEX_H
