#include <cinttypes>
#include <iostream>
#include <vector>
#include <atomic>
#include <cassert>
#include <string>
#include <stdio.h>
#include <unistd.h>
#include <sched.h>

#include <algorithm>
#include <string>
#include <unordered_map>

#include <sys/mman.h>
#include <sys/wait.h>

#include "Benchmark.h"

using namespace std;

int futex_wait(void* addr, int val){
  return syscall(SYS_futex, addr, FUTEX_WAIT, val, NULL, NULL, 0);
}

int futex_wake(void* addr, int n){
  return syscall(SYS_futex, addr, FUTEX_WAKE, n, NULL, NULL, 0);
}

class Mutex {
 private:
    void *mem;
    std::atomic<int>* vals;
    size_t nBytes;

 public:
  Mutex(size_t nProcesses)
    : mem{}
    , vals{}
    , nBytes{sizeof(vals[0]) * nProcesses}
  {
    assert(nBytes == sizeof(int) * nProcesses);
    mem = mmap(NULL,
               sizeof(vals[0]) * nProcesses,
               PROT_READ | PROT_WRITE,
               MAP_ANONYMOUS | MAP_SHARED, -1, 0);
    assert(mem != MAP_FAILED);
    bzero(mem, nBytes);

    vals = reinterpret_cast<decltype(vals)>(mem);
  }

  ~Mutex()
  {
    munmap(mem, nBytes);
  }

  void lock(size_t threadId)
  {
    auto& val = vals[threadId];
    int expected = 0;
    if (!val.compare_exchange_strong(expected, 1)) {
      while (true) {
        if (expected == 2 || !val.compare_exchange_strong(expected, 2)) {
          if (expected != 0)
            futex_wait(reinterpret_cast<int*>(&val), 2);
        }
        expected = 0;
        if (val.compare_exchange_strong(expected, 2))
          break;
       }
    }
  }

  void unlock(size_t threadId)
  {
    auto& val = vals[threadId];
    --val;
    val = 0;
    futex_wake(reinterpret_cast<int*>(&val), 1);
  }
};

class ContextSwitchBench {
  Mutex startMutex;
  Mutex doneMutex;
  int nProcesses;

 public:
  ContextSwitchBench(size_t n)
    : startMutex{n}
    , doneMutex{n}
  {
    nProcesses = n;
  }

  ~ContextSwitchBench()
  {
  }

  // Do normal benchmark start stuff, but then also prefill with some data.
  // This happens on the main thread, so it happens just once in the order
  // specified.
  void start() {
    typedef std::chrono::high_resolution_clock Time;
    typedef std::chrono::milliseconds ms;
    typedef std::chrono::duration<float> fsec;

    cpu_set_t set;
    CPU_ZERO(&set);

    for (int i = 0; i < nProcesses; i++) {
      startMutex.lock(i);
      doneMutex.lock(i);
    }

    for (int i = 0; i < nProcesses; i++) {
      pid_t pid = fork();
      if (pid == 0) {
        CPU_SET(0, &set);
        sched_setaffinity(getpid(), sizeof(set), &set);
        int r = prctl(PR_SET_PDEATHSIG, SIGHUP);
        if (r == -1) {exit(1); }
        while (1) {
          startMutex.lock(i);
          doneMutex.unlock(i);
        }
      }
    }

    CPU_SET(0, &set);
    sched_setaffinity(getpid(), sizeof(set), &set);

    auto before = Time::now();
    for (int i = 0; i < 1000000; i++) {
      startMutex.unlock(0);
      doneMutex.lock(0);
      startMutex.unlock(1);
      doneMutex.lock(1);
    }
    auto after = Time::now();
    fsec gap = after - before;
    ms m = std::chrono::duration_cast<ms>(gap);

    cout << m.count() << " " << "ns" << endl;
  }
};

int main(int argc, char* argv[]) {
  size_t nProcesses = 2;

  ContextSwitchBench bench{nProcesses};
  std::cout << "# nProcesses " << nProcesses 
            << std::endl;

  bench.start();

  return 0;
}
