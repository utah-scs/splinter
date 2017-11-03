#include <cinttypes>
#include <vector>
#include <thread>
#include <atomic>
#include <cassert>
#include <linux/futex.h>
#include <syscall.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <signal.h>
#include <sys/prctl.h>
#include <sys/mman.h>
#include <strings.h>

#ifndef BENCHMARK_H
#define BENCHMARK_H

class Benchmark {
 public:
  Benchmark(size_t nThreads, double seconds);
  ~Benchmark();

  bool getStop() { return stop; }

  virtual void start();

  const size_t nThreads;

 private:
  virtual void warmup(size_t threadId) {}
  virtual void run(size_t threadId) = 0;
  virtual void dumpHeader() {}
  virtual void dump(double time, double interval) {}

  void entry(size_t threadId);

  const double seconds;

  std::vector<std::thread> threads;

  double lastDumpSeconds;

  std::atomic<size_t> nReady;
  std::atomic<bool> go;
  std::atomic<bool> stop;
  std::atomic<size_t> nDone;
};

class PRNG {
 public:
  PRNG();
  PRNG(uint64_t seed);
  void reseed(uint64_t seed);
  uint64_t operator()();

 private:
  uint64_t x, y, z;
};

void pinTo(int id);

int futex_wait(void* addr, int val);
int futex_wake(void* addr, int n);

#endif
