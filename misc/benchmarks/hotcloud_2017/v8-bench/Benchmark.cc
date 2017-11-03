#include "Benchmark.h"

#include <iostream>

#include "Cycles.h"

using RAMCloud::Cycles;

Benchmark::Benchmark(size_t nThreads, double seconds)
  : nThreads{nThreads}
  , seconds{seconds}
  , threads{}
  , lastDumpSeconds{}
  , nReady{}
  , go{}
  , stop{}
  , nDone{}
{
}

Benchmark::~Benchmark()
{
}

void
Benchmark::start()
{
  dumpHeader();

  for (size_t i = 0; i < nThreads; ++i)
    threads.emplace_back(&Benchmark::entry, this, i);

  while (nReady < nThreads)
    std::this_thread::yield();
  go = true;

  using namespace std::chrono_literals;
  uint64_t start = Cycles::rdtsc();
  uint64_t endTs = start + Cycles::fromSeconds(seconds);
  uint64_t nextDumpTs = start + Cycles::fromSeconds(1.0);

  while (true) {
    uint64_t now = Cycles::rdtsc();
    if (nextDumpTs < now) {
      double nowSeconds = Cycles::toSeconds(now - start);
      dump(nowSeconds, nowSeconds - lastDumpSeconds);
      lastDumpSeconds = nowSeconds;
      nextDumpTs = nextDumpTs + Cycles::fromSeconds(1.0);
    }
    if (endTs < now || nDone == nThreads)
      break;
    std::this_thread::sleep_for(1ms);
  }

  stop = true;

  for (auto& thread : threads)
    thread.join();
}

void
Benchmark::entry(size_t threadId)
{
  warmup(threadId);

  ++nReady;
  while (!go)
    std::this_thread::yield();

  run(threadId);

  ++nDone;
}

PRNG::PRNG()
  : x{123456789}
  , y{362436069}
  , z{521288629}
{}

PRNG::PRNG(uint64_t seed)
  : x{123456789lu * ~(seed << seed)}
  , y{362436069 * ~(seed << (seed + 1))}
  , z{521288629 * ~(seed << (seed + 2))}
{}

void
PRNG::reseed(uint64_t seed)
{
  new (this) PRNG{seed};
}

uint64_t
PRNG::operator()()
{
  uint64_t t;
  x ^= x << 16;
  x ^= x >> 5;
  x ^= x << 1;

  t = x;
  x = y;
  y = z;

  z = t ^ x ^ y;
  return z;
}

int futex_wait(void* addr, int val){
  return syscall(SYS_futex, addr, FUTEX_WAIT, val, NULL, NULL, 0);
}

int futex_wake(void* addr, int n){
  return syscall(SYS_futex, addr, FUTEX_WAKE, n, NULL, NULL, 0);
}

void pinTo(int id) {
  cpu_set_t cpuset;

  CPU_ZERO(&cpuset);
  CPU_SET(id, &cpuset);
  int r = sched_setaffinity(0, sizeof(cpuset), &cpuset);
  //assert(r == 0); 
}
