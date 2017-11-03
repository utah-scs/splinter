#include <cinttypes>
#include <iostream>
#include <chrono>

#ifndef COMMON_H
#define COMMON_H

using hrc = std::chrono::high_resolution_clock;

template <class T>
void report(const std::string& testName, size_t bytes, const T& start, const T& end) {
  std::chrono::duration<double> secs = end - start;
  double ms = secs.count() * 1000;

  std::cout << testName << std::endl
            << " Language C++" << std::endl
            << " Runtime " << ms << " ms" << std::endl
            << " Throughput " << bytes / 1024 / 1024 / secs.count()
            << " MB/s" << std::endl;
}

#endif
