#include "common.h"

int main() {
  uint32_t c = 128 * 1024 * 1024;
  uint32_t it = 8;

  volatile uint32_t* a = new uint32_t[c];

  size_t bytes = sizeof(*a) * c * it;

  for (uint32_t i = 0; i < c; ++i) {
    a[i] = i;
  }

  auto start = hrc::now();

  uint32_t s = 0;
  for (uint32_t j = 0; j < 8; ++j) {
    for (uint32_t i = 0; i < c; ++i) {
      s += a[i];
    }
  }

  auto end = hrc::now();

  report("Uint32ArraySum", bytes, start, end);

  return 0;
}
