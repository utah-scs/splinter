#include "common.h"

#include <random>
#include <atomic>

template <size_t nFields>
struct Relation {
  struct Row {
    std::atomic<uint32_t> fields[nFields];
  };
  static_assert(sizeof(Row) == nFields * sizeof(uint32_t), "GRUU!");

  static constexpr size_t bytes = 1lu * 1024 * 1024 * 1024;
  static constexpr size_t nRows  = bytes / sizeof(Row);

  Row* a;
  std::atomic<uint32_t>* out;
  uint32_t outn;

  Relation()
    : a{new Row[nRows]}
    , out{new std::atomic<uint32_t>[nRows]}
    , outn{}
  {
    std::random_device rd{};
    std::mt19937 gen{rd()};
    std::uniform_int_distribution<> dis(0, 99);

    for (uint32_t i = 0; i < nRows; ++i) {
      for (uint32_t j = 0; j < nFields; ++j)
        a[i].fields[j] = dis(gen);
      out[i] = 0;
    }
  }

  ~Relation() {
    delete[] out;
    delete[] a;
  }

  Relation(const Relation&) = delete;
  Relation& operator=(const Relation&) = delete;

  uint32_t get(uint32_t row, uint32_t col) {
    if (nFields == 1)
      col = 0;
    return a[row].fields[col].load(std::memory_order_relaxed);
  }

  uint32_t set(uint32_t row, uint32_t col, uint32_t value) {
    if (nFields == 1)
      col = 0;
    a[row].fields[col].store(value, std::memory_order_relaxed);
  }

  uint32_t emit(uint32_t value) {
    out[outn++].store(value, std::memory_order_relaxed);
  }

  void reset() {
    outn = 0;
  }
};

Relation<16> r{};

// Schema: A: uint32_t
// Query: select sum(A) from T
void q1() {
  uint32_t s = 0;
  for (uint32_t i = 0; i < r.nRows; ++i) {
    s += r.get(i, 0);
  }
  r.emit(s);
}


// Schema: A: uint32_t
// Query: select sum(A) from T where A < x
void q2(uint32_t x) {
  uint32_t s = 0;
  for (uint32_t i = 0; i < r.nRows; ++i) {
    uint32_t a = r.get(i, 0);
    if (a < x)
      s += a;
  }
  r.emit(s);
}

// Schema: A: uint32_t
// Query: select sum(A) from T where A < x
void q3(uint32_t x) {
  size_t outn = 0;
  for (uint32_t i = 0; i < r.nRows; ++i) {
    uint32_t a = r.get(i, 0);
    if (a < x)
      r.emit(a + a);
  }
}

void q4(uint32_t x) {
  for (uint32_t i = 0; i < r.nRows; ++i) {
    uint32_t a = r.get(i, 0);
    uint32_t b = r.get(i, 1);
    uint32_t c = r.get(i, 2);
    uint32_t d = r.get(i, 3);
    if (a < x) {
      r.emit(a + b + c + d);
    }
  }
}

void q5(uint32_t x) {
  uint32_t s = 0;
  for (uint32_t i = 0; i < r.nRows; ++i) {
    uint32_t a = r.get(i, 0);
    uint32_t b = r.get(i, 1);
    uint32_t c = r.get(i, 2);
    uint32_t d = r.get(i, 3);
    if (a < x) {
      s += pow(a, pow(b, pow(c, d)));
    }
  }
  r.emit(s);
}

int main() {
  auto start = hrc::now();
  q1();
  auto end = hrc::now();
  report("OneColSelect::q1", r.bytes, start, end);
  r.reset();

  for (uint32_t iters = 0; iters < 10; ++iters) {
    for (uint32_t selectivity = 0; selectivity < 101; selectivity += 10) {
      std::string selstr = std::to_string(selectivity);
      start = hrc::now();
      q2(selectivity);
      end = hrc::now();
      report("OneColSelect::q2(" + selstr + ")",
             r.bytes, start, end);
      r.reset();

      start = hrc::now();
      q3(selectivity);
      end = hrc::now();
      report("OneColSelect::q3(" + selstr + ")",
             r.bytes, start, end);
      r.reset();

      start = hrc::now();
      q4(selectivity);
      end = hrc::now();
      report("OneColSelect::q4(" + selstr + ")",
             r.bytes, start, end);
      r.reset();

      start = hrc::now();
      q5(selectivity);
      end = hrc::now();
      report("OneColSelect::q5(" + selstr + ")",
             r.bytes, start, end);
      r.reset();
    }
  }

  return 0;
}

