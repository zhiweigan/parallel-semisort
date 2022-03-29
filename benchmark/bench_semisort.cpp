// A benchmark set for parallel semisort

#include <benchmark/benchmark.h>

#include <parlay/monoid.h>
#include <parlay/primitives.h>
#include <parlay/random.h>
#include <parlay/io.h>

#include <iostream>
#include <random>

#include "../src/semisort.h"
#include "genzipf.cpp"

using benchmark::Counter;

using parlay::parallel_for;

const float HASH_RANGE_K = constants::HASH_RANGE_K;

// Use this macro to avoid accidentally timing the destructors
// of the output produced by algorithms that return data
//
// The expression e is evaluated as if written in the context
// auto result_ = (e);
//
#define RUN_AND_CLEAR(e)      \
  {                           \
    auto result_ = (e);       \
    state.PauseTiming();      \
  }                           \
  state.ResumeTiming();

// Use this macro to copy y into x without measuring the cost
// of the copy in the benchmark
//
// The effect of this macro on the arguments x and is equivalent
// to the statement (x) = (y)
#define COPY_NO_TIME(x, y)    \
  state.PauseTiming();        \
  (x) = (y);                  \
  state.ResumeTiming();

// Report bandwidth and throughput statistics for the benchmark
//
// Arguments:
//  n:             The number of elements processed
//  bytes_read:    The number of bytes read per element processed
//  bytes_written: The number of bytes written per element processed
//
#define REPORT_STATS(n, bytes_read, bytes_written)                                                                                   \
  state.counters["       Bandwidth"] = Counter(state.iterations()*(n)*((bytes_read) + 0.7 * (bytes_written)), Counter::kIsRate);     \
  state.counters["    Elements/sec"] = Counter(state.iterations()*(n), Counter::kIsRate);                                            \
  state.counters["       Bytes/sec"] = Counter(state.iterations()*(n)*(sizeof(T)), Counter::kIsRate);

// ------------------------- Input generation methods -------------------------------

//
// Return uniform_distributions input, note that the records are already hashed
//
static parlay::sequence<record<uint64_t, uint64_t>> uniform_distribution_input(size_t n, size_t para) {
  
  std::default_random_engine generator;
  std::uniform_int_distribution<uint64_t> distribution(0, para);
  uint64_t k = pow(n, HASH_RANGE_K);
  uint64_t key;

  parlay::sequence<record<uint64_t, uint64_t>> arr(n);
  parallel_for (size_t i = 0; i < n; i++) {
    key = distribution(generator);
    record<uint64_t, uint64_t> elt = {
      0,
      key,
      static_cast<int>(parlay::hash64(key) % k)
    };
    arr[i] = elt;
  }

  return arr;
}

//
// Return exponential_distribution input, note that the records are already hashed
//
static parlay::sequence<record<uint64_t, uint64_t>> exponential_distribution_input(size_t n, size_t para) {
  
  std::default_random_engine generator;
  std::exponential_distribution<double> distribution(para);
  uint64_t k = pow(n, HASH_RANGE_K);
  uint64_t key;

  parlay::sequence<record<uint64_t, uint64_t>> arr(n);
  parallel_for (size_t i = 0; i < n; i++) {
    key = static_cast<uint64_t>(n * distribution(generator));
    record<uint64_t, uint64_t> elt = {
      0,
      key,
      static_cast<int>(parlay::hash64(key) % k)
    };
    arr[i] = elt;
  }

  return arr;
}

//
// Return zipfian_distribution input, note that the records are already hashed
//
static parlay::sequence<record<uint64_t, uint64_t>> zipfian_distribution_input(size_t n, size_t para) {
  
  rand_val(1);
  uint64_t k = pow(n, HASH_RANGE_K);
  uint64_t key;

  parlay::sequence<record<uint64_t, uint64_t>> arr(n);
  parallel_for (size_t i = 0; i < n; i++) {
    key = static_cast<uint64_t>(zipf(1.0, para)); // according to section 5.1: "the i-th number in this range has a probability 1/(iM-) of being chosen..."
    record<uint64_t, uint64_t> elt = {
      0,
      key,
      static_cast<int>(parlay::hash64(key) % k)
    };
    arr[i] = elt;
  }

  return arr;
}

// Helper function to print keys of sequence for debugging
template<class Object, class Key>
static void print_sequence_key(parlay::sequence<record<Object, Key>> &arr) {
    for(size_t i = 0; i < arr.size(); i++){
        std::cout<<arr[i].key<<" ";
    }
    std::cout<<std::endl;
}

// ------------------------- Benchmark functions -------------------------------

//
// Benchmark figure 1 exponential distribution input
//
template<typename T>
static void bench_semisort_figure1_a(benchmark::State& state) {

  size_t n = 100000000; // my laptop cannot run 100M input size...
  size_t para = state.range(0);
  // std::cout << "figure1_a_exponential distribution: para = " << para << std::endl;
  auto in = exponential_distribution_input(n, para);
  if DEBUG {
    print_sequence_key(in);
  }
  auto out = in;

  while (state.KeepRunningBatch(10)) {
    for (int i = 0; i < 10; i++) {
      COPY_NO_TIME(out, in);
      semi_sort(out); // here does not check for correctness
    }
  }

  REPORT_STATS(n, 0, 0);
}

//
// Benchmark figure 1 uniform distribution input
//
template<typename T>
static void bench_semisort_figure1_b(benchmark::State& state) {
  size_t n = 100000000;
  size_t para = state.range(0);
  std::cout << "figure1_b_uniform distribution: para = " << para << std::endl;
  auto in = uniform_distribution_input(n, para);
  auto out = in;

  while (state.KeepRunningBatch(10)) {
    for (int i = 0; i < 10; i++) {
      COPY_NO_TIME(out, in);
      semi_sort(out); // here does not check for correctness
    }
  }

  REPORT_STATS(n, 0, 0);
}

//
// Benchmark figure 1 zipfian distribution input
//
template<typename T>
static void bench_semisort_figure1_c(benchmark::State& state) {
  size_t n = 100000000;
  size_t para = state.range(0);
  std::cout << "figure1_c_zipfian distribution: para = " << para << std::endl;
  auto in = zipfian_distribution_input(n, para);
  auto out = in;

  while (state.KeepRunningBatch(10)) {
    for (int i = 0; i < 10; i++) {
      COPY_NO_TIME(out, in);
      semi_sort(out); // here does not check for correctness
    }
  }

  REPORT_STATS(n, 0, 0);
}

//
// Benchmark figure 2 exponential distribution input. Question: how to benchmark different number of threads?
//
template<typename T>
static void bench_semisort_figure2_a(benchmark::State& state) {
  size_t n = 100000000;
  auto in = exponential_distribution_input(100000000, 100000); // figure2 has fixed size and para
  auto out = in;

  while (state.KeepRunningBatch(10)) {
    for (int i = 0; i < 10; i++) {
      COPY_NO_TIME(out, in);
      semi_sort(out); // here does not check for correctness
    }
  }

  REPORT_STATS(n, 0, 0);
}

//
// Benchmark figure 2 uniform distribution input. Question: how to benchmark different number of threads?
//
template<typename T>
static void bench_semisort_figure2_b(benchmark::State& state) {
  size_t n = 100000000;
  auto in = uniform_distribution_input(100000000, 100000000); // figure2 has fixed size and para
  auto out = in;

  while (state.KeepRunningBatch(10)) {
    for (int i = 0; i < 10; i++) {
      COPY_NO_TIME(out, in);
      semi_sort(out); // here does not check for correctness
    }
  }

  REPORT_STATS(n, 0, 0);
}

// See various input distributions
template<typename T>
static void bench_semi_sort(benchmark::State& state) {
  size_t n = state.range(0);

  for (auto _ : state) {
    std::cout<<"uniform_distribution:"<< std::endl;
    auto arr1 = uniform_distribution_input(n, n);
    for(size_t i = 0; i < arr1.size(); i++){
      std::cout<<arr1[i].key<<" ";
    }
    std::cout<<std::endl;
    std::cout<<"exponential_distribution:"<< std::endl;
    auto arr2 = exponential_distribution_input(n, n/1000);
    for(size_t i = 0; i < arr2.size(); i++){
      std::cout<<arr2[i].key<<" ";
    }
    std::cout<<std::endl;
    std::cout<<"zipfian_distribution:"<< std::endl;
    auto arr3 = zipfian_distribution_input(n, n);
    for(size_t i = 0; i < arr3.size(); i++){
      std::cout<<arr3[i].key<<" ";
    }
    std::cout<<std::endl;
  }

  REPORT_STATS(n, 0, 0);
}

// Define the radix-sort benchmark
template<typename T>
static void bench_integer_sort(benchmark::State& state) {
  size_t n = state.range(0);
  parlay::random r(0);
  size_t bits = sizeof(T)*8;
  auto S = parlay::tabulate(n, [&] (size_t i) -> T {
				 return r.ith_rand(i);});
  auto identity = [] (T a) {return a;};

  while (state.KeepRunningBatch(10)) {
    for (int i = 0; i < 10; i++) {
      RUN_AND_CLEAR(parlay::internal::integer_sort(parlay::make_slice(S), identity, bits));
    }
  }

  REPORT_STATS(n, 0, 0);
}

// ------------------------- Registration -------------------------------

#define BENCH(NAME, T, args...) BENCHMARK_TEMPLATE(bench_ ## NAME, T)               \
                          ->UseRealTime()                                           \
                          ->Unit(benchmark::kMillisecond)                           \
                          ->Args({args});

// Figure 1
BENCH(semisort_figure1_a, size_t, 100);
BENCH(semisort_figure1_a, size_t, 1000);
BENCH(semisort_figure1_a, size_t, 10000);
BENCH(semisort_figure1_a, size_t, 100000);
BENCH(semisort_figure1_a, size_t, 300000);
BENCH(semisort_figure1_a, size_t, 1000000);
BENCH(semisort_figure1_b, size_t, 10);
BENCH(semisort_figure1_b, size_t, 100000);
BENCH(semisort_figure1_b, size_t, 320000);
BENCH(semisort_figure1_b, size_t, 500000);
BENCH(semisort_figure1_b, size_t, 1000000);
BENCH(semisort_figure1_b, size_t, 100000000);
BENCH(semisort_figure1_c, size_t, 10000);
BENCH(semisort_figure1_c, size_t, 100000);
BENCH(semisort_figure1_c, size_t, 1000000);
BENCH(semisort_figure1_c, size_t, 10000000);
BENCH(semisort_figure1_c, size_t, 100000000);

// Figure 2
BENCH(semisort_figure2_a, size_t);
BENCH(semisort_figure2_a, size_t);