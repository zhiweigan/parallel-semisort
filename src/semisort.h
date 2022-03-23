#include <stdio.h>
#include <iostream>
#include <functional>
#include <unordered_map>
#include <unordered_set>
#include <random>

#include "../parlaylib/include/parlay/utilities.h"
#include "../parlaylib/include/parlay/primitives.h"
#include "../parlaylib/include/parlay/parallel.h"
#include "../parlaylib/include/parlay/sequence.h"
#include "../parlaylib/include/parlay/hash_table.h"

template <class A, class B>
struct record
{
  A obj;
  B key;
  int hashed_key;

  inline bool isEmpty() {
    return hashed_key == -1;
  }

  inline bool operator!=(record a) {
    return a.hashed_key != hashed_key || a.obj != obj || a.key != key;
  }

  inline bool operator==(record a) {
    return a.hashed_key == hashed_key || a.obj == obj || a.key == key;
  }
};

struct Bucket
{
  int bucket_id;
  int property;

  inline bool operator!=(Bucket a) {
    return a.bucket_id != bucket_id || a.property != property;
  }

  inline bool operator==(Bucket a)
  {
    return a.bucket_id == bucket_id && a.property == property;
  }

  operator long long () {
    return ((long long)bucket_id) << 32 | ((long long)property);
  }
};

template <class T>
struct hash_numeric
{
  using eType = T;
  using kType = T;
  eType empty() { return -1; }
  kType getKey(eType v) { return v >> 32; }
  size_t hash(kType v) { return static_cast<size_t>(parlay::hash64(v)); }
  int cmp(kType v, kType b) { return (v > b) ? 1 : ((v == b) ? 0 : -1); }
  bool replaceQ(eType, eType) { return 0; }
  eType update(eType v, eType) { return v; }
  bool cas(eType *p, eType o, eType n)
  {
    return std::atomic_compare_exchange_strong_explicit(
        reinterpret_cast<std::atomic<eType> *>(p), &o, n, std::memory_order_relaxed, std::memory_order_relaxed);
  }
};

int size_func(int num_records, double p, int n, double c) {
  double lnn = (double)log((double) n);
  double clnn = c * lnn;

  double fs = (num_records + clnn + sqrt(clnn * clnn + 2 * num_records * c * clnn)) / p;
  double array_size = 1.1 * fs;

  return (int) pow(2, ceil(log(array_size) / log(2)));
}

template <class eType>
bool bucket_cas(eType *p, eType o, eType n)
{
  return std::atomic_compare_exchange_strong_explicit(
      reinterpret_cast<std::atomic<eType> *>(p), &o, n, std::memory_order_relaxed, std::memory_order_relaxed);
}


// round n down to nearest multiple of m
int roundDown(int n, int m) {
  return n >= 0 ? (n / m) * m : ((n - m + 1) / m) * m;
}