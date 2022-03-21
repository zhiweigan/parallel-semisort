#include <stdio.h>
#include <iostream>
#include <functional>
#include <unordered_map>
#include <unordered_set>
#include <random>

#include "../parlaylib/include/parlay/primitives.h"
#include "../parlaylib/include/parlay/parallel.h"
#include "../parlaylib/include/parlay/sequence.h"

template <class A, class B>
struct record
{
  A obj;
  B key;
  int hashed_key;
};