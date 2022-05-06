#pragma once
#include <stdio.h>
#include <iostream>
#include <functional>
#include <unordered_map>
#include <unordered_set>
#include <random>

#include "semisort_helpers.h"

// ----------------------- DECLARATION -------------------------
namespace constants
{
    const float HASH_RANGE_K = 2.25;
    const float SAMPLE_PROBABILITY_CONSTANT = 3;
    const float DELTA_THRESHOLD = 1;
    const float F_C = 1.25;
    const float LIGHT_KEY_BUCKET_CONSTANT = 2;
}

using namespace std;
using parlay::parallel_for;

const float HASH_RANGE_K = constants::HASH_RANGE_K;
const float SAMPLE_PROBABILITY_CONSTANT = constants::SAMPLE_PROBABILITY_CONSTANT;
const float DELTA_THRESHOLD = constants::DELTA_THRESHOLD;
const float F_C = constants::F_C;
const float LIGHT_KEY_BUCKET_CONSTANT = constants::LIGHT_KEY_BUCKET_CONSTANT;

template <class Object, class Key>
void semi_sort_with_hash(parlay::sequence<record<Object, Key>> &arr)
{
    hash<Key> hash_fn;
    uint64_t k = pow(arr.size(), HASH_RANGE_K);

    // Hash every key in parallel
    parallel_for(0, arr.size(), [&](size_t i)
                 { arr[i].hashed_key = hash_fn(arr[i].key) % k + 1; });

#ifdef DEBUG
    cout << "Original Records w/ Hashed Keys: \n";
    for (uint32_t i = 0; i < arr.size(); i++)
    {
        cout << arr[i].obj << " " << arr[i].key << " " << arr[i].hashed_key << endl;
    }
#endif

    // Call the semisort function on the hashed keys
    semi_sort(arr);
}

template <class Object, class Key>
void semi_sort(parlay::sequence<record<Object, Key>> &arr)
{
    size_t n = arr.size();
    auto int_scrap = parlay::sequence<uint64_t>::uninitialized(2 * n);
    auto record_scrap = parlay::sequence<record<Object, Key>>::uninitialized(100 * n);
    auto buckets = parlay::sequence<record<Object, Key>>::uninitialized(100 * n);
    semi_sort_without_alloc(arr, int_scrap, record_scrap, buckets);
}

template <class Object, class Key>
void semi_sort_without_alloc(
    parlay::sequence<record<Object, Key>> &arr,
    parlay::sequence<uint64_t> &int_scrap,
    parlay::sequence<record<Object, Key>> &record_scrap,
    parlay::sequence<record<Object, Key>> &buckets)
{
    // Create a frequency map for step 4
    size_t n = arr.size();
    parlay::random_generator gen;
    std::uniform_int_distribution<size_t> dis(0, n - 1);

    // Step 2
    double logn = log2((double)n);
    double p = min(SAMPLE_PROBABILITY_CONSTANT / logn, 0.25); // this is theta(1 / log n) so we can autotune later
    uint32_t num_samples = floor(n * p) - 1;
    assert(num_samples != 0);

    get_sampled_elements(arr, int_scrap, record_scrap, num_samples, n, gen, dis);

    // hash table T
    parlay::hashtable<hash_buckets> hash_table(2 * n, hash_buckets());
    parlay::sequence<Bucket> heavy_key_buckets;
    uint32_t num_buckets = LIGHT_KEY_BUCKET_CONSTANT * ((double)n / logn / logn + 1);
    parlay::sequence<Bucket> light_buckets(num_buckets);
    size_t nk = pow(arr.size(), HASH_RANGE_K);
    uint64_t bucket_range = (double)nk / (double)num_buckets;
    uint32_t current_bucket_offset = get_bucket_sizes(
        arr, int_scrap, record_scrap, hash_table,
        heavy_key_buckets, light_buckets,
        num_samples, num_buckets, bucket_range, n, DELTA_THRESHOLD, p, F_C
    );
    uint32_t buckets_size = current_bucket_offset + n;

    // insert buckets into table in parallel
    parallel_for(0, heavy_key_buckets.size(), [&](size_t i) { 
        hash_table.insert(heavy_key_buckets[i]); 
    });
    parallel_for(0, num_buckets, [&](size_t i) { 
        hash_table.insert(light_buckets[i]); 
    });

#ifdef DEBUG
    cout << "buckets" << endl;
    parlay::sequence<Bucket> entries = hash_table.entries();
    for (uint32_t i = 0; i < entries.size(); i++)
    {
        cout << entries[i].bucket_id << " " << entries[i].offset << " " << entries[i].size << " " << entries[i].isHeavy << " " << endl;
    }
#endif

    uint32_t num_partitions = (int)((double)n / logn);
    // scatter keys
    scatter_keys(arr, buckets, hash_table, n, logn, num_partitions, bucket_range, gen, dis, true);
    scatter_keys(arr, buckets, hash_table, n, logn, num_partitions, bucket_range, gen, dis, false);

    // Step 7b, 7c
    sort_light_buckets(buckets, light_buckets, n, num_buckets);
#ifdef DEBUG
    cout << "bucket" << endl;
    for (uint32_t i = 0; i < buckets_size; i++)
    {
        cout << i << " " << buckets[i].obj << " " << buckets[i].key << " " << buckets[i].hashed_key << endl;
    }
#endif

    // step 8
    pack_elements(arr, buckets, buckets_size);

#ifdef DEBUG
    cout << "final result" << endl;
    for (uint32_t i = 0; i < arr.size(); i++)
    {
        cout << i << " " << arr[i].obj << " " << arr[i].key << " " << arr[i].hashed_key << endl;
    }
#endif
}
