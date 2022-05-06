#include "semisort_types.h"
#define DEBUG 1

using namespace std;
using parlay::parallel_for;
using parlay::make_slice;

inline unsigned int size_func(unsigned int num_records, double p, unsigned int n, double c)
{
    double lnn = (double)log((double)n);
    double clnn = c * lnn;

    double fs = (num_records + clnn + sqrt(clnn * clnn + 2 * num_records * c * clnn)) / p;
    double array_size = 1.1 * fs;

    return (unsigned int)pow(2, ceil(log(array_size) / log(2)));
}

template <class eType>
inline bool bucket_cas(eType *p, eType o, eType n)
{
    return std::atomic_compare_exchange_strong_explicit(
        reinterpret_cast<std::atomic<eType> *>(p), &o, n, std::memory_order_relaxed, std::memory_order_relaxed);
}

// round n down to nearest multiple of m
uint64_t round_down(uint64_t n, uint64_t m)
{
    return n >= 0 ? (n / m) * m : ((n - m + 1) / m) * m;
}

template <class Object, class Key>
inline void get_sampled_elements(
    parlay::sequence<record<Object, Key>> &arr,
    parlay::sequence<uint64_t> &int_scrap,
    parlay::sequence<record<Object, Key>> &record_scrap,
    int num_samples,
    int n,
    parlay::random_generator gen,
    std::uniform_int_distribution<size_t> dis)
{
    // Choose which items to sample
    parallel_for(0, num_samples, [&](size_t i) {
	    auto r = gen[i];
	    int_scrap[(uint64_t)(dis(r) % (n / num_samples) + i * n / num_samples)] = true; 
    });

    // // Pack sampled elements into smaller vector
    parlay::pack_into_uninitialized(
        arr, 
        make_slice(int_scrap.begin(), int_scrap.begin()), 
        record_scrap
    );

    // Step 3 sort samples so we can more easily determine offsets
    auto comp = [&](record<Object, Key> x)
    { return x.hashed_key; };
    parlay::internal::integer_sort_inplace(
        parlay::make_slice(record_scrap.begin(), record_scrap.begin() + num_samples),
        comp,
        sizeof(uint64_t));

#ifdef DEBUG
    cout << "Sample Objects:" << endl;
    for (uint32_t i = 0; i < num_samples; i++)
    {
        cout << record_scrap[i].obj << " " << record_scrap[i].key << " " << record_scrap[i].hashed_key << endl;
    }
#endif
}

template <class Object, class Key>
inline uint32_t get_bucket_sizes(
    parlay::sequence<record<Object, Key>> &arr,
    parlay::sequence<uint64_t> &int_scrap,
    parlay::sequence<record<Object, Key>> &record_scrap,
    parlay::hashtable<hash_buckets> &hash_table,
    parlay::sequence<Bucket> &heavy_key_buckets,
    parlay::sequence<Bucket> &light_buckets,
    uint32_t num_samples,
    uint32_t num_buckets,
    uint64_t bucket_range,
    size_t n,
    float DELTA_THRESHOLD,
    float p,
    float F_C)
{
    // Step 4
    uint32_t gamma = DELTA_THRESHOLD * log(n);
    parlay::sequence<uint64_t> differences = int_scrap;
    // get array differences
    parallel_for(0, num_samples, [&](size_t i) {
        if (record_scrap[i].hashed_key != record_scrap[i+1].hashed_key){
            differences[i] = i+1;
        } 
    });
    differences[num_samples - 1] = num_samples;

    // get offsets of differences in sorted array
    auto offset_filter = [&](uint32_t x)
    { return x != 0; };
    parlay::sequence<uint64_t> offsets = parlay::filter(differences, offset_filter);

    size_t num_unique_in_sample = offsets.size();
    parlay::sequence<uint64_t> counts(num_unique_in_sample);
    parlay::sequence<uint64_t> unique_hashed_keys(num_unique_in_sample);

    // save the unique hashed keys into an array for future use
    parallel_for(0, num_unique_in_sample, [&](size_t i){
        unique_hashed_keys[i] = record_scrap[offsets[i] - 1].hashed_key; 
    });

    // get size of heavy key buckets
    counts[0] = offsets[0];
    parallel_for(1, num_unique_in_sample, [&](size_t i) { 
        counts[i] = offsets[i] - offsets[i - 1]; 
    });

    // calculate the number of light buckets we want
    parlay::sequence<uint32_t> light_key_bucket_sample_counts(num_buckets);

    // add heavy buckets and count number of light items in light buckets
    uint32_t current_bucket_offset = 0;
    for (uint32_t i = 0; i < num_unique_in_sample; i++)
    {
        if (counts[i] > gamma)
        {
            uint32_t bucket_size = (uint32_t)size_func(counts[i], p, n, F_C);
            Bucket new_heavy_bucket = {unique_hashed_keys[i], current_bucket_offset, bucket_size, true};
            heavy_key_buckets.push_back(new_heavy_bucket);
            current_bucket_offset += bucket_size;
        }
        else
        {
            // determine how big we should make the buckets
            uint64_t bucket_num = unique_hashed_keys[i] / bucket_range;
            light_key_bucket_sample_counts[bucket_num] += counts[i];
        }
    }

    for (uint32_t i = 0; i < num_buckets; i++)
    {
        uint32_t bucket_size = size_func(light_key_bucket_sample_counts[i], p, n, F_C);
        Bucket new_light_bucket = {i * bucket_range, current_bucket_offset, bucket_size, false};
        light_buckets[i] = new_light_bucket;
        current_bucket_offset += bucket_size;
    }

#ifdef DEBUG
    cout << "differences, offsets, uniques" << endl;
    for (uint32_t i = 0; i < num_samples; i++)
    {
        cout << differences[i] << ", ";
    }
    cout << endl;
    for (uint32_t i = 0; i < num_unique_in_sample; i++)
    {
        cout << offsets[i] << ", ";
    }
    cout << endl;
    for (uint32_t i = 0; i < num_unique_in_sample; i++)
    {
        cout << unique_hashed_keys[i] << ", ";
    }
    cout << endl;
    cout << "counts to bucket sizes" << endl;
    for (uint32_t i = 0; i < num_unique_in_sample; i++)
    {
        cout << counts[i] << endl;
    }
#endif

    return current_bucket_offset;
}

template <class Object, class Key>
inline void scatter_keys(
    parlay::sequence<record<Object, Key>> &arr,
    parlay::sequence<record<Object, Key>> &buckets,
    parlay::hashtable<hash_buckets> &hash_table,
    uint32_t n,
    double logn,
    uint32_t num_partitions,
    uint64_t bucket_range,
    parlay::random_generator gen,
    std::uniform_int_distribution<size_t> dis,
    bool isHeavy)
{
    parallel_for(0, num_partitions + 1, [&](size_t partition) {
        uint32_t end_partition = (uint32_t)((partition + 1) * logn);
        uint32_t end_state = (end_partition > n) ? n : end_partition;
        for(uint32_t i = partition * logn; i < end_state; i++) {
            Bucket entry;
            if (isHeavy) {
                entry = hash_table.find(arr[i].hashed_key);
                if (entry == (Bucket){0, 0, 0, 0}) // continue if it is not a heavy key
                    continue;
            } else {
                uint64_t rounded_down_key = round_down(arr[i].hashed_key, bucket_range);
                if (hash_table.find(arr[i].hashed_key) != (Bucket){0, 0, 0, 0}) // perhaps we can remove this somehow
                    continue;

                entry = hash_table.find(rounded_down_key);
                if (entry == (Bucket){0, 0, 0, 0})
                    continue;
            }

            auto r = gen[partition];
            uint32_t insert_index = entry.offset + dis(r) % entry.size;
            while (true) {
                record<Object, Key> c = buckets[insert_index];
                if (c.isEmpty()) {
                    if (bucket_cas(&buckets[insert_index].hashed_key, (uint64_t)0, arr[i].hashed_key)) {
                        buckets[insert_index] = arr[i];
                        break;
                    }
                }
                insert_index++;
                if (insert_index >= entry.offset + entry.size) {
                  insert_index = entry.offset + dis(r) % entry.size;
                }
            }
        } 
    });
}

template <class Object, class Key>
inline void sort_light_buckets(
    parlay::sequence<record<Object, Key>> &buckets,
    parlay::sequence<Bucket> &light_buckets,
    uint32_t n,
    uint32_t num_buckets)
{
    auto light_key_filter = [&](record<Object, Key> x)
    { return x.hashed_key != 0; };
    auto light_key_comparison = [&](record<Object, Key> a, record<Object, Key> b)
    { return a.hashed_key < b.hashed_key; };
    parallel_for(0, num_buckets, [&](size_t i) {
        // sort here
        uint32_t start_range = light_buckets[i].offset;
        uint32_t end_range = light_buckets[i].offset + light_buckets[i].size;
        parlay::sort_inplace(buckets.cut(start_range, end_range), light_key_comparison); // sort light buckets

        auto out_cut = buckets.cut(start_range, end_range); // is this packing correct?
        parlay::sequence<record<Object, Key>> filtered = parlay::filter(
            buckets.cut(start_range, end_range),
            light_key_filter
        ); 
        parallel_for(start_range, end_range, [&](size_t j) {
            if (j - start_range < filtered.size()) {
                buckets[j] = filtered[j-start_range];
            } else{
                buckets[j] = (record<Object, Key>){};
                buckets[j].hashed_key = 0;
            }
        }); 
    });
}

template <class Object, class Key>
inline void pack_elements(
    parlay::sequence<record<Object, Key>> &arr,
    parlay::sequence<record<Object, Key>> &buckets,
    uint32_t buckets_size)
{
    uint32_t num_partitions_step8 = min((uint32_t)1000, (uint32_t)buckets_size);
    parlay::sequence<int> interval_length(num_partitions_step8);
    parlay::sequence<int> interval_prefix_sum(num_partitions_step8);
    parallel_for(0, num_partitions_step8 + 1, [&](size_t partition) { // leq or lt?
        uint32_t chunk_length = ceil((double)buckets_size / num_partitions_step8);
        uint32_t start_range = chunk_length * partition;
        uint32_t cur_chunk_pointer = 0;

        for (uint32_t i = 0; i < chunk_length; i++)
        {
            if (start_range + i >= buckets_size)
                break;
            if (buckets[start_range + i].hashed_key != 0)
            {
                buckets[start_range + cur_chunk_pointer] = buckets[start_range + i];
                cur_chunk_pointer++;
            }
        }
        interval_length[partition] = cur_chunk_pointer;
    });

#ifdef DEBUG
    cout << "bucket after pack" << endl;
    for (uint32_t i = 0; i < buckets_size; i++)
    {
        cout << i << " " << buckets[i].obj << " " << buckets[i].key << " " << buckets[i].hashed_key << endl;
    }
#endif

    for (uint32_t i = 1; i < num_partitions_step8; i++)
    {
        interval_prefix_sum[i] = interval_length[i - 1] + interval_prefix_sum[i - 1];
    }

#ifdef DEBUG
    cout << "interval_prefix_sum" << endl;
    for (uint32_t i = 0; i < num_partitions_step8; i++)
    {
        cout << interval_prefix_sum[i] << " ";
    }
    cout << endl;
#endif

    parallel_for(0, num_partitions_step8, [&](size_t partition) {
        uint32_t chunk_length = ceil((double)buckets_size / num_partitions_step8);
        uint32_t start_range = interval_prefix_sum[partition];
        for(uint32_t i = 0; i < interval_length[partition]; i++) {
            arr[start_range + i] = buckets[chunk_length * partition + i];
        } 
    });
}