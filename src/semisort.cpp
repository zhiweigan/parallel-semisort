#include "semisort.h"

using namespace std;
using parlay::parallel_for;

#ifndef PARALLEL
#define parallel_for for
#endif

#define DEBUG 1

#define HASH_RANGE_K 3
#define SAMPLE_PROBABILITY_CONSTANT 1
#define DELTA_THRESHOLD 1
#define F_C 1.25
#define LIGHT_KEY_BUCKET_CONSTANT 2

template <class Object, class Key>
void semi_sort(parlay::sequence<record<Object, Key>> &arr)
{
    hash<Key> hash_fn;
    int k = pow(arr.size(), HASH_RANGE_K);

    parallel_for(int i = 0; i < arr.size(); i++){
        arr[i].hashed_key = hash_fn(arr[i].key) % k;
    }

#ifdef DEBUG
    cout<<"Original Records w/ Hashed Keys: \n";
    for(int i = 0; i < arr.size(); i++){
        cout<<arr[i].obj<<" "<<arr[i].key<<" "<<arr[i].hashed_key<<endl;
    }
#endif
    
    semi_sort_recur(arr);
}

template <class Object, class Key>
void semi_sort_recur(parlay::sequence<record<Object, Key>> &arr)
{ 
    // Create a frequency map for step 4
    unordered_map<int, int> key_frequency;
    int n = arr.size();

    // Step 2
    double p = SAMPLE_PROBABILITY_CONSTANT / log2(n); // this is theta(1 / log n) so we can autotune later
    int cp = ceil(1 / p);
    
#ifdef DEBUG
    cout << "p: " << p << endl;
    cout << "cp: " << cp << endl;
#endif

    // Sample array
    parlay::sequence<bool> sample_index(n);
    parallel_for (int i = 0; i < cp; i++) {
        sample_index[(int)(rand() % cp + i / p)] = true;
    }
    parlay::sequence<record<Object, Key>> sample = parlay::pack(arr, sample_index);

#ifdef DEBUG
    cout << "Sample Objects:" << endl;
    for (int i = 0; i < cp; i++) {
        cout << sample[i].obj << " " << sample[i].key << " " << sample[i].hashed_key << endl;
    }
#endif

    // update key freq map (how to optimize?)
    for(int i = 0; i < cp; i++) {
        if (key_frequency.find(sample[i].hashed_key) == key_frequency.end()) {
            key_frequency[sample[i].hashed_key] = 0;
        }
        key_frequency[sample[i].hashed_key]++;
    }


#ifdef DEBUG
    cout << "Key Frequency:" << endl;
    for (pair<int, int> key_and_freq : key_frequency) {
        cout << key_and_freq.first << " " << key_and_freq.second << endl;
    }
#endif

    // Step 3
    auto comp = [&](record<Object, Key> x) { return x.hashed_key; };
    parlay::internal::integer_sort(parlay::make_slice(sample.begin(), sample.end()), comp, sizeof(int));

    // Step 4
    int gamma = DELTA_THRESHOLD * log(n);

#ifdef DEBUG
    cout << "Gamma: " << gamma << endl;
#endif

    // debug this
    parlay::sequence<int> differences(cp);
    parlay::sequence<int> unique_hashed_keys;
    parallel_for(int i = 1; i < cp; i++){
        if (sample[i].hashed_key != sample[i - 1].hashed_key){
            differences[i-1] = i;
            unique_hashed_keys.push_back(sample[i-1].hashed_key);
        } else{
            differences[i-1] = -1;
        }
    }
    differences[cp-1] = cp;
    unique_hashed_keys.push_back(sample[cp-1].hashed_key); 

    auto offset_filter = [&](int x) { return x != -1; };
    parlay::sequence<int> offsets = parlay::filter(differences, offset_filter);
    parlay::sequence<int> counts(offsets.size());
    parlay::sequence<int> bucket_sizes(offsets.size());

#ifdef DEBUG
    cout << "differences, offsets, uniques" << endl;
    for (int i = 0; i < cp; i++) {
        cout << differences[i] << ", ";
    } 
    cout<<endl;
    for (int i = 0; i < offsets.size(); i++) {
        cout << offsets[i] << ", ";
    }
    cout<<endl; 
    for (int i = 0; i < unique_hashed_keys.size(); i++)
    {
        cout << unique_hashed_keys[i] << ", ";
    }
    cout<<endl;
#endif

    // make sure this is corect
    parallel_for(int i = 0; i < offsets.size(); i++){
        if (i == 0){
            counts[i] = offsets[i]; 
        } else{
            counts[i] = offsets[i] - offsets[i-1];
        }
        bucket_sizes[i] = size_func(counts[i], p, n, F_C);
    }

#ifdef DEBUG
    cout<<"counts to bucket sizes"<<endl;
    for (int i = 0; i < offsets.size(); i++) {
        cout << counts[i] << " : " << bucket_sizes[i] << endl;
    }
#endif

    parlay::hashtable<hash_numeric<long long>> hashed_key_to_offset(n, hash_numeric<long long>());
    parlay::hashtable<hash_numeric<long long>> hashed_key_to_bucket_size(n, hash_numeric<long long>());
    parlay::hashtable<hash_numeric<long long>> light_hashed_key_to_offset(n, hash_numeric<long long>());
    parlay::hashtable<hash_numeric<long long>> light_hashed_key_to_bucket_size(n, hash_numeric<long long>());
    // add heavy keys
    int current_bucket_offset = 0;
    for(int i = 0; i < offsets.size(); i++) {
        if(counts[i] > gamma){
            Bucket offset = { unique_hashed_keys[i], current_bucket_offset };
            Bucket size = {unique_hashed_keys[i], bucket_sizes[i]};
            hashed_key_to_offset.insert((long long) offset);
            hashed_key_to_bucket_size.insert((long long) size);
            current_bucket_offset += bucket_sizes[i];
        }
    }

    // partition and create arrays for light keys here
    // 7a
    double logn = log2((double)n);
    int num_buckets = LIGHT_KEY_BUCKET_CONSTANT * ((double) n / logn / logn + 1);
    int nk = pow(arr.size(), HASH_RANGE_K);
    int bucket_range = (double) nk / (double) num_buckets;

    parlay::sequence<int> light_bucket_sizes(num_buckets);
    parlay::sequence<int> light_bucket_offsets(num_buckets);

    for(int i = 0; i < num_buckets; i++){
        // do 7a here, populate
        // WIP
        int bucket_size = 32;
        Bucket offset = {i * bucket_range, current_bucket_offset};
        Bucket size = {i * bucket_range, bucket_size};
        light_hashed_key_to_offset.insert((long long)offset);
        light_hashed_key_to_bucket_size.insert((long long)size);
        light_bucket_sizes[i] = bucket_size;
        light_bucket_offsets[i] = current_bucket_offset;
        current_bucket_offset += bucket_size;
    }

#ifdef DEBUG
    cout<<"bucket id to array offset"<<endl;
    parlay::sequence<long long> offset_entries = hashed_key_to_offset.entries();
    parlay::sequence<long long> size_entries = hashed_key_to_bucket_size.entries();
    for(int i = 0; i < offset_entries.size(); i++){
        cout << offset_entries[i] << " " << size_entries[i] << endl;
    }
#endif

    parlay::sequence<record<Object, Key>> buckets(current_bucket_offset);

    // how to get around this?
    parallel_for(int i = 0; i < current_bucket_offset; i++) {
        buckets[i] = {};
        buckets[i].hashed_key = -1;
    }

    // scatter heavy keys
    int num_partitions = (int)((double)n / logn);
    parallel_for(int partition = 0; partition <= num_partitions; partition++) {
        for(int i = partition * logn; i < (int)((partition + 1) * logn); i++) {
            if (i >= n) 
                break;
            if (hashed_key_to_offset.find(arr[i].hashed_key) == (Bucket){-1, -1}) 
                continue;

            long long offset_entry = hashed_key_to_offset.find(arr[i].hashed_key);
            long long size_entry = hashed_key_to_bucket_size.find(arr[i].hashed_key);
            int offset = (int)offset_entry;
            int size = (int)size_entry;
            int insert_index = offset + rand() % size;
            while (true) {
                record<Object, Key> c = buckets[insert_index];
                if (c.isEmpty()) {
                    if (bucket_cas(&buckets[insert_index].hashed_key, -1, arr[i].hashed_key)) {
                        buckets[insert_index] = arr[i];
                        break;
                    }
                }
                insert_index++;
            }
        }
    }
 

    // 7b
    // scatter light keys
    parallel_for(int partition = 0; partition <= num_partitions; partition++) {
        for (int i = partition * logn; i < (int)((partition + 1) * logn); i++) {
            if (i >= n)
                break;
            int rounded_down_key = round_down(arr[i].hashed_key, bucket_range);
            if (hashed_key_to_offset.find(arr[i].hashed_key) != (Bucket){-1, -1})
                continue;
            if (light_hashed_key_to_offset.find(rounded_down_key) == (Bucket){-1, -1})
                continue;

            long long offset_entry = light_hashed_key_to_offset.find(rounded_down_key);
            long long size_entry = light_hashed_key_to_bucket_size.find(rounded_down_key);
            int offset = (int)offset_entry;
            int size = (int)size_entry;
            int insert_index = offset + rand() % size;
            while (true) {
                record<Object, Key> c = buckets[insert_index];
                if (c.isEmpty()) {
                    if (bucket_cas(&buckets[insert_index].hashed_key, -1, arr[i].hashed_key)) {
                        buckets[insert_index] = arr[i];
                        break;
                    }
                }
                insert_index++;
            }
        }
    }

    // Step 7b, 7c
    auto light_key_filter = [&](record<Object, Key> x) { return x.hashed_key != -1; };
    auto light_key_comparison = [&](record<Object, Key> a, record<Object, Key> b){ return a.hashed_key < b.hashed_key; };
    parallel_for(int i = 0; i < num_buckets; i++) {
        // sort here
        int start_range = light_bucket_offsets[i];
        int end_range = light_bucket_offsets[i] + light_bucket_sizes[i];
        parlay::sort_inplace(buckets.cut(start_range, end_range), light_key_comparison);

        auto out_cut = buckets.cut(start_range, end_range);
        parlay::sequence<record<Object, Key>> filtered = parlay::filter(
            buckets.cut(start_range, end_range),
            light_key_filter
        );
        parallel_for(int j = start_range; j < end_range; j++){
            if (j - start_range < filtered.size()) {
                buckets[j] = filtered[j-start_range];
            } else{
                buckets[j] = (record<Object, Key>){};
                buckets[j].hashed_key = -1;
            }
        }
    }

#ifdef DEBUG
    cout << "bucket" << endl;
    for (int i = 0; i < buckets.size(); i++)
    {
        cout << i << " " << buckets[i].obj << " " << buckets[i].key << " " << buckets[i].hashed_key << endl;
    }
#endif

    // step 8
    int num_partitions_step8 = min(1000, current_bucket_offset);
    parlay::sequence<int> interval_length(num_partitions_step8);
    parlay::sequence<int> interval_prefix_sum(num_partitions_step8);
    parallel_for(int partition = 0; partition < num_partitions_step8; partition++)
    {
        int chunk_length = ceil((double)current_bucket_offset / num_partitions_step8);
        int start_range = chunk_length * partition;
        int cur_chunk_pointer = 0;

        for(int i = 0; i < chunk_length; i++){
            if (buckets[start_range + i].hashed_key != -1) {
                buckets[cur_chunk_pointer] = buckets[start_range + i];
                cur_chunk_pointer++;
            }
        }
        interval_length[partition] = cur_chunk_pointer;
    }

#ifdef DEBUG
    cout << "bucket after pack" << endl;
    for (int i = 0; i < buckets.size(); i++)
    {
        cout << i << " " << buckets[i].obj << " " << buckets[i].key << " " << buckets[i].hashed_key << endl;
    }
#endif

    for (int i = 1; i < num_partitions_step8; i++) {
        interval_prefix_sum[i] = interval_length[i] + interval_prefix_sum[i-1];
    }

#ifdef DEBUG
    cout << "interval_prefix_sum" << endl;
    for (int i = 0; i < num_partitions_step8; i++)
    {
        cout << interval_prefix_sum[i] << endl;
    }
#endif

    parlay::sequence<record<Object, Key>> result(n);
    parallel_for(int partition = 0; partition < num_partitions_step8; partition++)
    {
        int chunk_length = ceil((double)current_bucket_offset / num_partitions_step8);
        int start_range = interval_prefix_sum[partition];
        for(int i = 0; i < interval_length[partition]; i++) {
            result[start_range + i] = buckets[chunk_length * partition + i];
        }
    }

#ifdef DEBUG
    cout << "result" << endl;
    for (int i = 0; i <= result.size(); i++)
    {
        cout << i << " " << result[i].obj << " " << result[i].key << " " << result[i].hashed_key << endl;
    }
#endif
}

// if what you get from hashing k is the same as what you get from hashing j
// then it is possible k and j are equal, but not necessarily true

// but if k and j are equal, then they will have the same value when you
// hash them guaranteed

int main() {
    int ex_size = 20;
    parlay::sequence<record<string, int>> arr(ex_size);
    for(int i = 0; i < ex_size; i++){
        record<string, int> a = {
            "object_" + to_string(i),
            i / 10,
            0
        };
        arr[i] = a;
    }

    auto rng = default_random_engine {};
    shuffle(arr.begin(), arr.end(), rng);

    semi_sort(arr);
}