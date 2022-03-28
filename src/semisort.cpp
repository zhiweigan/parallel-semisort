#include "semisort.h"

using namespace std;
using parlay::parallel_for;

// #define DEBUG 1

// #define HASH_RANGE_K 2.25 // moved to the header file
const float HASH_RANGE_K = constants::HASH_RANGE_K;

#define SAMPLE_PROBABILITY_CONSTANT 1
#define DELTA_THRESHOLD 1
#define F_C 1.25
#define LIGHT_KEY_BUCKET_CONSTANT 2

template <class Object, class Key>
void semi_sort_with_hash(parlay::sequence<record<Object, Key>> &arr)
{
    hash<Key> hash_fn;
    unsigned long long k = pow(arr.size(), HASH_RANGE_K);

    // Hash every key in parallel
    parallel_for(0, arr.size(), [&](size_t i) {
        arr[i].hashed_key = hash_fn(arr[i].key) % k + 1;
    });

#ifdef DEBUG
    cout<<"Original Records w/ Hashed Keys: \n";
    for(int i = 0; i < arr.size(); i++){
        cout<<arr[i].obj<<" "<<arr[i].key<<" "<<arr[i].hashed_key<<endl;
    }
#endif
    
    // Call the semisort function on the hashed keys
    semi_sort(arr);
}

template <class Object, class Key>
void semi_sort(parlay::sequence<record<Object, Key>> &arr)
{ 
    // Create a frequency map for step 4
    int n = arr.size();

    // Step 2
    double logn = log2((double)n);
    double p = SAMPLE_PROBABILITY_CONSTANT / logn; // this is theta(1 / log n) so we can autotune later

    int num_samples = ceil(1 / p);
    assert(num_samples != 0);

#ifdef DEBUG
    cout << "p: " << p << endl;
    cout << "cp: " << num_samples << endl;
#endif

    // Sample array
    parlay::sequence<bool> sample_index(n);

    // Choose which items to sample
    parallel_for(0, num_samples, [&](size_t i) {
        sample_index[(int)(rand() % num_samples + i / p)] = true;
    });

    // Pack sampled elements into smaller vector
    parlay::sequence<record<Object, Key>> sample = parlay::pack(arr, sample_index);

#ifdef DEBUG
    cout << "Sample Objects:" << endl;
    for (int i = 0; i < num_samples; i++) {
        cout << sample[i].obj << " " << sample[i].key << " " << sample[i].hashed_key << endl;
    }
#endif

    // Step 3 sort samples so we can more easily determine offsets
    auto comp = [&](record<Object, Key> x) { return x.hashed_key; };
    parlay::internal::integer_sort(parlay::make_slice(sample.begin(), sample.end()), comp, sizeof(int));

    // Step 4 
    int gamma = DELTA_THRESHOLD * log(n);

#ifdef DEBUG
    cout << "Gamma: " << gamma << endl;
#endif

    parlay::sequence<int> differences(num_samples);
    // get array differecnes
    parallel_for(0, num_samples, [&](size_t i) {
        if (sample[i].hashed_key != sample[i+1].hashed_key){
            differences[i] = i+1;
        }
    });
    differences[num_samples-1] = num_samples;

    // get offsets of differences in sorted array
    auto offset_filter = [&](int x) { return x != 0; };
    parlay::sequence<int> offsets = parlay::filter(differences, offset_filter);

    int num_unique_in_sample = offsets.size();
    parlay::sequence<int> counts(num_unique_in_sample);
    parlay::sequence<unsigned long long> unique_hashed_keys(num_unique_in_sample);

    // save the unique hashed keys into an array for future use
    parallel_for(0, num_unique_in_sample, [&](size_t i) {
        unique_hashed_keys[i] = sample[offsets[i]-1].hashed_key;
    });

#ifdef DEBUG
    cout << "differences, offsets, uniques" << endl;
    for (int i = 0; i < num_samples; i++) {
        cout << differences[i] << ", ";
    } 
    cout<<endl;
    for (int i = 0; i < num_unique_in_sample; i++) {
        cout << offsets[i] << ", ";
    }
    cout<<endl; 
    for (int i = 0; i < num_unique_in_sample; i++) {
        cout << unique_hashed_keys[i] << ", ";
    }
    cout<<endl;
#endif

    // get size of heavy key buckets
    counts[0] = offsets[0];
    parallel_for(1, num_unique_in_sample, [&](size_t i) {
        counts[i] = offsets[i] - offsets[i-1];
    });

    // parallel_for(0, num_unique_in_sample, [&](size_t i) {
    //     if (i == 0){
    //         counts[i] = offsets[i]; 
    //     } else{
    //         counts[i] = offsets[i] - offsets[i-1];
    //     }
    // });

#ifdef DEBUG
    cout<<"counts to bucket sizes"<<endl;
    for (int i = 0; i < num_unique_in_sample; i++) {
        cout << counts[i] << " : " << size_func(counts[i], p, n, F_C) << endl;
    }
#endif

    // hash table T
    parlay::hashtable<hash_buckets> hash_table(n, hash_buckets());

    // calculate the number of light buckets we want
    int num_buckets = LIGHT_KEY_BUCKET_CONSTANT * ((double)n / logn / logn + 1);
    parlay::sequence<int> light_key_bucket_sample_counts(num_buckets);
    parlay::sequence<Bucket> heavy_key_buckets;

    // add heavy buckets and count number of light items in light buckets
    unsigned int current_bucket_offset = 0;
    for(int i = 0; i < num_unique_in_sample; i++) {
        if(counts[i] > gamma){
            unsigned int bucket_size = (unsigned int) size_func(counts[i], p, n, F_C);
            Bucket new_heavy_bucket = {unique_hashed_keys[i], current_bucket_offset, bucket_size, true}; 
            heavy_key_buckets.push_back(new_heavy_bucket);
            current_bucket_offset += bucket_size;
        } else{
            // determine how big we should make the buckets
            unsigned long long bucket_num = unique_hashed_keys[i] / num_buckets;
            light_key_bucket_sample_counts[bucket_num] += counts[i];
        }
    }

    // insert buckets into table in parallel
    parallel_for(0, heavy_key_buckets.size(), [&](size_t i) {
        hash_table.insert(heavy_key_buckets[i]);     
    });

    // partition and create arrays for light keys here
    // 7a
    int nk = pow(arr.size(), HASH_RANGE_K);
    unsigned long long bucket_range = (double) nk / (double) num_buckets;
    parlay::sequence<Bucket> light_buckets(num_buckets);

    for(int i = 0; i < num_buckets; i++){
        unsigned int bucket_size = size_func(light_key_bucket_sample_counts[i], p, n, F_C);
        Bucket new_light_bucket = {i * bucket_range, current_bucket_offset, bucket_size, false};
        light_buckets[i] = new_light_bucket;
        current_bucket_offset += bucket_size;
    }

    parallel_for(0, num_buckets, [&](unsigned long long i) {
        hash_table.insert(light_buckets[i]); 
    });

#ifdef DEBUG
    cout<<"buckets"<<endl;
    parlay::sequence<Bucket> entries = hash_table.entries();
    for(int i = 0; i < entries.size(); i++){
        cout << entries[i].bucket_id << " " << entries[i].offset << " " << entries[i].size << " " << entries[i].isHeavy << " " << endl;
    }
#endif

    // A' in the paper
    parlay::sequence<record<Object, Key>> buckets(current_bucket_offset);

    // scatter heavy keys
    int num_partitions = (int)((double)n / logn);
    parallel_for(0, num_partitions+1, [&](size_t partition) {
        int end_partition = (int)((partition + 1) * logn);
        int end_state = (end_partition > n) ? n : end_partition;
        for(int i = partition * logn; i < end_state; i++) {

        // for(int i = partition * logn; i < (int)((partition + 1) * logn); i++) {
        //     if (i >= n) 
        //         break;

            Bucket entry = hash_table.find(arr[i].hashed_key);
            if (entry == (Bucket){0, 0, 0, 0}) // continue if it is not a heavy key
                continue;

            unsigned int insert_index = entry.offset + rand() % entry.size;
            while (true) {
                record<Object, Key> c = buckets[insert_index];
                if (c.isEmpty()) {
                    if (bucket_cas(&buckets[insert_index].hashed_key, 0, arr[i].hashed_key)) {
                        buckets[insert_index] = arr[i];
                        break;
                    }
                }
                insert_index++;
            }
        }
    });
 

    // 7b
    // scatter light keys
    parallel_for(0, num_partitions+1, [&](size_t partition) {
        int end_partition = (int)((partition + 1) * logn);
        int end_state = (end_partition > n) ? n : end_partition;
        for(int i = partition * logn; i < end_state; i++) {
        // for (int i = partition * logn; i < (int)((partition + 1) * logn); i++) {
        //     if (i >= n)
        //         break;
            int rounded_down_key = round_down(arr[i].hashed_key, bucket_range);
            if (hash_table.find(arr[i].hashed_key) != (Bucket){0, 0, 0, 0}) // perhaps we can remove this somehow
                continue;

            Bucket entry = hash_table.find(rounded_down_key);
            if (entry == (Bucket){0, 0, 0, 0}) 
                continue;

            unsigned int insert_index = entry.offset + rand() % entry.size;
            while (true) {
                record<Object, Key> c = buckets[insert_index];
                if (c.isEmpty()) {
                    if (bucket_cas(&buckets[insert_index].hashed_key, 0, arr[i].hashed_key)) {
                        buckets[insert_index] = arr[i];
                        break;
                    }
                }
                insert_index++;
            }
        }
    });

    // Step 7b, 7c
    auto light_key_filter = [&](record<Object, Key> x) { return x.hashed_key != 0; };
    auto light_key_comparison = [&](record<Object, Key> a, record<Object, Key> b){ return a.hashed_key < b.hashed_key; };
    parallel_for(0, num_buckets, [&](size_t i) {
        // sort here
        int start_range = light_buckets[i].offset;
        int end_range = light_buckets[i].offset + light_buckets[i].size;
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

#ifdef DEBUG
    cout << "bucket" << endl;
    for (int i = 0; i < buckets.size(); i++) {
        cout << i << " " << buckets[i].obj << " " << buckets[i].key << " " << buckets[i].hashed_key << endl;
    }
#endif

    // step 8
    int num_partitions_step8 = min((unsigned int) 1000, current_bucket_offset);
    parlay::sequence<int> interval_length(num_partitions_step8);
    parlay::sequence<int> interval_prefix_sum(num_partitions_step8);
    parallel_for(0, num_partitions_step8+1, [&](size_t partition) { // leq or lt?
        int chunk_length = ceil((double)current_bucket_offset / num_partitions_step8);
        int start_range = chunk_length * partition;
        int cur_chunk_pointer = 0;

        for(int i = 0; i < chunk_length; i++){
            if (buckets[start_range + i].hashed_key != 0) {
                buckets[start_range+cur_chunk_pointer] = buckets[start_range + i];
                cur_chunk_pointer++;
            }
        }
        interval_length[partition] = cur_chunk_pointer;
    });

#ifdef DEBUG
    cout << "bucket after pack" << endl;
    for (int i = 0; i < buckets.size(); i++) {
        cout << i << " " << buckets[i].obj << " " << buckets[i].key << " " << buckets[i].hashed_key << endl;
    }
#endif

    for (int i = 1; i < num_partitions_step8; i++) {
        interval_prefix_sum[i] = interval_length[i] + interval_prefix_sum[i-1];
    }

#ifdef DEBUG
    cout << "interval_prefix_sum" << endl;
    for (int i = 0; i < num_partitions_step8; i++) {
        cout << interval_prefix_sum[i] << " ";
    }
    cout<<endl;
#endif

    parallel_for(0, num_partitions_step8, [&](size_t partition) {
        int chunk_length = ceil((double)current_bucket_offset / num_partitions_step8);
        int start_range = interval_prefix_sum[partition];
        for(int i = 0; i < interval_length[partition]; i++) {
            arr[start_range + i] = buckets[chunk_length * partition + i];
        }
    });

#ifdef DEBUG
    cout << "result" << endl;
    for (int i = 0; i < arr.size(); i++) {
        cout << i << " " << arr[i].obj << " " << arr[i].key << " " << arr[i].hashed_key << endl;
    }
#endif
}

// if what you get from hashing k is the same as what you get from hashing j
// then it is possible k and j are equal, but not necessarily true

// but if k and j are equal, then they will have the same value when you
// hash them guaranteed

int main() {
    int ex_size = 50;
    // parlay::sequence<record<string, int>> arr(ex_size);
    parlay::sequence<record<string, string>> arr(ex_size);
    string boo[4] = {"hello", "goodbye", "wassup", "yoooo"};
    for(int i = 0; i < ex_size; i++){
        // record<string, int> a = {
        record<string, string> a = {
            "object_" + to_string(i),
            // i / 5,
            boo[i % 4],
            0
        };
        arr[i] = a;
    }

    auto rng = default_random_engine {};
    shuffle(arr.begin(), arr.end(), rng);

    semi_sort_with_hash(arr);

    for (int i = 0; i < ex_size; i++) {
        cout << i << " " << arr[i].obj << " " << arr[i].key << " " << arr[i].hashed_key << endl;
    }
}