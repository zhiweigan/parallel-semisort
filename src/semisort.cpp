#include "semisort.h"

using namespace std;
using parlay::parallel_for;

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

    parallel_for(0, arr.size(), [&](size_t i) {
        arr[i].hashed_key = hash_fn(arr[i].key) % k + 1;
    });

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
    int n = arr.size();

    // Step 2
    double logn = log2((double)n);
    double p = SAMPLE_PROBABILITY_CONSTANT / logn; // this is theta(1 / log n) so we can autotune later
    int cp = ceil(1 / p);
    assert(cp != 0);

#ifdef DEBUG
    cout << "p: " << p << endl;
    cout << "cp: " << cp << endl;
#endif

    // Sample array
    parlay::sequence<bool> sample_index(n);
    parallel_for(0, cp, [&](size_t i) {
        sample_index[(int)(rand() % cp + i / p)] = true;
    });
    parlay::sequence<record<Object, Key>> sample = parlay::pack(arr, sample_index);

#ifdef DEBUG
    cout << "Sample Objects:" << endl;
    for (int i = 0; i < cp; i++) {
        cout << sample[i].obj << " " << sample[i].key << " " << sample[i].hashed_key << endl;
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

    parlay::sequence<int> differences(cp);
    parallel_for(0, cp, [&](size_t i) {
        if (sample[i].hashed_key != sample[i+1].hashed_key){
            differences[i] = i+1;
        }
    });
    differences[cp-1] = cp;

    auto offset_filter = [&](int x) { return x != 0; };
    parlay::sequence<int> offsets = parlay::filter(differences, offset_filter);

    int num_unique_in_sample = offsets.size();
    parlay::sequence<int> counts(num_unique_in_sample);
    parlay::sequence<int> unique_hashed_keys(num_unique_in_sample);

    parallel_for(0, num_unique_in_sample, [&](size_t i) {
        unique_hashed_keys[i] = sample[offsets[i]-1].hashed_key;
    });

#ifdef DEBUG
    cout << "differences, offsets, uniques" << endl;
    for (int i = 0; i < cp; i++) {
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
    parallel_for(0, num_unique_in_sample, [&](size_t i) {
        if (i == 0){
            counts[i] = offsets[i]; 
        } else{
            counts[i] = offsets[i] - offsets[i-1];
        }
    });

#ifdef DEBUG
    cout<<"counts to bucket sizes"<<endl;
    for (int i = 0; i < num_unique_in_sample; i++) {
        cout << counts[i] << " : " << size_func(counts[i], p, n, F_C) << endl;
    }
#endif

    parlay::hashtable<hash_numeric<long long>> hashed_key_to_offset(n, hash_numeric<long long>());
    parlay::hashtable<hash_numeric<long long>> hashed_key_to_bucket_size(n, hash_numeric<long long>());
    parlay::hashtable<hash_numeric<long long>> light_hashed_key_to_offset(n, hash_numeric<long long>());
    parlay::hashtable<hash_numeric<long long>> light_hashed_key_to_bucket_size(n, hash_numeric<long long>());

    int num_buckets = LIGHT_KEY_BUCKET_CONSTANT * ((double)n / logn / logn + 1);
    parlay::sequence<int> light_key_bucket_sample_counts(num_buckets);
    parlay::sequence<pair<int, pair<int, int>>> heavy_key_bucket_sizes;

    // add heavy keys
    int current_bucket_offset = 0;
    for(int i = 0; i < num_unique_in_sample; i++) {
        if(counts[i] > gamma){
            int bucket_size = size_func(counts[i], p, n, F_C);
            heavy_key_bucket_sizes.push_back(make_pair(unique_hashed_keys[i], make_pair(current_bucket_offset, bucket_size)));
            current_bucket_offset += bucket_size;
        } else{
            int bucket_num = unique_hashed_keys[i] / num_buckets;
            light_key_bucket_sample_counts[bucket_num] += counts[i];
        }
    }

    parallel_for(0, heavy_key_bucket_sizes.size(), [&](size_t i) {
        pair<int, pair<int, int>> key_with_properties = heavy_key_bucket_sizes[i];
        Bucket offset = {key_with_properties.first, key_with_properties.second.first};
        Bucket size = {key_with_properties.first, key_with_properties.second.second};
        hashed_key_to_offset.insert((long long)offset);    
        hashed_key_to_bucket_size.insert((long long)size); 
    });

    // partition and create arrays for light keys here
    // 7a
    int nk = pow(arr.size(), HASH_RANGE_K);
    int bucket_range = (double) nk / (double) num_buckets;

    parlay::sequence<int> light_bucket_sizes(num_buckets);
    parlay::sequence<int> light_bucket_offsets(num_buckets);

    for(int i = 0; i < num_buckets; i++){
        int bucket_size = size_func(light_key_bucket_sample_counts[i], p, n, F_C);
        light_bucket_offsets[i] = current_bucket_offset;
        light_bucket_sizes[i] = bucket_size;
        current_bucket_offset += bucket_size;
    }

    parallel_for(0, num_buckets, [&](int i) {
        Bucket offset = {i * bucket_range, light_bucket_offsets[i]};
        Bucket size = {i * bucket_range, light_bucket_sizes[i]};
        light_hashed_key_to_offset.insert((long long)offset);   
        light_hashed_key_to_bucket_size.insert((long long)size); 
    });

#ifdef DEBUG
    cout<<"bucket id to array offset"<<endl;
    parlay::sequence<long long> offset_entries = hashed_key_to_offset.entries();
    parlay::sequence<long long> size_entries = hashed_key_to_bucket_size.entries();
    for(int i = 0; i < offset_entries.size(); i++){
        cout << offset_entries[i] << " " << size_entries[i] << endl;
    }
#endif

    // A' in the paper
    parlay::sequence<record<Object, Key>> buckets(current_bucket_offset);

    // scatter heavy keys
    int num_partitions = (int)((double)n / logn);
    parallel_for(0, num_partitions+1, [&](size_t partition) {
        for(int i = partition * logn; i < (int)((partition + 1) * logn); i++) {
            if (i >= n) 
                break;
            if (hashed_key_to_offset.find(arr[i].hashed_key) == (Bucket){-1, -1}) // continue if it is not a heavy key
                continue;

            long long offset_entry = hashed_key_to_offset.find(arr[i].hashed_key);
            long long size_entry = hashed_key_to_bucket_size.find(arr[i].hashed_key);
            int offset = (int)offset_entry;
            int size = (int)size_entry;
            int insert_index = offset + rand() % size;
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
        for (int i = partition * logn; i < (int)((partition + 1) * logn); i++) {
            if (i >= n)
                break;
            int rounded_down_key = round_down(arr[i].hashed_key, bucket_range);
            if (hashed_key_to_offset.find(arr[i].hashed_key) != (Bucket){-1, -1}) // exclude keys that are heavy keys
                continue;
            if (light_hashed_key_to_offset.find(rounded_down_key) == (Bucket){-1, -1}) // redundant? check if this affects runtime
                continue;

            long long offset_entry = light_hashed_key_to_offset.find(rounded_down_key);
            long long size_entry = light_hashed_key_to_bucket_size.find(rounded_down_key);
            int offset = (int)offset_entry;
            int size = (int)size_entry;
            int insert_index = offset + rand() % size;
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
        int start_range = light_bucket_offsets[i];
        int end_range = light_bucket_offsets[i] + light_bucket_sizes[i];
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
    for (int i = 0; i < buckets.size(); i++)
    {
        cout << i << " " << buckets[i].obj << " " << buckets[i].key << " " << buckets[i].hashed_key << endl;
    }
#endif

    // step 8
    int num_partitions_step8 = min(1000, current_bucket_offset);
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
    for (int i = 0; i < arr.size(); i++)
    {
        cout << i << " " << arr[i].obj << " " << arr[i].key << " " << arr[i].hashed_key << endl;
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
            i / 15,
            0
        };
        arr[i] = a;
    }

    auto rng = default_random_engine {};
    shuffle(arr.begin(), arr.end(), rng);

    semi_sort(arr);
}