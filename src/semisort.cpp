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

    // Step 2
    double p = SAMPLE_PROBABILITY_CONSTANT / log(arr.size()); // this is theta(1 / log n) so we can autotune later
    int cp = ceil(1 / p);
    
#ifdef DEBUG
    cout << "p: " << p << endl;
    cout << "cp: " << cp << endl;
#endif

    // Sample array
    parlay::sequence<bool> sample_index(arr.size());
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
    auto f = [&](record<Object, Key> x) { return x.hashed_key; };
    parlay::internal::integer_sort(parlay::make_slice(sample.begin(), sample.end()), f, sizeof(int));

    // Step 4
    int gamma = DELTA_THRESHOLD * log(arr.size());

#ifdef DEBUG
    cout << "Gamma: " << gamma << endl;
#endif

    auto H_filter = [&](record<Object, Key> x) 
        { return key_frequency[x.hashed_key] > gamma; };
    auto L_filter = [&](record<Object, Key> x)
        { return key_frequency[x.hashed_key] <= gamma; };

    parlay::sequence<record<Object, Key>> H = parlay::filter(sample, H_filter);
    parlay::sequence<record<Object, Key>> L = parlay::filter(sample, L_filter);

#ifdef DEBUG
    cout<<"Heavy Records:"<<endl;
    for(int i = 0; i < H.size(); i++){
        cout << H[i].obj << " " << H[i].key << " " << H[i].hashed_key <<endl;
    }
    cout << "Light Records:" << endl;
    for (int i = 0; i < L.size(); i++) {
        cout << L[i].obj << " " << L[i].key << " " << L[i].hashed_key << endl;
    }
#endif

    // Step 5
}

// if what you get from hashing k is the same as what you get from hashing j
// then it is possible k and j are equal, but not necessarily true

// but if k and j are equal, then they will have the same value when you
// hash them guaranteed

int main() {
    int ex_size = 10;
    parlay::sequence<record<string, int>> arr(ex_size);
    for(int i = 0; i < ex_size; i++){
        record<string, int> a = {
            "object_" + to_string(i),
            i / 2,
            0
        };
        arr[i] = a;
    }

    auto rng = default_random_engine {};
    shuffle(arr.begin(), arr.end(), rng);

    semi_sort(arr);
}