#include <stdio.h>
#include <iostream>
#include <functional>
#include "../parlaylib/include/parlay/parallel.h"
#include "../parlaylib/include/parlay/sequence.h"

using namespace std;

template <class a, class b>
struct record {
    a obj;
    b key;
    int hashed_key;
};

template <class T>
void semiSort(parlay::sequence<T> &A){
    // Step 1
    hash<T, int> hash_fn;
    int param1 = 3; // TODO: this is a parameter we can change -- k > 2 from algo
    int k = pow(A.size(), param1);
    parlay::parallel_for (int i = 0; i < A.size(); i++) { // TODO: can parallelize
        A[i].hashed_key = hash_fn(A[i]) % k;
    }
    // Create a frequency map for step 4
    unordered_map<int, int> key_frequency;
    // Step 2
    paralay::sequence<T> sample;
    int param2 = 1; 
    int p = param2 / log(A.size()); // this is theta(1 / log n) so we can autotune later
    int cp = ceil(1 / p);
    // parlay::parallel_for (int i = 0; i < p; i++) {
    for (int i = 0; i < p; i++) {
        sample[i] = A[rand() % cp + i/p];
        // Update frequency map for step 4
        if (key_frequency.find(sample[i]) == key_frequency.end()) {
            key_frequency[sample[i]] = 0;
        }
        key_frequency[sample[i]]++;
    }
    // Step 3
    sample.sort(); // TODO: this needs to be radix sort from PBBS
    // Step 4
    unordered_set<int> h;
    unordered_set<int> l;
    int param3 = 1;
    int gamma = param3 * log(A.size())
    for (int key : key_frequency) {
        if (key_frequency[key] > gamma) {
            h.push(key);
        } else {
            l.push(key);
        }
    }
    // Step 5
}

// if what you get from hashing k is the same as what you get from hashing j
// then it is possible k and j are equal, but not necessarily true

// but if k and j are equal, then they will have the same value when you
// hash them guaranteed