#include "semisort_header.h"
#include <fstream>

// runs a small test case
int main() {
    uint32_t ex_size = 1000;
    parlay::sequence<record<string, uint64_t>> int_keys(ex_size);
    for(int i = 0; i < ex_size; i++){
        record<string, uint64_t> a = {
            "object_" + to_string(i),
            static_cast<unsigned long long>(i % 5 + 1),
            static_cast<unsigned long long>(i % 5 + 1)
        };
        int_keys[i] = a;
    }

    // auto rng = default_random_engine {};
    // shuffle(int_keys.begin(), int_keys.end(), rng);

    // std::ifstream inputFile("/Users/zhiweigan/Desktop/Spring 2022/6.827/parallel-semisort/src/test/zipfDistSeq_10000_100000000");
    // std::ifstream inputFile("/Users/zhiweigan/Desktop/Spring 2022/6.827/parallel-semisort/src/test/expDistSeq_10000_100");

    // long k = pow(ex_size, HASH_RANGE_K);
    // std::string myline;
    // int i = 0;
    // if (inputFile.is_open())
    // {
    //     std::getline(inputFile, myline);
    //     while (inputFile)
    //     {
    //         std::getline(inputFile, myline);

    //         try {
    //             // cout<<stoull(myline)<<endl;
    //             record<string, uint64_t> a = {
    //                 "object_" + to_string(i),
    //                 stoull(myline) % k + 1,
    //                 stoull(myline) % k + 1,
    //             };

    //             int_keys[i] = a;
    //             i++;
    //         } catch (const std::exception& e) {

    //         }
    //     }
    // }
    // cout<<i<<endl;


    semi_sort(int_keys);

    cout<<"finished"<<endl;

    for (uint32_t i = 0; i < ex_size; i++) {
        cout << i << " " << int_keys[i].obj << " " << int_keys[i].key << " " << int_keys[i].hashed_key << endl;
    }

    // parlay::sequence<record<string, uint8_t>> string_keys(ex_size);
    // uint8_t keys[4] = {135, 236, 1, 10};
    // for (uint32_t i = 0; i < ex_size; i++)
    // {
    //     record<string, uint8_t> a = {
    //         "object_" + to_string(i),
    //         keys[i % 4],
    //         0};
    //     string_keys[i] = a;
    // }

    // shuffle(string_keys.begin(), string_keys.end(), rng);

    // semi_sort_with_hash(string_keys);

    // cout<<"finished"<<endl;

    // for (uint32_t i = 0; i < ex_size; i++)
    // {
    //     cout << i << " " << string_keys[i].obj << " " << string_keys[i].key << " " << string_keys[i].hashed_key << endl;
    // }
}