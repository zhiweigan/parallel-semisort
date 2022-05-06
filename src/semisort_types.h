#include "parlay/utilities.h"
#include "parlay/primitives.h"
#include "parlay/parallel.h"
#include "parlay/sequence.h"
#include "parlay/hash_table.h"
#include "parlay/random.h"

template <class A, class B>
struct record
{
    A obj;
    B key;
    uint64_t hashed_key;

    inline bool isEmpty()
    {
        return hashed_key == 0;
    }

    inline bool operator!=(record a)
    {
        return a.hashed_key != hashed_key || a.obj != obj || a.key != key;
    }

    inline bool operator==(record a)
    {
        return a.hashed_key == hashed_key && a.obj == obj && a.key == key;
    }
};

struct Bucket
{
    unsigned long long bucket_id;
    unsigned int offset;
    unsigned int size : 31;
    bool isHeavy;

    inline bool operator!=(Bucket a)
    {
        return a.bucket_id != bucket_id || a.offset != offset || a.size != size || a.isHeavy != isHeavy;
    }

    inline bool operator==(Bucket a)
    {
        return a.bucket_id == bucket_id && a.size == size && a.offset == offset && a.isHeavy == isHeavy;
    }
};

struct hash_buckets
{
    using eType = Bucket;
    using kType = unsigned long long;
    eType empty() { return {0, 0, 0, 0}; }
    kType getKey(eType v) { return v.bucket_id; }
    size_t hash(kType v) { return static_cast<size_t>(parlay::hash64(v)); }
    int cmp(kType v, kType b) { return (v > b) ? 1 : ((v == b) ? 0 : -1); }
    bool replaceQ(eType, eType) { return 0; }
    eType update(eType v, eType) { return v; }
    bool cas(eType *p, eType o, eType n)
    {
        auto pb = reinterpret_cast<std::atomic<unsigned long long> *>(&(p->bucket_id));
        if (std::atomic_compare_exchange_strong_explicit(pb, &(o.bucket_id), n.bucket_id, std::memory_order_relaxed, std::memory_order_relaxed))
        {
            *p = n;
            return true;
        }
        return false;
    }
};