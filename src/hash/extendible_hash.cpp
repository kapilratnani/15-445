#include <list>

#include "hash/extendible_hash.h"
#include "page/page.h"
#include "common/logger.h"
using namespace std;

namespace cmudb {

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size) {
  this->globalDepth = 1;
  this->bucketSize = size;
  this->numBuckets = 2;
  this->buckets = new Bucket*[this->numBuckets];
  this->buckets[0] = new Bucket();
  this->buckets[1] = new Bucket();
}

template <typename K, typename V>
void ExtendibleHash<K, V>::doubleBuckets(){
  this->bucketsLatch.lock();

  size_t curSize = this->numBuckets;
  size_t newSize = 2*curSize;
  // LOG_INFO("# Resizing, curSize:%zu, newSize:%zu", curSize, newSize);
  
  Bucket** newBuckets = new Bucket*[newSize];

  for(size_t i=0;i<curSize;i++) {
    newBuckets[i] = this->buckets[i];
    newBuckets[i+curSize] = this->buckets[i];
  }
  auto oldBuckets = this->buckets;
  delete [] oldBuckets;
  this->buckets = newBuckets;
  this->globalDepth++;
  this->numBuckets = newSize;

  this->bucketsLatch.unlock();
}

template <typename K, typename V>
bool ExtendibleHash<K, V>::isFull(Bucket& bucket){
  return bucket.entries.size() > this->bucketSize;
}

/*
 * helper function to calculate the hashing address of input key
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key) {
  std::hash<K> hasher;
  return hasher(key);
}

template <typename K, typename V>
int ExtendibleHash<K, V>::BucketIndex(size_t h) {
  // wait if bucket are being doubled
  std::lock_guard<mutex> lck(this->bucketsLatch);
  return h & ((1 << this->globalDepth) - 1);
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const {
  return this->globalDepth;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
  return this->buckets[bucket_id]->depth;
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
  return this->numBuckets;
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
  size_t hashKey = this->HashKey(key);
  int bucketId = this->BucketIndex(hashKey);
  Bucket * bucket = this->buckets[bucketId];
  for(auto entry:bucket->entries){
    if(entry.first == key){
      value = entry.second;
      return true;
    }
  }
  return false;
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {
  size_t hashKey = this->HashKey(key);
  int bucketId = this->BucketIndex(hashKey);
  Bucket * bucket = this->buckets[bucketId];

  std::lock_guard<mutex> lck(bucket->latch);

  auto &vec = bucket->entries;
  auto it = vec.begin();
  while(it != vec.end()){
    if((*it).first == key){
      it = vec.erase(it);
      return true;
    }else
      ++it;
  }
  return false;
}

template <typename K, typename V>
void ExtendibleHash<K, V>::redist(const K &key, Bucket *bucket) {
  if(bucket == nullptr)
    return;
  Bucket *fullBucket = bucket;
  while (fullBucket != nullptr) {
      size_t bucketId = BucketIndex(HashKey(key));

      // LOG_INFO("Bucket is Full!");
      if(fullBucket->depth == globalDepth){
        doubleBuckets();
      }

      int curDepth = fullBucket->depth;
      int diffBit = 1 << curDepth;
      Bucket *b0 = nullptr;
      Bucket *b1 = nullptr;

      if((bucketId & diffBit) == 0){
        b0 = fullBucket;
        b1 = new Bucket();
        auto &vec = b0->entries;
        auto it = vec.begin();
        while(it!=vec.end()){
          auto h = this->HashKey(it->first);
          if((h & diffBit) != 0){
            b1->entries.push_back(*it);
            it = vec.erase(it);
          }else{
            it++;
          }
        }
      }else{
        b0 = new Bucket();
        b1 = fullBucket;
        auto &vec = b1->entries;
        auto it = vec.begin();
        while(it != vec.end()){
          auto h = this->HashKey(it->first);
          if((h & diffBit) == 0){
            b0->entries.push_back(*it);
            it = vec.erase(it);
          }else{
            it++;
          }
        }
      }
      b0->depth = b1->depth = curDepth + 1;

      this->bucketsLatch.lock();
      if((diffBit & bucketId) == 0){
          buckets[bucketId] = b0;
          buckets[(bucketId + diffBit) % numBuckets] = b1;
      }else{
          buckets[bucketId] = b1;
          buckets[(bucketId + diffBit) % numBuckets] = b0;
      }
      this->bucketsLatch.unlock();

      if(this->isFull(*b0)){
        fullBucket = b0;
      }else if(this->isFull(*b1)){
        fullBucket = b1;
      }else{
        fullBucket = nullptr;
      }
  }
}


/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
  size_t hashKey = this->HashKey(key);
  int bucketId = this->BucketIndex(hashKey);
  // LOG_INFO("# Insert at hash key = %d", bucketId);
  Bucket* bucket = this->buckets[bucketId];

  lock_guard<mutex> lck(bucket->latch);
  bucket->entries.push_back(std::make_pair(key, value));

  if(this->isFull(*bucket)){
    this->redist(key, bucket);
  }
}

template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace cmudb
