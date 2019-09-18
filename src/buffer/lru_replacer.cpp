/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace cmudb {

template <typename T> LRUReplacer<T>::LRUReplacer() {
}

template <typename T> LRUReplacer<T>::~LRUReplacer() {}

/*
 * Insert value into LRU
 */
template <typename T> void LRUReplacer<T>::Insert(const T &value) {
	/*
		if not contained in map insert into front of linked list
		if contained in map remove and insert into front
	*/
	typename key_access_list::iterator it = access_list.end();
	if (map.Find(value, it)) {
		// contains
		if(it != access_list.begin())
			access_list.splice(access_list.begin(), access_list, it);
	}
	else {
		// doesn't contain
		it = access_list.insert(access_list.begin(), value);
		map.Insert(value, it);
	}
}

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template <typename T> bool LRUReplacer<T>::Victim(T &value) {
	if(access_list.size() == 0)
		return false;
	T& popped = access_list.back();
	access_list.pop_back();
	map.Remove(popped);
	value = popped;
	return true;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template <typename T> bool LRUReplacer<T>::Erase(const T &value) {
	typename key_access_list::iterator it = access_list.end();
	if (map.Find(value, it)) {
		map.Remove(value);
		access_list.erase(it);
		return true;
	}
	return false;
}

template <typename T> size_t LRUReplacer<T>::Size() { return access_list.size(); }

template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace cmudb
