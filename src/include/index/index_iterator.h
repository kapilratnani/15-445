/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "page/b_plus_tree_leaf_page.h"

namespace cmudb {

#define INDEXITERATOR_TYPE                                                     \
  IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
public:
  // you may define your own constructor based on your member variables
  IndexIterator(page_id_t start_page, int start_idx,
    BufferPoolManager &buf_manager):buf_manager(buf_manager) {
    cur_page_id = start_page;
    cur_idx = start_idx;
    cur_leaf_page = GetLeafPage(cur_page_id);
    no_more_records = false;
 }

  ~IndexIterator();

  bool isEnd() { return no_more_records; }

  const MappingType& operator*() {
    assert(!isEnd());
    const MappingType &ret = cur_leaf_page->GetItem(cur_idx);
    return ret;
  }

  IndexIterator& operator++() { 
    assert(cur_leaf_page != nullptr);
    cur_idx++;
    if (cur_idx >= cur_leaf_page->GetSize()) {
      page_id_t next_page_id = cur_leaf_page->GetNextPageId();
      buf_manager.UnpinPage(cur_page_id, false);
      if (next_page_id == INVALID_PAGE_ID) {
        no_more_records = true;
      } else {
        cur_leaf_page = GetLeafPage(next_page_id);
        cur_page_id = next_page_id;
        cur_idx = 0;
      }
    }
    return *this;
  }

private:
  // add your own private member variables here
 BufferPoolManager &buf_manager;
 int idx_in_page;
 int cur_idx;
 page_id_t cur_page_id;
 B_PLUS_TREE_LEAF_PAGE_TYPE *cur_leaf_page;
 bool no_more_records;

  B_PLUS_TREE_LEAF_PAGE_TYPE *GetLeafPage(page_id_t page_id) {
   if (page_id == INVALID_PAGE_ID) {
     return nullptr;
   }
   return reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(
       buf_manager.FetchPage(page_id)->GetData());
 }
};

} // namespace cmudb
