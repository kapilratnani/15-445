/**
 * b_plus_tree_leaf_page.cpp
 */

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "page/b_plus_tree_leaf_page.h"
#include "page/b_plus_tree_internal_page.h"

namespace cmudb {

  /*****************************************************************************
   * HELPER METHODS AND UTILITIES
   *****************************************************************************/

   /**
    * Init method after creating a new leaf page
    * Including set page type, set current size to zero, set page id/parent id, set
    * next page id and set max size
    */
  INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id) {
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetPageType(IndexPageType::LEAF_PAGE);
    SetSize(0);
    int size = (PAGE_SIZE - sizeof(BPlusTreeLeafPage)) / sizeof(MappingType) - 1;
    // bring it to nearest even size
    size &= ~(1);
    //
    SetMaxSize(size);
    SetNextPageId(INVALID_PAGE_ID);
  }

  /**
   * Helper methods to set/get next page id
   */
  INDEX_TEMPLATE_ARGUMENTS
    page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const {
    return next_page_id_;
  }

  INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
    this->next_page_id_ = next_page_id;
  }

  /**
   * Helper method to find the first index i so that array[i].first >= key
   * NOTE: This method is only used when generating index iterator
   */
  INDEX_TEMPLATE_ARGUMENTS
    int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(
      const KeyType& key, const KeyComparator& comparator) const {
    int low = 0, high = GetSize() - 1;
    while (low <= high) {
      int mid = low + (high - low) / 2;
      int cmp = comparator(array[mid].first, key);
      if (cmp == 0)
        return mid;
      else if (cmp == -1)
        low = mid + 1;
      else
        high = mid - 1;
    }

    return low;
  }

  /*
   * Helper method to find and return the key associated with input "index"(a.k.a
   * array offset)
   */
  INDEX_TEMPLATE_ARGUMENTS
    KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
    VALID_IDX(index, 0, GetSize());
    KeyType key = this->array[index].first;
    return key;
  }

  /*
   * Helper method to find and return the key & value pair associated with input
   * "index"(a.k.a array offset)
   */
  INDEX_TEMPLATE_ARGUMENTS
    const MappingType& B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
    VALID_IDX(index, 0, GetSize());
    return array[index];
  }

  /*****************************************************************************
   * INSERTION
   *****************************************************************************/
   /*
    * Insert key & value pair into leaf page ordered by key
    * @return  page size after insertion
    */
  INDEX_TEMPLATE_ARGUMENTS
    int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType& key,
      const ValueType& value,
      const KeyComparator& comparator) {
    assert(GetSize() < GetMaxSize());

    // if this is the first value
    if (GetSize() == 0) {
      this->array[0] = std::make_pair(key, value);
      IncreaseSize(1);
      return GetSize();
    }

    // find insert location
    int idx = this->KeyIndex(key, comparator);

    // if found key is equal then replace
    if (idx < GetSize() && comparator(this->KeyAt(idx), key) == 0) {
      this->array[idx].second = value;
      return GetSize();
    }

    // make space for the new key
    for (int i = GetSize() + 1; i > idx; i--) {
      this->array[i] = this->array[i - 1];
    }
    this->array[idx].first = key;
    this->array[idx].second = value;
    IncreaseSize(1);
    return GetSize();
  }

  /*****************************************************************************
   * SPLIT
   *****************************************************************************/
   /*
    * Remove half of key & value pairs from this page to "recipient" page
    */
  INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(
      BPlusTreeLeafPage* recipient,
      __attribute__((unused)) BufferPoolManager* buffer_pool_manager) {
    assert(recipient != nullptr);

    int start = GetSize() / 2;
    int targetIdx = recipient->GetSize();
    for (int i = start; i < GetSize(); i++) {
      recipient->array[targetIdx].first = this->array[i].first;
      recipient->array[targetIdx].second = this->array[i].second;
      targetIdx++;
    }

    recipient->IncreaseSize(GetSize() - start);
    this->SetSize(start);
  }

  INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyHalfFrom(MappingType* items, int size) {}

  /*****************************************************************************
   * LOOKUP
   *****************************************************************************/
   /*
    * For the given key, check to see whether it exists in the leaf page. If it
    * does, then store its corresponding value in input "value" and return true.
    * If the key does not exist, then return false
    */
  INDEX_TEMPLATE_ARGUMENTS
    bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType& key, ValueType& value,
      const KeyComparator& comparator) const {
    if (GetSize() == 0) return false;
    int idx = KeyIndex(key, comparator);
    if (comparator(this->array[idx].first, key) == 0) {
      value = array[idx].second;
      return true;
    }
    return false;
  }

  /*****************************************************************************
   * REMOVE
   *****************************************************************************/
   /*
    * First look through leaf page to see whether delete key exist or not. If
    * exist, perform deletion, otherwise return immdiately.
    * NOTE: store key&value pair continuously after deletion
    * @return   page size after deletion
    */
  INDEX_TEMPLATE_ARGUMENTS
    int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(
      const KeyType& key, const KeyComparator& comparator) {
    if (GetSize() == 0) return 0;

    int idx = KeyIndex(key, comparator);
    if (idx < GetSize() && comparator(this->array[idx].first, key) == 0) {
      for (int i = idx; i < GetSize() - 1; i++) {
        this->array[i] = this->array[i + 1];
      }
      IncreaseSize(-1);
    }
    return GetSize();
  }

  /*****************************************************************************
   * MERGE
   *****************************************************************************/
   /*
    * Remove all of key & value pairs from this page to "recipient" page, then
    * update next page id
    */
  INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage* recipient, int,
      BufferPoolManager*) {
    assert(recipient != nullptr);
    int r_idx = recipient->GetSize();
    for (int i = 0; i < GetSize(); i++) {
      recipient->array[r_idx] = this->array[i];
      r_idx++;
    }
    recipient->IncreaseSize(GetSize());
    recipient->SetNextPageId(GetNextPageId());
  }

  INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyAllFrom(MappingType* items, int size) {}

  /*****************************************************************************
   * REDISTRIBUTE
   *****************************************************************************/
   /*
    * Remove the first key & value pair from this page to "recipient" page, then
    * update relavent key & value pair in its parent page.
    */
  INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(
      BPlusTreeLeafPage* recipient, BufferPoolManager* buffer_pool_manager) {
    // left rotation
    assert(recipient != nullptr);
    assert(recipient->GetParentPageId() == this->GetParentPageId());
    assert(recipient->GetParentPageId() != INVALID_PAGE_ID);

    // get parent page
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* parent_page =
      reinterpret_cast<
      BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(
        buffer_pool_manager->FetchPage(GetParentPageId()));
    // get parent index
    int index_in_parent = parent_page->ValueIndex(this->GetPageId());

    int r_size = recipient->GetSize();
    recipient->array[r_size].first = array[0].first;
    recipient->array[r_size].second = array[0].second;
    recipient->IncreaseSize(1);

    // shift array
    for (int i = 0; i < GetSize() - 1; i++) {
      this->array[i].first = this->array[i + 1].first;
      this->array[i].second = this->array[i + 1].second;
    }
    IncreaseSize(-1);

    parent_page->SetKeyAt(index_in_parent, this->array[0].first);
    buffer_pool_manager->UnpinPage(GetParentPageId(), true);
  }


  INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType& item) {}
  /*
   * Remove the last key & value pair from this page to "recipient" page, then
   * update relavent key & value pair in its parent page.
   */
  INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(
      BPlusTreeLeafPage* recipient, int parentIndex,
      BufferPoolManager* buffer_pool_manager) {
    // right rotation
    assert(recipient != nullptr);
    assert(recipient->GetParentPageId() == this->GetParentPageId());
    assert(recipient->GetParentPageId() != INVALID_PAGE_ID);

    // get parent page
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* parent_page =
      reinterpret_cast<
      BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(
        buffer_pool_manager->FetchPage(GetParentPageId()));

    // get parent index
    int index_in_parent = parent_page->ValueIndex(recipient->GetPageId());

    // make space in recipient
    for (int i = recipient->GetSize(); i > 0; i--) {
      recipient->array[i].first = recipient->array[i - 1].first;
      recipient->array[i].second = recipient->array[i - 1].second;
    }
    // move last key to front
    recipient->array[0].first = this->array[GetSize() - 1].first;
    recipient->array[0].second = this->array[GetSize() - 1].second;
    // update size
    recipient->IncreaseSize(1);
    this->IncreaseSize(-1);
    // set parent key
    parent_page->SetKeyAt(index_in_parent, array[0].first);
    buffer_pool_manager->UnpinPage(GetParentPageId(), true);
  }

  INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(
      const MappingType& item, int parentIndex,
      BufferPoolManager* buffer_pool_manager) {}

  /*****************************************************************************
   * DEBUG
   *****************************************************************************/
  INDEX_TEMPLATE_ARGUMENTS
    std::string B_PLUS_TREE_LEAF_PAGE_TYPE::ToString(bool verbose) const {
    if (GetSize() == 0) {
      return "";
    }
    std::ostringstream stream;
    if (verbose) {
      stream << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
        << "]<" << GetSize() << "> ";
    }
    int entry = 0;
    int end = GetSize();
    bool first = true;

    while (entry < end) {
      if (first) {
        first = false;
      }
      else {
        stream << " ";
      }
      stream << std::dec << array[entry].first;
      if (verbose) {
        stream << "(" << array[entry].second << ")";
      }
      ++entry;
    }
    return stream.str();
  }

  template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
  template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
  template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
  template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
  template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace cmudb
