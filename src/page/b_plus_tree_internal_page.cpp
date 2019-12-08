/**
 * b_plus_tree_internal_page.cpp
 */
#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "page/b_plus_tree_internal_page.h"

namespace cmudb {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id,
                                          page_id_t parent_id) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetPageType(IndexPageType::INTERNAL_PAGE);
  // page size - header
  int size =
      (PAGE_SIZE - sizeof(BPlusTreeInternalPage)) / sizeof(MappingType) - 1;
  // bring it to nearest even size
  size &= ~(1);
  //
  SetMaxSize(size);
  SetSize(0);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  VALID_IDX(index, 0, GetSize());
  return this->array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  VALID_IDX(index, 0, GetSize());
  this->array[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  for (int i = 0; i < GetSize(); i++) {
    if (array[i].second == value) return i;
  }
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  VALID_IDX(index, 0, GetSize());
  return array[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(
    const KeyType &key, const KeyComparator &comparator) const {
  int low = 1, high = GetSize() - 1;
  while (low <= high) {
    int mid = low + (high - low) / 2;
    int cmp = comparator(array[mid].first, key);
    if (cmp == 0)
      return array[mid].second;
    else if (cmp == -1)
      low = mid + 1;
    else
      high = mid - 1;
  }

  if (low >= 1 && low < GetSize() && comparator(array[low].first, key) < 0)
    return array[low].second;

  return array[high].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) {
  array[0].second = old_value;
  array[1].first = new_key;
  array[1].second = new_value;
  IncreaseSize(2);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) {
  assert(GetSize() < GetMaxSize());
  int idx = ValueIndex(old_value) + 1;
  for (int i = GetSize(); i > idx; i--) {
    array[i] = array[i - 1];
  }
  array[idx].first = new_key;
  array[idx].second = new_value;
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
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(
    BPlusTreeInternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
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

  auto newParentPageId = recipient->GetPageId();
  // set parent page id of the transferred pages
  for (int i = 0; i < recipient->GetSize(); i++) {
    page_id_t page_id = recipient->ValueAt(i);
    auto *page = buffer_pool_manager->FetchPage(page_id);
    assert(page);
    BPlusTreePage *btreePage = reinterpret_cast<BPlusTreePage *>(page);
    btreePage->SetParentPageId(newParentPageId);
    buffer_pool_manager->UnpinPage(page_id, true);
  }

}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyHalfFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  assert(items != nullptr);
  assert((GetSize() + size) < GetMaxSize());
  int start = GetSize();
  auto newParentPageId = this->GetPageId();
  for (int i = 0; i < size; i++) {
    array[start].first = items[i].first;
    array[start].second = items[i].second;
    page_id_t page_id = items[i].second;
    auto *page = buffer_pool_manager->FetchPage(page_id);
    assert(page);
    BPlusTreePage *btreePage = reinterpret_cast<BPlusTreePage *>(page);
    btreePage->SetParentPageId(newParentPageId);
    buffer_pool_manager->UnpinPage(page_id, true);
  }
  IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  if (index == GetSize() - 1) {
    IncreaseSize(-1);
    return;
  }

  for (int i = index; i < GetSize() - 1; i++) {
    array[i] = array[i + 1];
  }

  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  IncreaseSize(-1);
  return array[0].second;
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(
    BPlusTreeInternalPage *recipient, int index_in_parent,
    BufferPoolManager *buffer_pool_manager) {
  assert(recipient != nullptr);
  int r_idx = recipient->GetSize();
  for (int i = 0; i < GetSize(); i++) {
    recipient->array[r_idx] = this->array[i];
    r_idx++;
  }
  recipient->IncreaseSize(GetSize());

}

INDEX_TEMPLATE_ARGUMENTS void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyAllFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeInternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
  // this is left rotation
  assert(recipient != nullptr);
  // make sure the nodes are siblings
  assert(recipient->GetParentPageId() == this->GetParentPageId());
  assert(recipient->GetParentPageId() != INVALID_PAGE_ID);

  // get parent page
  B_PLUS_TREE_INTERNAL_PAGE_TYPE *parent_page =
      reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(
          buffer_pool_manager->FetchPage(this->GetParentPageId()));
  int index_in_parent = parent_page->ValueIndex(this->GetPageId());
  assert(index_in_parent >= 0);
  KeyType key = parent_page->KeyAt(index_in_parent);

  // move first page id to last of recipient
  int r_size = recipient->GetSize();
  // key will be copied from parent
  recipient->array[r_size].first = key;
  recipient->array[r_size].second = array[0].second;
  recipient->IncreaseSize(1);

  this->Remove(0);
  // set key at parent
  parent_page->SetKeyAt(index_in_parent, this->KeyAt(0));
  buffer_pool_manager->UnpinPage(parent_page->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(
    const MappingType &pair, BufferPoolManager *buffer_pool_manager) {}

/*
 * Remove the last key & value pair from this page to head of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeInternalPage *recipient, int parent_index,
    BufferPoolManager *buffer_pool_manager) {
  // this is right rotation
  assert(recipient != nullptr);
  assert(recipient->GetParentPageId() == this->GetParentPageId());
  assert(recipient->GetParentPageId() != INVALID_PAGE_ID);

  // get parent page
  B_PLUS_TREE_INTERNAL_PAGE_TYPE *parent_page =
      reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(
          buffer_pool_manager->FetchPage(this->GetParentPageId()));

  int index_in_parent = parent_page->ValueIndex(this->GetPageId());
  assert(index_in_parent >= 0);
  KeyType current_parent_key = parent_page->KeyAt(index_in_parent);

  // shift recipient array by 1, to make space for new keys
  for (int i = recipient->GetSize(); i > 0; i--) {
    recipient->array[i] = recipient->array[i - 1];
  }
  // put the current parent key
  recipient->array[1].first = current_parent_key;
  // move the last index's value
  recipient->array[0].second = this->ValueAt(GetSize() - 1);
  // put last index's key in parent
  parent_page->SetKeyAt(index_in_parent, this->KeyAt(GetSize() - 1));
  // remove from current node
  this->Remove(GetSize() - 1);
  buffer_pool_manager->UnpinPage(parent_page->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(
    const MappingType &pair, int parent_index,
    BufferPoolManager *buffer_pool_manager) {}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::QueueUpChildren(
    std::queue<BPlusTreePage *> *queue,
    BufferPoolManager *buffer_pool_manager) {
  for (int i = 0; i < GetSize(); i++) {
    auto *page = buffer_pool_manager->FetchPage(array[i].second);
    if (page == nullptr)
      throw Exception(EXCEPTION_TYPE_INDEX,
                      "all page are pinned while printing");
    BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    queue->push(node);
  }
}

INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_INTERNAL_PAGE_TYPE::ToString(bool verbose) const {
  if (GetSize() == 0) {
    return "";
  }
  std::ostringstream os;
  if (verbose) {
    os << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
       << "]<" << GetSize() << "> ";
  }

  int entry = verbose ? 0 : 1;
  int end = GetSize();
  bool first = true;
  while (entry < end) {
    if (first) {
      first = false;
    } else {
      os << " ";
    }
    os << std::dec << array[entry].first.ToString();
    if (verbose) {
      os << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return os.str();
}

// valuetype for internalNode should be page id_t

template class BPlusTreeInternalPage<GenericKey<4>, page_id_t,
                                     GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t,
                                     GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t,
                                     GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t,
                                     GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t,
                                     GenericComparator<64>>;
}  // namespace cmudb
