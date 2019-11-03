/**
 * b_plus_tree.cpp
 */
#include <iostream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "index/b_plus_tree.h"
#include "page/header_page.h"

namespace cmudb {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(const std::string &name,
                          BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator,
                          page_id_t root_page_id)
    : index_name_(name),
      root_page_id_(root_page_id),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
  if (root_page_id_ == INVALID_PAGE_ID) return true;
  auto *page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage *btreePage = reinterpret_cast<BPlusTreePage *>(page);
  return btreePage->GetSize() == 0;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key,
                              std::vector<ValueType> &result,
                              Transaction *transaction) {
  if (IsEmpty()) return false;

  B_PLUS_TREE_LEAF_PAGE_TYPE *lPage = GetLeafPage(key, transaction);

  ValueType val;
  if (lPage->Lookup(key, val, comparator_)) {
    result.push_back(val);
    buffer_pool_manager_->UnpinPage(lPage->GetPageId(), false);
    return true;
  }
  buffer_pool_manager_->UnpinPage(lPage->GetPageId(), false);
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value,
                            Transaction *transaction) {
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value,
                                  Transaction *transaction) {
  auto *dPage = buffer_pool_manager_->NewPage(root_page_id_);
  if (dPage == nullptr) {
    throw std::bad_alloc();
  }
  // make a new root page of type leaf
  B_PLUS_TREE_LEAF_PAGE_TYPE *mPage =
      reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(dPage->GetData());
  // no parent for root
  mPage->Init(root_page_id_, INVALID_PAGE_ID);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
  // as it is a new root, we need to insert so flag is true
  UpdateRootPageId(true);
  // continue with inserting on the leaf
  InsertIntoLeaf(key, value, transaction);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value,
                                    Transaction *transaction) {
  // find a leaf page that can contain the current key
  B_PLUS_TREE_LEAF_PAGE_TYPE *leafPage = GetLeafPage(key, transaction);
  assert(leafPage != nullptr);

  ValueType v;
  if (leafPage->Lookup(key, v, comparator_)) {
    return false;
  }

  int sizeAfterInsert = leafPage->Insert(key, value, comparator_);
  if (sizeAfterInsert == leafPage->GetMaxSize()) {
    // split the leaf page and send the mid value up to the parent
    B_PLUS_TREE_LEAF_PAGE_TYPE *oldLeaf = leafPage;
    B_PLUS_TREE_LEAF_PAGE_TYPE *otherLeaf = Split(leafPage);

    otherLeaf->SetNextPageId(oldLeaf->GetNextPageId());

    oldLeaf->SetNextPageId(otherLeaf->GetPageId());

    InsertIntoParent(oldLeaf, oldLeaf->KeyAt(oldLeaf->GetSize() - 1), otherLeaf,
                     transaction);

    buffer_pool_manager_->UnpinPage(otherLeaf->GetPageId(), true);
  }
  buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), true);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t page_id;
  Page *newPage = buffer_pool_manager_->NewPage(page_id);
  if (newPage == nullptr) {
    throw std::bad_alloc();
  }

  typedef typename std::remove_pointer<N>::type *PagePtr;
  PagePtr ptr = reinterpret_cast<PagePtr>(newPage->GetData());
  ptr->Init(page_id, node->GetParentPageId());

  // this is different between leaf node and internal node.
  node->MoveHalfTo(ptr, buffer_pool_manager_);
  return ptr;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node,
                                      const KeyType &key,
                                      BPlusTreePage *new_node,
                                      Transaction *transaction) {
  // get parent from old node
  page_id_t parent_page_id = old_node->GetParentPageId();
  // if parent page is invalid, then the split was of the root page
  if (parent_page_id == INVALID_PAGE_ID) {
    // create new root page as an internal node and insert the key
    page_id_t new_page_id;
    Page *new_page = buffer_pool_manager_->NewPage(new_page_id);

    if (new_page == nullptr) throw std::bad_alloc();

    // root page should be an internal page now
    B_PLUS_TREE_INTERNAL_PAGE_TYPE *new_i_page =
        reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(new_page->GetData());

    new_i_page->Init(new_page_id, INVALID_PAGE_ID);
    old_node->SetParentPageId(new_page_id);
    new_node->SetParentPageId(new_page_id);
    new_i_page->PopulateNewRoot(old_node->GetPageId(), key,
                                new_node->GetPageId());
    root_page_id_ = new_page_id;
    UpdateRootPageId(false);
    buffer_pool_manager_->UnpinPage(new_page_id, true);
    return;
  }
  // split was of an internal page

  // get the parent page
  Page *parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
  B_PLUS_TREE_INTERNAL_PAGE_TYPE *parent_i_page =
      reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(parent_page);

  // insert the key in the parent page and the value should be the page_id of
  // new node
  parent_i_page->InsertNodeAfter(old_node->GetPageId(), key,
                                 new_node->GetPageId());

  // after insert check if split is needed
  if (parent_i_page->GetSize() == parent_i_page->GetMaxSize()) {
    // perform split
    B_PLUS_TREE_INTERNAL_PAGE_TYPE *oldPage = parent_i_page;
    B_PLUS_TREE_INTERNAL_PAGE_TYPE *otherPage = Split(oldPage);

    InsertIntoParent(oldPage, oldPage->KeyAt(oldPage->GetSize() - 1), otherPage,
                     transaction);

    buffer_pool_manager_->UnpinPage(otherPage->GetPageId(), true);
  }
  buffer_pool_manager_->UnpinPage(parent_i_page->GetPageId(), true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) return;
  // find leaf page which contains the key
  B_PLUS_TREE_LEAF_PAGE_TYPE *target_leaf = GetLeafPage(key, transaction);

  int size_before = target_leaf->GetSize();
  int size_after = target_leaf->RemoveAndDeleteRecord(key, comparator_);

  if (size_after == size_before) return;

  bool shouldRemovePage = false;
  if (size_after < target_leaf->GetMinSize()) {
    shouldRemovePage = CoalesceOrRedistribute(target_leaf, transaction);
  }

  if (shouldRemovePage) {
    page_id_t target_page_id = target_leaf->GetPageId();
    buffer_pool_manager_->UnpinPage(target_page_id, true);
    assert(buffer_pool_manager_->DeletePage(target_leaf->GetPageId()));
    if (target_page_id == root_page_id_) UpdateRootPageId(false);
  }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  if (node->GetSize() >= node->GetMinSize()) return false;

  // find a sibling node
  // first get parent
  page_id_t parent_page_id = node->GetParentPageId();
  // if the current page is root page
  if (parent_page_id == INVALID_PAGE_ID) {
    return AdjustRoot(node);
  }
  // get parent page
  auto *parent_page =
      reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(
          buffer_pool_manager_->FetchPage(parent_page_id));
  
  // index in parent where page_id exists
  int idx = parent_page->ValueIndex(node->GetPageId());
  assert(idx >= 0);

  // redistribute takes precedence
  // try left sibling
  if (idx - 1 >= 0) {
    auto left_page_id = parent_page->ValueAt(idx - 1);
    auto left_page = reinterpret_cast<decltype(node)>(GetPage(left_page_id));
    if (left_page->GetSize() > left_page->GetMinSize()) {
      // redistribute
      Redistribute(left_page, node, 1);
      return false;
    }
  }

  // try right sibling
  if (idx + 1 < parent_page->GetSize()) {
    auto right_page_id = parent_page->ValueAt(idx + 1);
    auto right_page = reinterpret_cast<decltype(node)>(GetPage(right_page_id));
    if (right_page->GetSize() > node->GetMinSize()) {
      Redistribute(right_page, node, 0);
      return false;
    }
  }

  // now try merging
  bool node_delete = false;
  if (idx - 1 >= 0) {
    auto left_page_id = parent_page->ValueAt(idx - 1);
    auto left_page = reinterpret_cast<decltype(node)>(GetPage(left_page_id));
    if (left_page->GetSize() + node->GetSize() < node->GetMaxSize()) {
      Coalesce(left_page, node, parent_page, 1, transaction);
      buffer_pool_manager_->UnpinPage(left_page_id, true);
      node_delete = true;
    }
  }

  if (!node_delete && idx + 1 < parent_page->GetSize()) {
    auto right_page_id = parent_page->ValueAt(idx + 1);
    auto right_page = reinterpret_cast<decltype(node)>(GetPage(right_page_id));
    if (right_page->GetSize() + node->GetSize() < node->GetMaxSize()) {
      Coalesce(right_page, node, parent_page, 0, transaction);
    }
  }

  auto parent_del = CoalesceOrRedistribute(parent_page, transaction);
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);

  if (parent_del) {
    assert(buffer_pool_manager_->DeletePage(parent_page->GetPageId()));
  }

  return node_delete;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(
    N *&neighbor_node, N *&node,
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent,
    int index, Transaction *transaction) {
  assert(index == 0 || index == 1);
  if (index == 1) {
    // merging with left sibling
    // move all keys of node to neighbor node
    node->MoveAllTo(neighbor_node, 0, buffer_pool_manager_);
    // remove node's page_id from parent
    int index_in_parent = parent->ValueIndex(node->GetPageId());
    assert(index_in_parent >= 0);
    parent->Remove(index_in_parent);
    return true;
  } else {
    // merging with right sibling
    // move all keys from right sibling to node
    neighbor_node->MoveAllTo(node, 0, buffer_pool_manager_);
    int index_in_parent = parent->ValueIndex(neighbor_node->GetPageId());
    assert(index_in_parent >= 0);
    // remove right siblings page_id from parent
    parent->Remove(index_in_parent);
    // delete right sibling node
    buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
    assert(buffer_pool_manager_->DeletePage(neighbor_node->GetPageId()));
    return false;  // don't delete current node
  }
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  assert(index == 0 || index == 1);
  if (index == 0) {
    // neighbor node is right sibling
    neighbor_node->MoveFirstToEndOf(node, buffer_pool_manager_);
  } else {
    neighbor_node->MoveLastToFrontOf(node, 0, buffer_pool_manager_);
  }
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) { return false; }

INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::GetLeafPage(
    const KeyType &key, Transaction *transaction) {
  // start binary search from leaf
  BPlusTreePage *mPage = GetPage(root_page_id_);

  // while a leaf is not found
  while (!mPage->IsLeafPage()) {
    // since current page is not a leaf page, cast it to internal page

    BPLUSTREE_INTERNAL_TYPE *miPage =
        reinterpret_cast<BPLUSTREE_INTERNAL_TYPE *>(mPage);
    // find the next page where key can be inserted
    page_id_t nextPageId = miPage->Lookup(key, comparator_);
    buffer_pool_manager_->UnpinPage(mPage->GetPageId(), false);
    mPage = GetPage(nextPageId);
  }

  B_PLUS_TREE_LEAF_PAGE_TYPE *lPage =
      reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(mPage);
  return lPage;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  return INDEXITERATOR_TYPE();
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key,
                                                         bool leftMost) {
  return nullptr;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(
      buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record)
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  else
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for debug only
 * print out whole b+tree sturcture, rank by rank
 */
INDEX_TEMPLATE_ARGUMENTS
std::string BPLUSTREE_TYPE::ToString(bool verbose) { return "Empty tree"; }

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace cmudb
