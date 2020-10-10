/**
 * b_plus_tree.h
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
#pragma once

#include <queue>
#include <vector>

#include "concurrency/transaction.h"
#include "index/index_iterator.h"
#include "page/b_plus_tree_internal_page.h"
#include "page/b_plus_tree_leaf_page.h"

namespace cmudb {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

  // Main class providing the API for the Interactive B+ Tree.
  INDEX_TEMPLATE_ARGUMENTS
    class BPlusTree {
    public:
      explicit BPlusTree(const std::string& name,
        BufferPoolManager* buffer_pool_manager,
        const KeyComparator& comparator,
        page_id_t root_page_id = INVALID_PAGE_ID);

      // Returns true if this B+ tree has no keys and values.
      bool IsEmpty() const;

      // Insert a key-value pair into this B+ tree.
      bool Insert(const KeyType& key, const ValueType& value,
        Transaction* transaction = nullptr);

      // Remove a key and its value from this B+ tree.
      void Remove(const KeyType& key, Transaction* transaction = nullptr);

      // return the value associated with a given key
      bool GetValue(const KeyType& key, std::vector<ValueType>& result,
        Transaction* transaction = nullptr);

      // index iterator
      INDEXITERATOR_TYPE Begin();
      INDEXITERATOR_TYPE Begin(const KeyType& key);

      // Print this B+ tree to stdout using a simple command-line
      std::string ToString(bool verbose = false);

      // read data from file and insert one by one
      void InsertFromFile(const std::string& file_name,
        Transaction* transaction = nullptr);

      // read data from file and remove one by one
      void RemoveFromFile(const std::string& file_name,
        Transaction* transaction = nullptr);
      // expose for test purpose
      B_PLUS_TREE_LEAF_PAGE_TYPE* FindLeafPage(const KeyType& key,
        bool leftMost = false);

    private:
      void StartNewTree(const KeyType& key, const ValueType& value, Transaction* txn);

      bool InsertIntoLeaf(const KeyType& key, const ValueType& value,
        Transaction* transaction = nullptr);

      void InsertIntoParent(BPlusTreePage* old_node, const KeyType& key,
        BPlusTreePage* new_node,
        Transaction* transaction = nullptr);

      template <typename N> N* Split(N* node);

      template <typename N>
      bool CoalesceOrRedistribute(N* node, std::unordered_set<page_id_t> &to_delete, Transaction* transaction = nullptr);

      template <typename N>
      bool Coalesce(
        N*& neighbor_node, N*& node,
        BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*& parent,
        int index, Transaction* transaction = nullptr);

      template <typename N> void Redistribute(N* neighbor_node, N* node, int index);

      bool AdjustRoot(BPlusTreePage* node);

      void UpdateRootPageId(int insert_record = false);

      using BPInternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;

      BPlusTreePage* GetPage(page_id_t page_id, bool read_only, Transaction* transaction = nullptr) {
        assert(page_id != INVALID_PAGE_ID);
        auto* dPage = buffer_pool_manager_->FetchPage(page_id);

        if (read_only) {
          dPage->RLatch();
        }
        else {
          dPage->WLatch();
          if(transaction)
            transaction->AddIntoPageSet(dPage);
        }
        return reinterpret_cast<BPlusTreePage*>(dPage->GetData());
      }

      void ReleasePage(page_id_t page_id, bool read_only, bool dirty, Transaction* transaction = nullptr) {
        assert(page_id != INVALID_PAGE_ID);
        auto* dPage = buffer_pool_manager_->FetchPage(page_id);
        if (read_only) {
          dPage->RUnlatch();
        }
        else {
          dPage->WUnlatch();
        }
        buffer_pool_manager_->UnpinPage(page_id, dirty);
        buffer_pool_manager_->UnpinPage(page_id, dirty);
      }

      B_PLUS_TREE_LEAF_PAGE_TYPE* GetLeafPage(const KeyType& key, bool read_only,
        Transaction* transaction = nullptr);

      B_PLUS_TREE_LEAF_PAGE_TYPE* UpgradeToExclusive(page_id_t page_id, Transaction* txn) {
        assert(page_id != INVALID_PAGE_ID);
        ReleasePage(page_id, true, false, txn);
        auto* page = GetPage(page_id, false, txn);
        return reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(page);
      }

      void ReleaseAllTxnPages(Transaction* txn) {
        assert(txn);
        auto page_set = txn->GetPageSet();
        while (!page_set->empty())
        {
          auto page = page_set->front();
          auto page_id = page->GetPageId();
          page->WUnlatch();
          buffer_pool_manager_->UnpinPage(page_id, true);
          page_set->pop_front();
        }

        auto del_page_set = txn->GetDeletedPageSet();
        auto it = del_page_set->begin();
        while (it != del_page_set->end())
        {
          auto page_id = *it;
          auto* dPage = buffer_pool_manager_->FetchPage(page_id);
          dPage->WUnlatch();
          buffer_pool_manager_->UnpinPage(page_id, false);
          buffer_pool_manager_->DeletePage(page_id);
          it++;
        }
        del_page_set->clear();
      }

      // member variable
      std::string index_name_;
      page_id_t root_page_id_;
      // lock to protect internal b+-tree data
      std::mutex int_mtx;
      BufferPoolManager* buffer_pool_manager_;
      KeyComparator comparator_;
  };

} // namespace cmudb
