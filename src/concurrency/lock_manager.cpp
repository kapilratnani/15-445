/**
 * lock_manager.cpp
 */

#include "concurrency/lock_manager.h"
#include <assert.h>

namespace cmudb {
  bool isValidTxnState(Transaction* txn) {
    switch (txn->GetState()) {
    case TransactionState::ABORTED:
    case TransactionState::COMMITTED:
      return false;
    case TransactionState::SHRINKING:
      txn->SetState(TransactionState::ABORTED);
      return false;
    case TransactionState::GROWING:
    default:
      return true;
    }
  }

  bool LockManager::LockShared(Transaction* txn, const RID& rid) {
    if (!isValidTxnState(txn))
      return false;

    std::unique_lock<std::mutex> lck(mtx);

    auto t_id = txn->GetTransactionId();

    if (lock_map.find(rid) == lock_map.end()) {
      auto wl_ptr = std::make_shared<WaitList>(t_id, WaitState::SHARED);
      lock_map[rid] = wl_ptr;
      txn->GetSharedLockSet()->insert(rid);
      return true;
    }

    auto wl_ptr = lock_map[rid];
    if (wl_ptr->state == WaitState::EXCLUSIVE) {

      // wait-die rule
      if (t_id > wl_ptr->oldest&& wl_ptr->oldest != -1) {
        txn->SetState(TransactionState::ABORTED);
        return false;
      }

      wl_ptr->lst.emplace_back(t_id, WaitState::SHARED);
      auto promise = wl_ptr->lst.back().p;
      lck.unlock();
      // wait for promise to be fulfilled, which will be done in unlock call
      promise->get_future().get();
      txn->GetSharedLockSet()->insert(rid);
      return true;
    }

    wl_ptr->granted.insert(t_id);
    wl_ptr->oldest = std::max(t_id, wl_ptr->oldest);
    txn->GetSharedLockSet()->insert(rid);
    return true;
  }

  bool LockManager::LockExclusive(Transaction* txn, const RID& rid) {
    if (!isValidTxnState(txn))
      return false;

    std::unique_lock<std::mutex> lck(mtx);

    auto t_id = txn->GetTransactionId();

    if (lock_map.find(rid) == lock_map.end()) {
      auto wl_ptr = std::make_shared<WaitList>(t_id, WaitState::EXCLUSIVE);
      lock_map[rid] = wl_ptr;
      txn->GetExclusiveLockSet()->insert(rid);
      return true;
    }

    auto wl_ptr = lock_map[rid];

    // wait-die rule
    if (t_id > wl_ptr->oldest&& wl_ptr->oldest != -1) {
      txn->SetState(TransactionState::ABORTED);
      return false;
    }

    wl_ptr->lst.emplace_back(t_id, WaitState::EXCLUSIVE);
    auto promise = wl_ptr->lst.back().p;
    lck.unlock();
    // wait for promise to be fulfilled, which will be done in unlock call
    promise->get_future().get();
    txn->GetExclusiveLockSet()->insert(rid);
    return true;
  }

  bool LockManager::LockUpgrade(Transaction* txn, const RID& rid) {
    // should have a shared lock first
    if (!isValidTxnState(txn))
      return false;

    std::unique_lock<std::mutex> lck(mtx);
    auto t_id = txn->GetTransactionId();

    if (lock_map.find(rid) == lock_map.end()) {
      return false;
    }

    auto wl_ptr = lock_map[rid];
    if (wl_ptr->granted.find(t_id) == wl_ptr->granted.end()) {
      return false;
    }
    lck.unlock();
    auto unlock = Unlock(txn, rid, true);
    assert(unlock);
    auto lock_ex = LockExclusive(txn, rid);
    assert(lock_ex);

    return true;
  }

  bool LockManager::Unlock(Transaction* txn, const RID& rid, bool upgrading) {
    // if in strict mode then unlock can only be done while commiting or aborting

    std::unique_lock<std::mutex> lck(mtx);

    auto txn_state = txn->GetState();
    if (strict_2PL_) {
      if (!(txn_state == TransactionState::COMMITTED || txn_state == TransactionState::ABORTED))
        return false;
    }
    else if (txn_state == TransactionState::GROWING && !upgrading) {
      txn->SetState(TransactionState::SHRINKING);
    }

    auto t_id = txn->GetTransactionId();

    if (lock_map.find(rid) == lock_map.end())
      return false;

    auto wl_ptr = lock_map[rid];
    if (wl_ptr->granted.find(t_id) == wl_ptr->granted.end()) {
      return false;
    }

    wl_ptr->granted.erase(t_id);
    if (wl_ptr->lst.empty()) {
      lock_map.erase(rid);
      return true;
    }

    wl_ptr->oldest = -1;
    wl_ptr->state = WaitState::INIT;

    auto& next_txn = wl_ptr->lst.front();
    auto promise = next_txn.p;
    wl_ptr->granted.insert(next_txn.txn_id);
    wl_ptr->lst.erase(wl_ptr->lst.begin());
    if (!wl_ptr->lst.empty()) {
      wl_ptr->oldest = wl_ptr->lst.front().txn_id;
      wl_ptr->state = wl_ptr->lst.front().target_state;
    }
    promise->set_value(true);
    return true;
  }

  bool LockManager::Unlock(Transaction* txn, const RID& rid) {
    return Unlock(txn, rid, false);
  }

} // namespace cmudb
