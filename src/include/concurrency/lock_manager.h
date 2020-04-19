/**
 * lock_manager.h
 *
 * Tuple level lock manager, use wait-die to prevent deadlocks
 */

#pragma once

#include <condition_variable>
#include <future>
#include <list>
#include <set>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "common/rid.h"
#include "concurrency/transaction.h"

namespace cmudb {

  enum class WaitState
  {
    INIT,
    SHARED,
    EXCLUSIVE
  };

  class WaitList
  {
  public:
    WaitList(const WaitList&) = delete;
    WaitList& operator=(const WaitList&) = delete;
    
    WaitList(txn_id_t t_id, WaitState state) : oldest(t_id), state(state) {
      granted.insert(t_id);
    }

    class WaitItem
    {
    public:
      WaitItem(const WaitItem&) = delete;
      WaitItem& operator=(const WaitItem&) = delete;
      WaitItem(txn_id_t txn, WaitState t_state) : txn_id(txn), target_state(t_state) {}
      txn_id_t txn_id;
      WaitState target_state;
      std::shared_ptr<std::promise<bool>> p = std::make_shared<std::promise<bool>>();
    };
    
    std::list<WaitItem> lst;
    txn_id_t oldest = -1;
    WaitState state = WaitState::INIT;
    std::set<txn_id_t> granted;
  };

  class LockManager
  {
  public:
    LockManager(bool strict_2PL)
      : strict_2PL_(strict_2PL) {};

    /*** below are APIs need to implement ***/
    // lock:
    // return false if transaction is aborted
    // it should be blocked on waiting and should return true when granted
    // note the behavior of trying to lock locked rids by same txn is undefined
    // it is transaction's job to keep track of its current locks
    bool LockShared(Transaction* txn, const RID& rid);
    bool LockExclusive(Transaction* txn, const RID& rid);
    bool LockUpgrade(Transaction* txn, const RID& rid);

    // unlock:
    // release the lock hold by the txn
    bool Unlock(Transaction* txn, const RID& rid);
    /*** END OF APIs ***/

  private:
    bool strict_2PL_;
    std::mutex mtx;
    std::unordered_map<RID, std::shared_ptr<WaitList>> lock_map;
    bool Unlock(Transaction* txn, const RID& rid, bool upgrading);
  };

} // namespace cmudb
