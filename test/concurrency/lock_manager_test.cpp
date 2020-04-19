/**
 * lock_manager_test.cpp
 */

#include <thread>

#include "concurrency/transaction_manager.h"
#include "gtest/gtest.h"

namespace cmudb {


  /*
   * This test is only a sanity check. Please do not rely on this test
   * to check the correctness.
   */
  TEST(LockManagerTest, BasicTest) {
    LockManager lock_mgr{ false };
    TransactionManager txn_mgr{ &lock_mgr };
    RID rid{ 0, 0 };
    // shared locks per thread
    std::thread t0([&] {
      Transaction txn(0);
      bool res = lock_mgr.LockShared(&txn, rid);
      EXPECT_EQ(res, true);
      EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
      txn_mgr.Commit(&txn);
      EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
    });

    std::thread t1([&] {
      Transaction txn(1);
      bool res = lock_mgr.LockShared(&txn, rid);
      EXPECT_EQ(res, true);
      EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
      txn_mgr.Commit(&txn);
      EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
    });

    t0.join();
    t1.join();
  }

  TEST(LockManagerTest, Basic2Test) {
    LockManager lock_mgr{ false };
    TransactionManager txn_mgr{ &lock_mgr };
    RID rid{ 0, 0 };
    // exlusive lock and a shared lock
    std::thread t0([&] {
      Transaction txn(1);
      bool res = lock_mgr.LockExclusive(&txn, rid);
      EXPECT_EQ(res, true);
      EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
      std::this_thread::sleep_for(std::chrono::seconds(2));
      txn_mgr.Commit(&txn);
      EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
    });


    std::thread t1([&] {
      Transaction txn(0);
      bool res = lock_mgr.LockShared(&txn, rid);
      EXPECT_EQ(res, true);
      EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
      txn_mgr.Commit(&txn);
      EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
    });

    t0.join();
    t1.join();
  }

  TEST(LockManagerTest, Basic3Test) {
    LockManager lock_mgr{ true };
    TransactionManager txn_mgr{ &lock_mgr };
    RID rid{ 0, 0 };
    // shared followed by exclusive
    // wait-die "old waits for the young"
    // txn id in first thread is lower
    std::thread t0([&] {
      Transaction txn(1);
      bool res = lock_mgr.LockShared(&txn, rid);
      EXPECT_EQ(res, true);
      EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
      std::this_thread::sleep_for(std::chrono::seconds(2));
      txn_mgr.Commit(&txn);
      EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
    });


    std::thread t1([&] {
      Transaction txn(0);
      bool res = lock_mgr.LockExclusive(&txn, rid);
      EXPECT_EQ(res, true);
      EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
      txn_mgr.Commit(&txn);
      EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
    });

    t0.join();
    t1.join();
  }

  TEST(LockManagerTest, Basic4Test) {
    LockManager lock_mgr{ false };
    TransactionManager txn_mgr{ &lock_mgr };
    RID rid{ 0, 0 };
    // lock upgrade and exclusive lock
    std::thread t0([&] {
      Transaction txn(1);
      bool res = lock_mgr.LockShared(&txn, rid);
      EXPECT_EQ(res, true);
      EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
      std::this_thread::sleep_for(std::chrono::seconds(2));
      res = lock_mgr.LockUpgrade(&txn, rid);
      EXPECT_EQ(res, true);
      std::this_thread::sleep_for(std::chrono::seconds(2));
      txn_mgr.Commit(&txn);
      EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
    });


    std::thread t1([&] {
      Transaction txn(0);
      bool res = lock_mgr.LockExclusive(&txn, rid);
      EXPECT_EQ(res, true);
      EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
      txn_mgr.Commit(&txn);
      EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
    });

    t0.join();
    t1.join();
  }

  TEST(LockManagerTest, BasicS2PLTest) {
    LockManager lock_mgr{ true };
    TransactionManager txn_mgr{ &lock_mgr };
    RID rid1{ 0, 0 };
    RID rid2{ 0, 1 };

    Transaction txn(1);
    bool res = lock_mgr.LockShared(&txn, rid1);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
    res = lock_mgr.Unlock(&txn, rid1);
    EXPECT_EQ(res, false);
    res = lock_mgr.LockShared(&txn, rid2);
    EXPECT_EQ(res, true);
    txn_mgr.Commit(&txn);
    EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);

  }

} // namespace cmudb
