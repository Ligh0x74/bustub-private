//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::CanTxnTakeLock(Transaction *txn, LockMode lock_mode) -> bool {
  if (txn->GetState() == TransactionState::SHRINKING) {
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED) {
        return true;
      }
    }
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  if (txn->GetState() == TransactionState::GROWING) {
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
    }
    return true;
  }

  return false;
}

auto LockManager::CheckAppropriateLockOnTable(Transaction *txn, const table_oid_t &oid, LockMode row_lock_mode)
    -> bool {
  if (row_lock_mode == LockMode::SHARED) {
    if (txn->IsTableSharedLocked(oid) || txn->IsTableExclusiveLocked(oid) || txn->IsTableIntentionSharedLocked(oid) ||
        txn->IsTableIntentionExclusiveLocked(oid) || txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      return true;
    }
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
  }

  if (row_lock_mode == LockMode::EXCLUSIVE) {
    if (txn->IsTableExclusiveLocked(oid) || txn->IsTableIntentionExclusiveLocked(oid) ||
        txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      return true;
    }
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
  }

  return false;
}

auto LockManager::CanLockUpgrade(LockMode curr_lock_mode, LockMode requested_lock_mode) -> bool {
  if (curr_lock_mode == LockMode::INTENTION_SHARED) {
    return true;
  }
  if (curr_lock_mode == LockMode::SHARED || curr_lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    if (requested_lock_mode == LockMode::EXCLUSIVE || requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      return true;
    }
  } else if (curr_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    if (requested_lock_mode == LockMode::EXCLUSIVE) {
      return true;
    }
  }
  return false;
}

auto LockManager::AreLocksCompatible(LockMode l1, LockMode l2) -> bool {
  if (l1 == LockMode::INTENTION_SHARED) {
    return l2 != LockMode::EXCLUSIVE;
  }
  if (l1 == LockMode::INTENTION_EXCLUSIVE) {
    return l2 == LockMode::INTENTION_SHARED || l2 == LockMode::INTENTION_EXCLUSIVE;
  }
  if (l1 == LockMode::SHARED) {
    return l2 == LockMode::INTENTION_SHARED || l2 == LockMode::SHARED;
  }
  if (l1 == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    return l2 == LockMode::INTENTION_SHARED;
  }
  return false;
}

auto LockManager::TryLock(txn_id_t txn_id, LockRequestQueue *lock_request_queue, LockMode lock_mode) -> bool {
  if (lock_request_queue->upgrading_ != INVALID_TXN_ID && lock_request_queue->upgrading_ != txn_id) {
    return false;
  }

  LockRequest *curr_lock_request = nullptr;
  for (auto &lock_request : lock_request_queue->request_queue_) {
    if (lock_request->txn_id_ == txn_id) {
      curr_lock_request = lock_request.get();
    }
    if ((lock_request->granted_ || (lock_request->txn_id_ != txn_id && curr_lock_request == nullptr)) &&
        !AreLocksCompatible(lock_request->lock_mode_, lock_mode)) {
      return false;
    }
  }
  curr_lock_request->granted_ = true;
  return true;
}

auto LockManager::UpgradeLockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  std::unique_lock<std::mutex> table_lock_map_latch(table_lock_map_latch_);
  auto q = table_lock_map_[oid];
  std::unique_lock<std::mutex> latch(q->latch_);
  table_lock_map_latch.unlock();
  if (q->upgrading_ != INVALID_TXN_ID) {
    q->request_queue_.remove_if([&](const std::shared_ptr<LockRequest> &lock_request) {
      return lock_request->txn_id_ == txn->GetTransactionId();
    });
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }
  q->upgrading_ = txn->GetTransactionId();
  auto lock_request = std::find_if(
      q->request_queue_.begin(), q->request_queue_.end(),
      [&](std::shared_ptr<LockRequest> &lock_request) { return lock_request->txn_id_ == txn->GetTransactionId(); });
  (*lock_request)->granted_ = false;
  (*lock_request)->lock_mode_ = lock_mode;
  q->cv_.wait(latch, [&] {
    return txn->GetState() == TransactionState::ABORTED || TryLock(txn->GetTransactionId(), q.get(), lock_mode);
  });
  q->upgrading_ = INVALID_TXN_ID;
  if (txn->GetState() == TransactionState::ABORTED) {
    q->request_queue_.remove_if([&](const std::shared_ptr<LockRequest> &lock_request) {
      return lock_request->txn_id_ == txn->GetTransactionId();
    });
    latch.unlock();
    q->cv_.notify_all();
    return false;
  }
  latch.unlock();
  return true;
}

auto LockManager::UpgradeLockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  std::unique_lock<std::mutex> row_lock_map_latch(row_lock_map_latch_);
  auto q = row_lock_map_[rid];
  std::unique_lock<std::mutex> latch(q->latch_);
  row_lock_map_latch.unlock();
  if (q->upgrading_ != INVALID_TXN_ID) {
    q->request_queue_.remove_if([&](const std::shared_ptr<LockRequest> &lock_request) {
      return lock_request->txn_id_ == txn->GetTransactionId();
    });
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }
  q->upgrading_ = txn->GetTransactionId();
  auto lock_request = std::find_if(
      q->request_queue_.begin(), q->request_queue_.end(),
      [&](std::shared_ptr<LockRequest> &lock_request) { return lock_request->txn_id_ == txn->GetTransactionId(); });
  (*lock_request)->granted_ = false;
  (*lock_request)->lock_mode_ = lock_mode;
  q->cv_.wait(latch, [&] {
    return txn->GetState() == TransactionState::ABORTED || TryLock(txn->GetTransactionId(), q.get(), lock_mode);
  });
  q->upgrading_ = INVALID_TXN_ID;
  if (txn->GetState() == TransactionState::ABORTED) {
    q->request_queue_.remove_if([&](const std::shared_ptr<LockRequest> &lock_request) {
      return lock_request->txn_id_ == txn->GetTransactionId();
    });
    latch.unlock();
    q->cv_.notify_all();
    return false;
  }
  latch.unlock();
  return true;
}

void LockManager::GetCurrTableLockModAndLockSet(Transaction *txn, const table_oid_t &oid,
                                                LockMode &curr_table_lock_mode,
                                                std::shared_ptr<std::unordered_set<table_oid_t>> &curr_table_lock_set) {
  // 事务是否需要加锁
  if (txn->IsTableIntentionSharedLocked(oid)) {
    curr_table_lock_mode = LockMode::INTENTION_SHARED;
    curr_table_lock_set = txn->GetIntentionSharedTableLockSet();
  } else if (txn->IsTableSharedLocked(oid)) {
    curr_table_lock_mode = LockMode::SHARED;
    curr_table_lock_set = txn->GetSharedTableLockSet();
  } else if (txn->IsTableIntentionExclusiveLocked(oid)) {
    curr_table_lock_mode = LockMode::INTENTION_EXCLUSIVE;
    curr_table_lock_set = txn->GetIntentionExclusiveTableLockSet();
  } else if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
    curr_table_lock_mode = LockMode::SHARED_INTENTION_EXCLUSIVE;
    curr_table_lock_set = txn->GetSharedIntentionExclusiveTableLockSet();
  } else if (txn->IsTableExclusiveLocked(oid)) {
    curr_table_lock_mode = LockMode::EXCLUSIVE;
    curr_table_lock_set = txn->GetExclusiveTableLockSet();
  }
}

void LockManager::GetRequestedTableLockModAndLockSet(
    Transaction *txn, const table_oid_t &oid, LockMode requested_table_lock_mode,
    std::shared_ptr<std::unordered_set<table_oid_t>> &requested_table_lock_set) {
  // 事务是否需要加锁
  if (requested_table_lock_mode == LockMode::INTENTION_SHARED) {
    requested_table_lock_set = txn->GetIntentionSharedTableLockSet();
  } else if (requested_table_lock_mode == LockMode::SHARED) {
    requested_table_lock_set = txn->GetSharedTableLockSet();
  } else if (requested_table_lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    requested_table_lock_set = txn->GetIntentionExclusiveTableLockSet();
  } else if (requested_table_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    requested_table_lock_set = txn->GetSharedIntentionExclusiveTableLockSet();
  } else {
    requested_table_lock_set = txn->GetExclusiveTableLockSet();
  }
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  if (!CanTxnTakeLock(txn, lock_mode)) {
    return false;
  }

  LockMode curr_table_lock_mode;
  std::shared_ptr<std::unordered_set<table_oid_t>> curr_table_lock_set = nullptr;
  std::shared_ptr<std::unordered_set<table_oid_t>> requested_table_lock_set = nullptr;
  GetCurrTableLockModAndLockSet(txn, oid, curr_table_lock_mode, curr_table_lock_set);
  GetRequestedTableLockModAndLockSet(txn, oid, lock_mode, requested_table_lock_set);

  if (curr_table_lock_set == nullptr) {
    std::unique_lock<std::mutex> table_lock_map_latch(table_lock_map_latch_);
    if (table_lock_map_.count(oid) == 0) {
      table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
    }
    auto q = table_lock_map_[oid];
    std::unique_lock<std::mutex> latch(q->latch_);
    table_lock_map_latch.unlock();
    q->request_queue_.push_back(std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid));
    if (q->request_queue_.empty()) {
      q->request_queue_.back()->granted_ = true;
      return true;
    }
    q->cv_.wait(latch, [&] {
      return txn->GetState() == TransactionState::ABORTED || TryLock(txn->GetTransactionId(), q.get(), lock_mode);
    });
    if (txn->GetState() == TransactionState::ABORTED) {
      q->request_queue_.remove_if([&](const std::shared_ptr<LockRequest> &lock_request) {
        return lock_request->txn_id_ == txn->GetTransactionId();
      });
      latch.unlock();
      q->cv_.notify_all();
      return false;
    }
    latch.unlock();

    // 事务是否需要加锁
    requested_table_lock_set->insert(oid);
    return true;
  }

  if (curr_table_lock_mode == lock_mode) {
    return true;
  }
  if (!CanLockUpgrade(curr_table_lock_mode, lock_mode)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
  }

  // 事务是否需要加锁
  curr_table_lock_set->erase(oid);

  if (!UpgradeLockTable(txn, lock_mode, oid)) {
    return false;
  }

  // 事务是否需要加锁
  requested_table_lock_set->insert(oid);
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  if (!(*txn->GetSharedRowLockSet())[oid].empty() || !(*txn->GetExclusiveRowLockSet())[oid].empty()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  LockMode curr_table_lock_mode;
  std::shared_ptr<std::unordered_set<table_oid_t>> curr_table_lock_set = nullptr;
  GetCurrTableLockModAndLockSet(txn, oid, curr_table_lock_mode, curr_table_lock_set);

  if (curr_table_lock_set == nullptr) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  // 事务是否需要加锁
  curr_table_lock_set->erase(oid);
  if (curr_table_lock_mode == LockMode::SHARED) {
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      txn->SetState(TransactionState::SHRINKING);
    }
  } else if (curr_table_lock_mode == LockMode::EXCLUSIVE) {
    txn->SetState(TransactionState::SHRINKING);
  }

  std::unique_lock<std::mutex> table_lock_map_latch(table_lock_map_latch_);
  auto q = table_lock_map_[oid];
  std::unique_lock<std::mutex> latch(q->latch_);
  table_lock_map_latch.unlock();
  q->request_queue_.remove_if([&](const std::shared_ptr<LockRequest> &lock_request) {
    return lock_request->txn_id_ == txn->GetTransactionId();
  });
  latch.unlock();
  q->cv_.notify_all();
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  if (lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
  if (!CanTxnTakeLock(txn, lock_mode) || !CheckAppropriateLockOnTable(txn, oid, lock_mode)) {
    return false;
  }

  if (txn->IsRowSharedLocked(oid, rid)) {
    if (lock_mode == LockMode::SHARED) {
      return true;
    }

    // 事务是否需要加锁
    (*txn->GetSharedRowLockSet())[oid].erase(rid);

    if (!UpgradeLockRow(txn, lock_mode, oid, rid)) {
      return false;
    }

    // 事务是否需要加锁
    (*txn->GetExclusiveRowLockSet())[oid].insert(rid);
    return true;
  }

  if (txn->IsRowExclusiveLocked(oid, rid)) {
    if (lock_mode == LockMode::EXCLUSIVE) {
      return true;
    }
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
  }

  std::unique_lock<std::mutex> row_lock_map_latch(row_lock_map_latch_);
  if (row_lock_map_.count(rid) == 0) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  auto q = row_lock_map_[rid];
  std::unique_lock<std::mutex> latch(q->latch_);
  row_lock_map_latch.unlock();
  q->request_queue_.push_back(std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid));
  if (q->request_queue_.empty()) {
    q->request_queue_.back()->granted_ = true;
    return true;
  }
  q->cv_.wait(latch, [&] {
    return txn->GetState() == TransactionState::ABORTED || TryLock(txn->GetTransactionId(), q.get(), lock_mode);
  });
  if (txn->GetState() == TransactionState::ABORTED) {
    q->request_queue_.remove_if([&](const std::shared_ptr<LockRequest> &lock_request) {
      return lock_request->txn_id_ == txn->GetTransactionId();
    });
    latch.unlock();
    q->cv_.notify_all();
    return false;
  }
  latch.unlock();

  // 事务是否需要加锁
  if (lock_mode == LockMode::SHARED) {
    (*txn->GetSharedRowLockSet())[oid].insert(rid);
  } else {
    (*txn->GetExclusiveRowLockSet())[oid].insert(rid);
  }
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  if (!txn->IsRowSharedLocked(oid, rid) && !txn->IsRowExclusiveLocked(oid, rid)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  // 事务是否需要加锁
  if (txn->IsRowSharedLocked(oid, rid)) {
    if (!force && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      txn->SetState(TransactionState::SHRINKING);
    }
    (*txn->GetSharedRowLockSet())[oid].erase(rid);
  } else {
    if (!force) {
      txn->SetState(TransactionState::SHRINKING);
    }
    (*txn->GetExclusiveRowLockSet())[oid].erase(rid);
  }

  std::unique_lock<std::mutex> row_lock_map_latch(row_lock_map_latch_);
  auto q = row_lock_map_[rid];
  std::unique_lock<std::mutex> latch(q->latch_);
  row_lock_map_latch.unlock();
  q->request_queue_.remove_if([&](const std::shared_ptr<LockRequest> &lock_request) {
    return lock_request->txn_id_ == txn->GetTransactionId();
  });
  latch.unlock();
  q->cv_.notify_all();
  return true;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

}  // namespace bustub
