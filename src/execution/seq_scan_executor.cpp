//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  //  iterator_ =
  //      std::make_unique<TableIterator>(exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->table_->MakeIterator());

  const auto &txn = exec_ctx_->GetTransaction();
  if (exec_ctx_->IsDelete()) {
    if (!exec_ctx_->GetLockManager()->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->table_oid_)) {
      throw ExecutionException("SeqScanExecutor Lock Fail");
    }
  } else if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED &&
             !txn->IsTableIntentionExclusiveLocked(plan_->table_oid_)) {
    if (!exec_ctx_->GetLockManager()->LockTable(txn, LockManager::LockMode::INTENTION_SHARED, plan_->table_oid_)) {
      throw ExecutionException("SeqScanExecutor Lock Fail");
    }
  }
  iterator_ = std::make_unique<TableIterator>(
      exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->table_->MakeEagerIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  //  for (; !iterator_->IsEnd(); ++(*iterator_)) {
  //    if (!iterator_->GetTuple().first.is_deleted_) {
  //      *tuple = iterator_->GetTuple().second;
  //      *rid = iterator_->GetRID();
  //      ++(*iterator_);
  //      return true;
  //    }
  //  }
  //  return false;

  const auto &txn = exec_ctx_->GetTransaction();
  for (; !iterator_->IsEnd(); ++(*iterator_)) {
    if (exec_ctx_->IsDelete()) {
      if (!exec_ctx_->GetLockManager()->LockRow(txn, LockManager::LockMode::EXCLUSIVE, plan_->table_oid_,
                                                iterator_->GetRID())) {
        throw ExecutionException("SeqScanExecutor Lock Fail");
      }
    } else if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      if (!exec_ctx_->GetLockManager()->LockRow(txn, LockManager::LockMode::SHARED, plan_->table_oid_,
                                                iterator_->GetRID())) {
        throw ExecutionException("SeqScanExecutor Lock Fail");
      }
    }
    if (!iterator_->GetTuple().first.is_deleted_) {
      *tuple = iterator_->GetTuple().second;
      *rid = iterator_->GetRID();
      ++(*iterator_);
      return true;
    }
  }
  return false;
}

}  // namespace bustub
