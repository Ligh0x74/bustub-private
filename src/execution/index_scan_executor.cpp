//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_);
  table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info->table_name_);
  auto index = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info->index_.get());
  iterator_ = std::make_unique<BPlusTreeIndexIteratorForTwoIntegerColumn>(index->GetBeginIterator());
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  for (; !iterator_->IsEnd(); ++(*iterator_)) {
    *rid = (**iterator_).second;
    auto p = table_info_->table_->GetTuple(*rid);
    if (!p.first.is_deleted_) {
      *tuple = p.second;
      ++(*iterator_);
      return true;
    }
  }
  return false;
}

}  // namespace bustub
