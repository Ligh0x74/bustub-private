//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  uint64_t cnt = 0;
  while (true) {
    bool status = child_executor_->Next(tuple, rid);
    if (!status) {
      break;
    }
    auto catalog = exec_ctx_->GetCatalog();
    auto table_info = catalog->GetTable(plan_->table_oid_);
    table_info->table_->UpdateTupleMeta({INVALID_TXN_ID, INVALID_TXN_ID, true}, *rid);
    for (auto index_info : catalog->GetTableIndexes(table_info->name_)) {
      auto key_tuple = tuple->KeyFromTuple(table_info->schema_, *index_info->index_->GetKeySchema(),
                                           index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(key_tuple, *rid, nullptr);
    }
    ++cnt;
  }
  auto schema = Schema({{"row_num", BIGINT}});
  *tuple = Tuple{{{BIGINT, cnt}}, &schema};
  return ok_ = !ok_;
}

}  // namespace bustub
