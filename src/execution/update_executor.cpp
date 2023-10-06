//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() { child_executor_->Init(); }

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  uint64_t cnt = 0;
  while (true) {
    bool status = child_executor_->Next(tuple, rid);
    if (!status) {
      break;
    }
    auto catalog = exec_ctx_->GetCatalog();
    auto table_info = catalog->GetTable(plan_->table_oid_);
    table_info->table_->UpdateTupleMeta({INVALID_TXN_ID, INVALID_TXN_ID, true}, *rid);

    Tuple t = {};
    std::vector<Value> values{};
    values.reserve(child_executor_->GetOutputSchema().GetColumnCount());
    for (const auto &col : plan_->target_expressions_) {
      values.push_back(col->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    t = Tuple{values, &child_executor_->GetOutputSchema()};

    auto new_rid = table_info->table_->InsertTuple({INVALID_TXN_ID, INVALID_TXN_ID, false}, t).value();
    for (auto index_info : catalog->GetTableIndexes(table_info->name_)) {
      auto key_tuple = tuple->KeyFromTuple(table_info->schema_, *index_info->index_->GetKeySchema(),
                                           index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(key_tuple, *rid, nullptr);
      key_tuple =
          t.KeyFromTuple(table_info->schema_, *index_info->index_->GetKeySchema(), index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(key_tuple, new_rid, nullptr);
    }
    ++cnt;
  }
  auto schema = Schema({{"row_num", BIGINT}});
  *tuple = Tuple{{{BIGINT, cnt}}, &schema};
  return ok_ = !ok_;
}

}  // namespace bustub
