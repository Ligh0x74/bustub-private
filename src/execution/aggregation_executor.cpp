//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void AggregationExecutor::Init() {
  child_executor_->Init();

  aht_ = std::make_unique<SimpleAggregationHashTable>(plan_->aggregates_, plan_->agg_types_);

  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    aht_->InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }
  aht_iterator_ = std::make_unique<SimpleAggregationHashTable::Iterator>(aht_->Begin());
  ok_ = aht_->Begin() == aht_->End();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (*aht_iterator_ != aht_->End()) {
    auto key = aht_iterator_->Key().group_bys_;
    auto &val = aht_iterator_->Val().aggregates_;
    key.insert(key.end(), val.begin(), val.end());
    *tuple = Tuple{key, &GetOutputSchema()};
    ++(*aht_iterator_);
    return true;
  }
  if (ok_ && GetOutputSchema().GetColumnCount() == plan_->aggregates_.size()) {
    const auto &val = aht_->GenerateInitialAggregateValue().aggregates_;
    *tuple = Tuple{val, &GetOutputSchema()};
    ok_ = false;
    return true;
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
