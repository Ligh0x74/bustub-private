//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  left_ht_.clear();
  bucket_ = nullptr;
  left_rid_set_.clear();
  ok_ = true;

  Tuple tuple;
  RID rid;
  while (left_executor_->Next(&tuple, &rid)) {
    left_ht_[MakeLeftJoinKey(&tuple)].push_back(tuple);
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (ok_) {
    if (bucket_ == nullptr || bucket_iterator_ == bucket_->cend()) {
      ok_ = false;
      while (right_executor_->Next(&right_tuple_, &right_rid_)) {
        auto key = MakeRightJoinKey(&right_tuple_);
        if (left_ht_.count(key) != 0) {
          ok_ = true;
          bucket_ = &left_ht_[key];
          bucket_iterator_ = bucket_->cbegin();
          break;
        }
      }
      if (!ok_) {
        left_ht_iterator_ = left_ht_.cbegin();
        bucket_ = nullptr;
        break;
      }
    }

    for (; bucket_iterator_ != bucket_->cend(); ++bucket_iterator_) {
      if (MakeLeftJoinKey(&(*bucket_iterator_)) == MakeRightJoinKey(&right_tuple_)) {
        if (plan_->GetJoinType() == JoinType::LEFT) {
          left_rid_set_.insert(bucket_iterator_.base());
        }
        std::vector<Value> values;
        values.reserve(GetOutputSchema().GetColumnCount());
        for (size_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
          values.push_back(bucket_iterator_->GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (size_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
          values.push_back(right_tuple_.GetValue(&right_executor_->GetOutputSchema(), i));
        }
        *tuple = Tuple{values, &GetOutputSchema()};
        ++bucket_iterator_;
        return true;
      }
    }
  }

  if (plan_->GetJoinType() == JoinType::LEFT) {
    while (true) {
      if (bucket_ == nullptr || bucket_iterator_ == bucket_->cend()) {
        if (bucket_ != nullptr && ++left_ht_iterator_ == left_ht_.cend()) {
          break;
        }
        bucket_ = &left_ht_iterator_->second;
        bucket_iterator_ = bucket_->cbegin();
      }

      for (; bucket_iterator_ != bucket_->cend(); ++bucket_iterator_) {
        if (left_rid_set_.count(bucket_iterator_.base()) == 0) {
          std::vector<Value> values;
          values.reserve(GetOutputSchema().GetColumnCount());
          for (size_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
            values.push_back(bucket_iterator_->GetValue(&left_executor_->GetOutputSchema(), i));
          }
          for (size_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
            auto type = right_executor_->GetOutputSchema().GetColumn(i).GetType();
            values.emplace_back(ValueFactory::GetNullValueByType(type));
          }
          *tuple = Tuple{values, &GetOutputSchema()};
          ++bucket_iterator_;
          return true;
        }
      }
    }
  }

  return false;
}

}  // namespace bustub
