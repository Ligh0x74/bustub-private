//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() { left_executor_->Init(); }

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple right_tuple;
  RID right_rid;

  while (true) {
    if (cnt_ == 0) {
      if (!left_executor_->Next(&left_tuple_, &left_rid_)) {
        break;
      }
      right_executor_->Init();
    }
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      auto value = plan_->predicate_->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                                   right_executor_->GetOutputSchema());
      if (value.IsNull() || value.GetAs<bool>()) {
        ++cnt_;
        std::vector<Value> values;
        values.reserve(GetOutputSchema().GetColumnCount());
        for (size_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
          values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (size_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
          values.push_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
        }
        *tuple = Tuple{values, &GetOutputSchema()};
        return true;
      }
    }

    if (plan_->GetJoinType() == JoinType::LEFT && cnt_ == 0) {
      cnt_ = 0;
      std::vector<Value> values;
      values.reserve(GetOutputSchema().GetColumnCount());
      for (size_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
        values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
      }
      for (size_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
        auto type = right_executor_->GetOutputSchema().GetColumn(i).GetType();
        values.emplace_back(ValueFactory::GetNullValueByType(type));
      }
      *tuple = Tuple{values, &GetOutputSchema()};
      return true;
    }
    cnt_ = 0;
  }

  return false;
}

}  // namespace bustub
