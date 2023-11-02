#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  tuples_.clear();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.push_back(tuple);
  }

  std::sort(tuples_.begin(), tuples_.end(), [&](const Tuple &a, const Tuple &b) {
    for (const auto &p : plan_->GetOrderBy()) {
      auto va = p.second->Evaluate(&a, child_executor_->GetOutputSchema());
      auto vb = p.second->Evaluate(&b, child_executor_->GetOutputSchema());
      if (va.CompareEquals(vb) != CmpBool::CmpTrue) {
        if (p.first == OrderByType::DESC) {
          return va.CompareGreaterThan(vb);
        }
        return va.CompareLessThan(vb);
      }
    }
    return CmpBool::CmpTrue;
  });
  iterator_ = std::make_unique<std::vector<Tuple>::const_iterator>(tuples_.cbegin());
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (*iterator_ != tuples_.cend()) {
    *tuple = **iterator_;
    ++(*iterator_);
    return true;
  }
  return false;
}

}  // namespace bustub
