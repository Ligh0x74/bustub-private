#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();

  auto cmp = [&](const Tuple &a, const Tuple &b) {
    for (const auto &p : plan_->GetOrderBy()) {
      auto va = p.second->Evaluate(&a, plan_->OutputSchema());
      auto vb = p.second->Evaluate(&b, plan_->OutputSchema());
      if (va.CompareEquals(vb) != CmpBool::CmpTrue) {
        if (p.first == OrderByType::DESC) {
          return va.CompareGreaterThan(vb);
        }
        return va.CompareLessThan(vb);
      }
    }
    return CmpBool::CmpTrue;
  };

  std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> q{cmp};

  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    q.push(tuple);
    if (q.size() > plan_->n_) {
      q.pop();
    }
  }

  tuples_.clear();
  while (!q.empty()) {
    tuples_.push_back(q.top());
    q.pop();
  }
  iterator_ = std::make_unique<std::vector<Tuple>::const_reverse_iterator>(tuples_.crbegin());
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (*iterator_ != tuples_.crend()) {
    *tuple = **iterator_;
    ++(*iterator_);
    return true;
  }
  return false;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return 0; };

}  // namespace bustub
