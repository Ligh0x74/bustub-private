#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Spring: You should at least support join keys of the form:
  // 1. <column expr> = <column expr>
  // 2. <column expr> = <column expr> AND <column expr> = <column expr>

  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");

    auto check = [](const ComparisonExpression *expr) -> std::vector<AbstractExpressionRef> {
      if (expr->comp_type_ == ComparisonType::Equal) {
        if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
            left_expr != nullptr) {
          if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get());
              right_expr != nullptr) {
            AbstractExpressionRef left_expr_tuple_0;
            AbstractExpressionRef right_expr_tuple_1;

            if (left_expr->GetTupleIdx() == 0) {
              left_expr_tuple_0 = expr->children_[0];
              right_expr_tuple_1 = expr->children_[1];
            } else {
              left_expr_tuple_0 = expr->children_[1];
              right_expr_tuple_1 = expr->children_[0];
            }

            return {left_expr_tuple_0, right_expr_tuple_1};
          }
        }
      }
      return {};
    };

    if (const auto *expr = dynamic_cast<const ComparisonExpression *>(nlj_plan.Predicate().get()); expr != nullptr) {
      if (expr->comp_type_ == ComparisonType::Equal) {
        auto v = check(expr);
        if (v.size() == 2) {
          return std::make_unique<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                    nlj_plan.GetRightPlan(), std::vector{v[0]}, std::vector{v[1]},
                                                    nlj_plan.GetJoinType());
        }
      }
    }

    if (const auto *expr = dynamic_cast<const LogicExpression *>(nlj_plan.Predicate().get()); expr != nullptr) {
      if (expr->logic_type_ == LogicType::And) {
        if (const auto *left_expr = dynamic_cast<const ComparisonExpression *>(expr->children_[0].get());
            left_expr != nullptr) {
          if (const auto *right_expr = dynamic_cast<const ComparisonExpression *>(expr->children_[1].get());
              right_expr != nullptr) {
            std::vector<AbstractExpressionRef> left_expr_tuple_0;
            std::vector<AbstractExpressionRef> right_expr_tuple_1;

            auto v1 = check(left_expr);
            auto v2 = check(right_expr);
            if (v1.size() == 2 && v2.size() == 2) {
              left_expr_tuple_0.push_back(v1[0]);
              left_expr_tuple_0.push_back(v2[0]);
              right_expr_tuple_1.push_back(v1[1]);
              right_expr_tuple_1.push_back(v2[1]);
              return std::make_unique<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                        nlj_plan.GetRightPlan(), std::vector{left_expr_tuple_0},
                                                        std::vector{right_expr_tuple_1}, nlj_plan.GetJoinType());
            }
          }
        }
      }
    }
  }

  return optimized_plan;
}

}  // namespace bustub
