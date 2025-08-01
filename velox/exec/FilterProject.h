/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include "velox/core/PlanNode.h"
#include "velox/exec/Operator.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/expression/Expr.h"

namespace facebook::velox::exec {
class FilterProject : public Operator {
 public:
  FilterProject(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::FilterNode>& filter,
      const std::shared_ptr<const core::ProjectNode>& project);

  bool isFilter() const override {
    return true;
  }

  bool preservesOrder() const override {
    return true;
  }

  bool needsInput() const override {
    return !input_;
  }

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  BlockingReason isBlocked(ContinueFuture* /* unused */) override {
    return BlockingReason::kNotBlocked;
  }

  bool startDrain() override {
    // No need to drain for project/filter operator.
    return false;
  }

  bool isFinished() override;

  void close() override {
    Operator::close();
    if (exprs_ != nullptr) {
      exprs_->clear();
    } else {
      VELOX_CHECK(!initialized_);
    }
  }

  /// Data for accelerator conversion.
  struct Export {
    const ExprSet* exprs;
    bool hasFilter;
    const std::vector<IdentityProjection>* resultProjections;
  };

  Export exprsAndProjection() const {
    return Export{exprs_.get(), hasFilter_, &resultProjections_};
  }

  void initialize() override;

  /// Ensures that expression stats are added to the operator stats if their
  /// tracking is enabled via query config.
  OperatorStats stats(bool clear) override;

 private:
  // Evaluate filter on all rows. Return number of rows that passed the filter.
  // Populate filterEvalCtx_.selectedBits and selectedIndices with the indices
  // of the passing rows if only some rows pass the filter. If all or no rows
  // passed the filter filterEvalCtx_.selectedBits and selectedIndices are not
  // updated.
  vector_size_t filter(EvalCtx& evalCtx, const SelectivityVector& allRows);

  // Evaluate projections on the specified rows and return the results.
  // pre-condition: !isIdentityProjection_
  std::vector<VectorPtr> project(
      const SelectivityVector& rows,
      EvalCtx& evalCtx);

  // If true exprs_[0] is a filter and the other expressions are projections
  const bool hasFilter_{false};

  // Cached filter and project node for lazy initialization. After
  // initialization, they will be reset, and initialized_ will be set to true.
  std::shared_ptr<const core::ProjectNode> project_;
  std::shared_ptr<const core::FilterNode> filter_;
  bool initialized_{false};

  std::unique_ptr<ExprSet> exprs_;
  int32_t numExprs_;

  FilterEvalCtx filterEvalCtx_;

  // Indices for fields/input columns that are both an identity projection and
  // are referenced by either a filter or project expression. This is used to
  // identify fields that need to be preloaded before evaluating filters or
  // projections.
  // Consider projection with 2 expressions: f(c0) AND g(c1), c1
  // If c1 is a LazyVector and f(c0) AND g(c1) expression is evaluated first, it
  // will load c1 only for rows where f(c0) is true. However, c1 identity
  // projection needs all rows.
  std::vector<column_index_t> multiplyReferencedFieldIndices_;
};
} // namespace facebook::velox::exec
