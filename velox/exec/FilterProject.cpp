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
#include "velox/exec/FilterProject.h"
#include "velox/core/Expressions.h"
#include "velox/expression/Expr.h"
#include "velox/expression/FieldReference.h"

namespace facebook::velox::exec {
namespace {
bool checkAddIdentityProjection(
    const core::TypedExprPtr& projection,
    const RowTypePtr& inputType,
    column_index_t outputChannel,
    std::vector<IdentityProjection>& identityProjections) {
  if (auto field = core::TypedExprs::asFieldAccess(projection)) {
    const auto& inputs = field->inputs();
    if (inputs.empty() ||
        (inputs.size() == 1 &&
         dynamic_cast<const core::InputTypedExpr*>(inputs[0].get()))) {
      const auto inputChannel = inputType->getChildIdx(field->name());
      identityProjections.emplace_back(inputChannel, outputChannel);
      return true;
    }
  }

  return false;
}

// Split stats to attrbitute cardinality reduction to the Filter node.
std::vector<OperatorStats> splitStats(
    const OperatorStats& combinedStats,
    const core::PlanNodeId& filterNodeId) {
  OperatorStats filterStats;

  filterStats.operatorId = combinedStats.operatorId;
  filterStats.pipelineId = combinedStats.pipelineId;
  filterStats.planNodeId = filterNodeId;
  filterStats.operatorType = combinedStats.operatorType;
  filterStats.numDrivers = combinedStats.numDrivers;

  filterStats.inputBytes = combinedStats.inputBytes;
  filterStats.inputPositions = combinedStats.inputPositions;
  filterStats.inputVectors = combinedStats.inputVectors;

  // Estimate Filter's output bytes based on cardinality change.
  const double filterRate = combinedStats.inputPositions > 0
      ? (combinedStats.outputPositions * 1.0 / combinedStats.inputPositions)
      : 1.0;

  filterStats.outputBytes = (uint64_t)(filterStats.inputBytes * filterRate);
  filterStats.outputPositions = combinedStats.outputPositions;
  filterStats.outputVectors = combinedStats.outputVectors;

  auto projectStats = combinedStats;
  projectStats.inputBytes = filterStats.outputBytes;
  projectStats.inputPositions = filterStats.outputPositions;
  projectStats.inputVectors = filterStats.outputVectors;

  return {std::move(projectStats), std::move(filterStats)};
}

} // namespace

FilterProject::FilterProject(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::FilterNode>& filter,
    const std::shared_ptr<const core::ProjectNode>& project)
    : Operator(
          driverCtx,
          project ? project->outputType() : filter->outputType(),
          operatorId,
          project ? project->id() : filter->id(),
          "FilterProject"),
      hasFilter_(filter != nullptr),
      project_(project),
      filter_(filter) {
  if (filter_ != nullptr && project_ != nullptr) {
    folly::Synchronized<OperatorStats>& opStats = Operator::stats();
    opStats.withWLock([&](auto& stats) {
      stats.setStatSplitter(
          [filterId = filter_->id()](const auto& combinedStats) {
            return splitStats(combinedStats, filterId);
          });
    });
  }
}

void FilterProject::initialize() {
  Operator::initialize();
  std::vector<core::TypedExprPtr> allExprs;
  if (hasFilter_) {
    VELOX_CHECK_NOT_NULL(filter_);
    allExprs.push_back(filter_->filter());
  }
  if (project_) {
    const auto& inputType = project_->sources()[0]->outputType();
    for (column_index_t i = 0; i < project_->projections().size(); i++) {
      auto& projection = project_->projections()[i];
      bool identityProjection = checkAddIdentityProjection(
          projection, inputType, i, identityProjections_);
      if (!identityProjection) {
        allExprs.push_back(projection);
        resultProjections_.emplace_back(allExprs.size() - 1, i);
      }
    }
  } else {
    for (column_index_t i = 0; i < outputType_->size(); ++i) {
      identityProjections_.emplace_back(i, i);
    }
    isIdentityProjection_ = true;
  }
  numExprs_ = allExprs.size();
  exprs_ = makeExprSetFromFlag(std::move(allExprs), operatorCtx_->execCtx());

  if (numExprs_ > 0 && !identityProjections_.empty()) {
    const auto inputType = project_ ? project_->sources()[0]->outputType()
                                    : filter_->sources()[0]->outputType();
    std::unordered_set<uint32_t> distinctFieldIndices;
    for (auto field : exprs_->distinctFields()) {
      auto fieldIndex = inputType->getChildIdx(field->name());
      distinctFieldIndices.insert(fieldIndex);
    }
    for (auto identityField : identityProjections_) {
      if (distinctFieldIndices.find(identityField.inputChannel) !=
          distinctFieldIndices.end()) {
        multiplyReferencedFieldIndices_.push_back(identityField.inputChannel);
      }
    }
  }
  filter_.reset();
  project_.reset();
}

void FilterProject::addInput(RowVectorPtr input) {
  input_ = std::move(input);
}

bool FilterProject::isFinished() {
  return noMoreInput_ && !input_;
}

RowVectorPtr FilterProject::getOutput() {
  if (!input_) {
    return nullptr;
  }
  SCOPE_EXIT {
    input_.reset();
  };

  vector_size_t size = input_->size();
  LocalSelectivityVector localRows(*operatorCtx_->execCtx(), size);
  auto* rows = localRows.get();
  VELOX_DCHECK_NOT_NULL(rows);
  rows->setAll();
  EvalCtx evalCtx(operatorCtx_->execCtx(), exprs_.get(), input_.get());

  // Pre-load lazy vectors which are referenced by both expressions and identity
  // projections.
  for (auto fieldIdx : multiplyReferencedFieldIndices_) {
    evalCtx.ensureFieldLoaded(fieldIdx, *rows);
  }

  if (!hasFilter_) {
    VELOX_CHECK(!isIdentityProjection_);
    auto results = project(*rows, evalCtx);
    return fillOutput(size, nullptr, results);
  }

  // evaluate filter
  auto numOut = filter(evalCtx, *rows);
  if (numOut == 0) { // no rows passed the filer
    input_ = nullptr;
    return nullptr;
  }

  const bool allRowsSelected = (numOut == size);
  // evaluate projections (if present)
  std::vector<VectorPtr> results;
  if (!isIdentityProjection_) {
    if (!allRowsSelected) {
      rows->setFromBits(filterEvalCtx_.selectedBits->as<uint64_t>(), size);
    }
    results = project(*rows, evalCtx);
  }

  return fillOutput(
      numOut,
      allRowsSelected ? nullptr : filterEvalCtx_.selectedIndices,
      results);
}

std::vector<VectorPtr> FilterProject::project(
    const SelectivityVector& rows,
    EvalCtx& evalCtx) {
  std::vector<VectorPtr> results;
  exprs_->eval(
      hasFilter_ ? 1 : 0, numExprs_, !hasFilter_, rows, evalCtx, results);
  return results;
}

vector_size_t FilterProject::filter(
    EvalCtx& evalCtx,
    const SelectivityVector& allRows) {
  std::vector<VectorPtr> results;
  exprs_->eval(0, 1, true, allRows, evalCtx, results);
  return processFilterResults(results[0], allRows, filterEvalCtx_, pool());
}

OperatorStats FilterProject::stats(bool clear) {
  auto stats = Operator::stats(clear);
  if (operatorCtx()
          ->driverCtx()
          ->queryConfig()
          .operatorTrackExpressionStats() &&
      exprs_ != nullptr) {
    stats.expressionStats = exprs_->stats(true /*excludeSpecialForm*/);
  }
  return stats;
}
} // namespace facebook::velox::exec
