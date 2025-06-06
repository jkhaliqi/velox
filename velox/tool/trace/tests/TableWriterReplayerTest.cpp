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

#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <algorithm>
#include <memory>
#include <string>

#include "folly/dynamic.h"
#include "velox/common/base/Fs.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/hyperloglog/SparseHll.h"
#include "velox/exec/OperatorTraceReader.h"
#include "velox/exec/PartitionFunction.h"
#include "velox/exec/TableWriter.h"
#include "velox/exec/TraceUtil.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/tool/trace/TableWriterReplayer.h"
#include "velox/tool/trace/TraceReplayRunner.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::core;
using namespace facebook::velox::common;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::connector;
using namespace facebook::velox::connector::hive;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::common::testutil;
using namespace facebook::velox::common::hll;

namespace facebook::velox::tool::trace::test {
class TableWriterReplayerTest : public HiveConnectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    HiveConnectorTestBase::SetUpTestCase();
    filesystems::registerLocalFileSystem();
    if (!isRegisteredVectorSerde()) {
      serializer::presto::PrestoVectorSerde::registerVectorSerde();
    }
    Type::registerSerDe();
    common::Filter::registerSerDe();
    connector::hive::HiveTableHandle::registerSerDe();
    connector::hive::LocationHandle::registerSerDe();
    connector::hive::HiveColumnHandle::registerSerDe();
    connector::hive::HiveInsertTableHandle::registerSerDe();
    connector::hive::HiveInsertFileNameGenerator::registerSerDe();
    core::PlanNode::registerSerDe();
    velox::exec::trace::registerDummySourceSerDe();
    core::ITypedExpr::registerSerDe();
    registerPartitionFunctionSerDe();
  }

  std::vector<RowVectorPtr> makeBatches(
      vector_size_t numBatches,
      std::function<RowVectorPtr(int32_t)> makeVector) {
    std::vector<RowVectorPtr> batches;
    batches.reserve(numBatches);
    for (int32_t i = 0; i < numBatches; ++i) {
      batches.push_back(makeVector(i));
    }
    return batches;
  }

  std::set<std::string> getLeafSubdirectories(
      const std::string& directoryPath) {
    std::set<std::string> subdirectories;
    for (auto& path : fs::recursive_directory_iterator(directoryPath)) {
      if (path.is_regular_file()) {
        subdirectories.emplace(path.path().parent_path().string());
      }
    }
    return subdirectories;
  }

  // Helper method to return InsertTableHandle.
  std::shared_ptr<core::InsertTableHandle> createInsertTableHandle(
      const RowTypePtr& outputRowType,
      const connector::hive::LocationHandle::TableType& outputTableType,
      const std::string& outputDirectoryPath,
      const std::vector<std::string>& partitionedBy,
      const std::shared_ptr<HiveBucketProperty> bucketProperty,
      const std::optional<CompressionKind> compressionKind = {}) {
    return std::make_shared<core::InsertTableHandle>(
        kHiveConnectorId,
        makeHiveInsertTableHandle(
            outputRowType->names(),
            outputRowType->children(),
            partitionedBy,
            bucketProperty,
            makeLocationHandle(
                outputDirectoryPath, std::nullopt, outputTableType),
            fileFormat_,
            compressionKind));
  }

  // Returns a table insert plan node.
  PlanNodePtr createInsertPlan(
      PlanBuilder& inputPlan,
      const RowTypePtr& inputRowType,
      const RowTypePtr& tableRowType,
      const std::string& outputDirectoryPath,
      const std::vector<std::string>& partitionedBy = {},
      std::shared_ptr<HiveBucketProperty> bucketProperty = nullptr,
      const std::optional<CompressionKind> compressionKind = {},
      const connector::hive::LocationHandle::TableType& outputTableType =
          connector::hive::LocationHandle::TableType::kNew,
      const CommitStrategy& outputCommitStrategy = CommitStrategy::kNoCommit,
      bool aggregateResult = true,
      std::shared_ptr<core::AggregationNode> aggregationNode = nullptr) {
    auto insertPlan = inputPlan
                          .addNode(addTableWriter(
                              inputRowType,
                              tableRowType->names(),
                              aggregationNode,
                              createInsertTableHandle(
                                  tableRowType,
                                  outputTableType,
                                  outputDirectoryPath,
                                  partitionedBy,
                                  bucketProperty,
                                  compressionKind),
                              !partitionedBy.empty(),
                              outputCommitStrategy))
                          .capturePlanNodeId(tableWriteNodeId_);
    if (aggregateResult) {
      insertPlan.project({TableWriteTraits::rowCountColumnName()})
          .singleAggregation(
              {},
              {fmt::format("sum({})", TableWriteTraits::rowCountColumnName())});
    }
    return insertPlan.planNode();
  }

  std::function<PlanNodePtr(std::string, PlanNodePtr)> addTableWriter(
      const RowTypePtr& inputColumns,
      const std::vector<std::string>& tableColumnNames,
      const std::shared_ptr<core::AggregationNode>& aggregationNode,
      const std::shared_ptr<core::InsertTableHandle>& insertHandle,
      bool hasPartitioningScheme,
      connector::CommitStrategy commitStrategy =
          connector::CommitStrategy::kNoCommit) {
    return [=](core::PlanNodeId nodeId,
               core::PlanNodePtr source) -> core::PlanNodePtr {
      std::shared_ptr<core::AggregationNode> aggNode = nullptr;
      if (aggregationNode == nullptr) {
        aggNode = generateAggregationNode(
            "c0", nodeId, {}, core::AggregationNode::Step::kPartial, source);
      } else {
        aggNode = aggregationNode;
      }

      return std::make_shared<core::TableWriteNode>(
          nodeId,
          inputColumns,
          tableColumnNames,
          aggNode,
          insertHandle,
          hasPartitioningScheme,
          TableWriteTraits::outputType(aggNode),
          commitStrategy,
          source);
    };
  }

  RowTypePtr getNonPartitionsColumns(
      const std::vector<std::string>& partitionedKeys,
      const RowTypePtr& rowType) {
    std::vector<std::string> dataColumnNames;
    std::vector<TypePtr> dataColumnTypes;
    for (auto i = 0; i < rowType->size(); i++) {
      auto name = rowType->names()[i];
      if (std::find(partitionedKeys.cbegin(), partitionedKeys.cend(), name) ==
          partitionedKeys.cend()) {
        dataColumnNames.emplace_back(name);
        dataColumnTypes.emplace_back(rowType->findChild(name));
      }
    }

    return ROW(std::move(dataColumnNames), std::move(dataColumnTypes));
  }

  std::vector<std::shared_ptr<connector::ConnectorSplit>>
  makeHiveSplitsFromDirectory(const std::string& directoryPath) {
    std::vector<std::shared_ptr<connector::ConnectorSplit>> splits;

    for (auto& path : fs::recursive_directory_iterator(directoryPath)) {
      if (path.is_regular_file()) {
        splits.push_back(HiveConnectorTestBase::makeHiveConnectorSplits(
            path.path().string(), 1, fileFormat_)[0]);
      }
    }

    return splits;
  }

  void checkWriteResults(
      const std::set<std::string>& actualDirs,
      const std::set<std::string>& expectedDirs,
      const std::vector<std::string>& partitionKeys,
      const RowTypePtr& rowType) {
    ASSERT_EQ(actualDirs.size(), expectedDirs.size());
    auto actualDirIt = actualDirs.begin();
    auto expectedDirIt = expectedDirs.begin();
    const auto newOutputType = getNonPartitionsColumns(partitionKeys, rowType);
    while (actualDirIt != actualDirs.end()) {
      const auto actualWrites =
          AssertQueryBuilder(PlanBuilder().tableScan(newOutputType).planNode())
              .splits(makeHiveSplitsFromDirectory(*actualDirIt))
              .copyResults(pool());
      const auto expectedWrites =
          AssertQueryBuilder(PlanBuilder().tableScan(newOutputType).planNode())
              .splits(makeHiveSplitsFromDirectory(*expectedDirIt))
              .copyResults(pool());
      assertEqualResults({actualWrites}, {expectedWrites});
      ++actualDirIt;
      ++expectedDirIt;
    }
  }

  static std::shared_ptr<core::AggregationNode> generateAggregationNode(
      const std::string& name,
      const core::PlanNodeId nodeId,
      const std::vector<core::FieldAccessTypedExprPtr>& groupingKeys,
      AggregationNode::Step step,
      const PlanNodePtr& source) {
    core::TypedExprPtr inputField =
        std::make_shared<const core::FieldAccessTypedExpr>(BIGINT(), name);
    auto callExpr = std::make_shared<const core::CallTypedExpr>(
        BIGINT(), std::vector<core::TypedExprPtr>{inputField}, "min");
    std::vector<std::string> aggregateNames = {"min"};
    std::vector<core::AggregationNode::Aggregate> aggregates = {
        core::AggregationNode::Aggregate{
            callExpr, {{BIGINT()}}, nullptr, {}, {}}};
    return std::make_shared<core::AggregationNode>(
        nodeId,
        step,
        groupingKeys,
        std::vector<core::FieldAccessTypedExprPtr>{},
        aggregateNames,
        aggregates,
        false, // ignoreNullKeys
        source);
  }

  std::string tableWriteNodeId_;
  FileFormat fileFormat_{FileFormat::DWRF};
};

TEST_F(TableWriterReplayerTest, runner) {
  vector_size_t size = 1'000;
  auto data = makeRowVector({
      makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      makeFlatVector<int32_t>(
          size, [](auto row) { return row * 2; }, nullEvery(7)),
  });
  auto sourceFilePath = TempFilePath::create();
  writeToFile(sourceFilePath->getPath(), data);

  std::string traceNodeId;
  auto targetDirectoryPath = TempDirectoryPath::create();
  auto rowType = asRowType(data->type());
  auto plan = PlanBuilder()
                  .tableScan(rowType)
                  .tableWrite(targetDirectoryPath->getPath())
                  .capturePlanNodeId(traceNodeId)
                  .planNode();
  const auto testDir = TempDirectoryPath::create();
  const auto traceRoot = fmt::format("{}/{}", testDir->getPath(), "traceRoot");
  std::shared_ptr<Task> task;
  auto results =
      AssertQueryBuilder(plan)
          .config(core::QueryConfig::kQueryTraceEnabled, true)
          .config(core::QueryConfig::kQueryTraceDir, traceRoot)
          .config(core::QueryConfig::kQueryTraceMaxBytes, 100UL << 30)
          .config(core::QueryConfig::kQueryTraceTaskRegExp, ".*")
          .config(core::QueryConfig::kQueryTraceNodeId, traceNodeId)
          .split(makeHiveConnectorSplit(sourceFilePath->getPath()))
          .copyResults(pool(), task);

  const auto taskTraceDir =
      exec::trace::getTaskTraceDirectory(traceRoot, *task);
  const auto opTraceDir = exec::trace::getOpTraceDirectory(
      taskTraceDir,
      traceNodeId,
      /*pipelineId=*/0,
      /*driverId=*/0);
  const auto summary =
      exec::trace::OperatorTraceSummaryReader(opTraceDir, pool()).read();
  ASSERT_EQ(summary.opType, "TableWrite");
  ASSERT_GT(summary.peakMemory, 0);
  ASSERT_GT(summary.inputRows, 0);
  // NOTE: the input bytes is 0 because of the lazy materialization.
  ASSERT_EQ(summary.inputBytes, 0);
  ASSERT_EQ(summary.rawInputRows, 0);
  ASSERT_EQ(summary.rawInputBytes, 0);

  FLAGS_root_dir = traceRoot;
  FLAGS_query_id = task->queryCtx()->queryId();
  FLAGS_task_id = task->taskId();
  FLAGS_node_id = traceNodeId;
  FLAGS_summary = true;
  {
    TraceReplayRunner runner;
    runner.init();
    runner.run();
  }

  const auto traceOutputDir = TempDirectoryPath::create();
  FLAGS_task_id = task->taskId();
  FLAGS_driver_ids = "";
  FLAGS_table_writer_output_dir = traceOutputDir->getPath();
  FLAGS_summary = false;
  {
    TraceReplayRunner runner;
    runner.init();
    runner.run();
  }
}

TEST_F(TableWriterReplayerTest, basic) {
  vector_size_t size = 1'000;
  auto data = makeRowVector({
      makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      makeFlatVector<int32_t>(
          size, [](auto row) { return row * 2; }, nullEvery(7)),
  });
  auto sourceFilePath = TempFilePath::create();
  writeToFile(sourceFilePath->getPath(), data);

  std::string planNodeId;
  auto targetDirectoryPath = TempDirectoryPath::create();
  auto rowType = asRowType(data->type());
  auto plan = PlanBuilder()
                  .tableScan(rowType)
                  .tableWrite(targetDirectoryPath->getPath())
                  .capturePlanNodeId(planNodeId)
                  .planNode();
  const auto testDir = TempDirectoryPath::create();
  const auto traceRoot = fmt::format("{}/{}", testDir->getPath(), "traceRoot");
  std::shared_ptr<Task> task;
  auto results =
      AssertQueryBuilder(plan)
          .config(core::QueryConfig::kQueryTraceEnabled, true)
          .config(core::QueryConfig::kQueryTraceDir, traceRoot)
          .config(core::QueryConfig::kQueryTraceMaxBytes, 100UL << 30)
          .config(core::QueryConfig::kQueryTraceTaskRegExp, ".*")
          .config(core::QueryConfig::kQueryTraceNodeId, planNodeId)
          .split(makeHiveConnectorSplit(sourceFilePath->getPath()))
          .copyResults(pool(), task);
  const auto traceOutputDir = TempDirectoryPath::create();
  const auto result = TableWriterReplayer(
                          traceRoot,
                          task->queryCtx()->queryId(),
                          task->taskId(),
                          "1",
                          "TableWriter",
                          "",
                          0,
                          executor_.get(),
                          traceOutputDir->getPath())
                          .run();

  // Second column contains details about written files.
  const auto details = results->childAt(TableWriteTraits::kFragmentChannel)
                           ->as<FlatVector<StringView>>();
  const folly::dynamic obj = folly::parseJson(details->valueAt(1));
  const auto fileWriteInfos = obj["fileWriteInfos"];
  ASSERT_EQ(1, fileWriteInfos.size());

  const auto writeFileName = fileWriteInfos[0]["writeFileName"].asString();
  // Read from 'writeFileName' and verify the data matches the original.
  plan = PlanBuilder().tableScan(rowType).planNode();

  const auto copy =
      AssertQueryBuilder(plan)
          .split(makeHiveConnectorSplit(fmt::format(
              "{}/{}", targetDirectoryPath->getPath(), writeFileName)))
          .copyResults(pool());
  assertEqualResults({data}, {copy});
}

TEST_F(TableWriterReplayerTest, partitionWrite) {
  const int32_t numPartitions = 4;
  const int32_t numBatches = 2;
  const auto rowType =
      ROW({"c0", "p0", "p1", "c1", "c3", "c5"},
          {INTEGER(), INTEGER(), VARCHAR(), BIGINT(), REAL(), VARCHAR()});
  const std::vector<std::string> partitionKeys = {"p0", "p1"};
  const std::vector<TypePtr> partitionTypes = {INTEGER(), VARCHAR()};
  const std::vector<RowVectorPtr> vectors = makeBatches(numBatches, [&](auto) {
    return makeRowVector(
        rowType->names(),
        {
            makeFlatVector<int32_t>(
                numPartitions, [&](auto row) { return row + 100; }),
            makeFlatVector<int32_t>(
                numPartitions, [&](auto row) { return row; }),
            makeFlatVector<StringView>(
                numPartitions,
                [&](auto row) {
                  return StringView::makeInline(fmt::format("str_{}", row));
                }),
            makeFlatVector<int64_t>(
                numPartitions, [&](auto row) { return row + 1000; }),
            makeFlatVector<float>(
                numPartitions, [&](auto row) { return row + 33.23; }),
            makeFlatVector<StringView>(
                numPartitions,
                [&](auto row) {
                  return StringView::makeInline(
                      fmt::format("bucket_{}", row * 3));
                }),
        });
  });
  const auto inputFilePaths = makeFilePaths(numBatches);
  for (int i = 0; i < numBatches; i++) {
    writeToFile(inputFilePaths[i]->getPath(), vectors[i]);
  }

  const auto outputDirectory = TempDirectoryPath::create();
  auto inputPlan = PlanBuilder().tableScan(rowType);
  auto plan = createInsertPlan(
      inputPlan,
      inputPlan.planNode()->outputType(),
      rowType,
      outputDirectory->getPath(),
      partitionKeys,
      nullptr,
      CompressionKind::CompressionKind_ZSTD);
  AssertQueryBuilder(plan)
      .splits(makeHiveConnectorSplits(inputFilePaths))
      .copyResults(pool());
  // Verify that there is one partition directory for each partition.
  std::set<std::string> actualPartitionDirectories =
      getLeafSubdirectories(outputDirectory->getPath());
  std::set<std::string> expectedPartitionDirectories;
  std::set<std::string> partitionNames;
  for (auto i = 0; i < numPartitions; i++) {
    auto partitionName = fmt::format("p0={}/p1=str_{}", i, i);
    partitionNames.emplace(partitionName);
    expectedPartitionDirectories.emplace(
        fs::path(outputDirectory->getPath()) / partitionName);
  }
  EXPECT_EQ(actualPartitionDirectories, expectedPartitionDirectories);

  const auto outputDirWithTracing = TempDirectoryPath::create();
  auto inputPlanWithTracing = PlanBuilder().tableScan(rowType);
  auto planWithTracing = createInsertPlan(
      inputPlanWithTracing,
      inputPlanWithTracing.planNode()->outputType(),
      rowType,
      outputDirWithTracing->getPath(),
      partitionKeys,
      nullptr,
      CompressionKind::CompressionKind_ZSTD);
  const auto testDir = TempDirectoryPath::create();
  const auto traceRoot = fmt::format("{}/{}", testDir->getPath(), "traceRoot");
  const auto tableWriteNodeId = std::move(tableWriteNodeId_);
  std::shared_ptr<Task> task;
  AssertQueryBuilder(planWithTracing)
      .config(core::QueryConfig::kQueryTraceEnabled, true)
      .config(core::QueryConfig::kQueryTraceDir, traceRoot)
      .config(core::QueryConfig::kQueryTraceMaxBytes, 100UL << 30)
      .config(core::QueryConfig::kQueryTraceTaskRegExp, ".*")
      .config(core::QueryConfig::kQueryTraceNodeId, tableWriteNodeId)
      .splits(makeHiveConnectorSplits(inputFilePaths))
      .copyResults(pool(), task);
  actualPartitionDirectories =
      getLeafSubdirectories(outputDirWithTracing->getPath());
  ASSERT_EQ(
      actualPartitionDirectories.size(), expectedPartitionDirectories.size());
  checkWriteResults(
      actualPartitionDirectories,
      expectedPartitionDirectories,
      partitionKeys,
      rowType);

  const auto traceOutputDir = TempDirectoryPath::create();
  TableWriterReplayer(
      traceRoot,
      task->queryCtx()->queryId(),
      task->taskId(),
      tableWriteNodeId,
      "TableWriter",
      "",
      0,
      executor_.get(),
      traceOutputDir->getPath())
      .run();
  actualPartitionDirectories = getLeafSubdirectories(traceOutputDir->getPath());
  checkWriteResults(
      actualPartitionDirectories,
      expectedPartitionDirectories,
      partitionKeys,
      rowType);
}

} // namespace facebook::velox::tool::trace::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  // Signal handler required for ThreadDebugInfoTest
  facebook::velox::process::addDefaultFatalSignalHandler();
  folly::Init init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
