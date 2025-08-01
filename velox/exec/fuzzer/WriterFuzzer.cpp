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
#include "velox/exec/fuzzer/WriterFuzzer.h"

#include <boost/random/uniform_int_distribution.hpp>

#include <re2/re2.h>
#include <algorithm>
#include <unordered_set>
#include "velox/common/base/Fs.h"
#include "velox/common/encode/Base64.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/file/tests/FaultyFileSystem.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/exec/fuzzer/FuzzerUtil.h"
#include "velox/exec/fuzzer/PrestoQueryRunner.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/expression/fuzzer/FuzzerToolkit.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/vector/VectorSaver.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

DEFINE_bool(
    file_system_error_injection,
    true,
    "When enabled, inject file system write error with certain possibility");

DEFINE_int32(steps, 10, "Number of plans to generate and test.");

DEFINE_int32(
    duration_sec,
    0,
    "For how long it should run (in seconds). If zero, "
    "it executes exactly --steps iterations and exits.");

DEFINE_int32(
    batch_size,
    100,
    "The number of elements on each generated vector.");

DEFINE_int32(num_batches, 10, "The number of generated vectors.");

DEFINE_double(
    null_ratio,
    0.1,
    "Chance of adding a null value in a vector "
    "(expressed as double from 0 to 1).");

using namespace facebook::velox::connector::hive;
using namespace facebook::velox::test;

namespace facebook::velox::exec::test {

namespace {
using facebook::velox::filesystems::FileSystem;
using tests::utils::FaultFileOperation;
using tests::utils::FaultyFileSystem;

class WriterFuzzer {
 public:
  WriterFuzzer(
      size_t seed,
      std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner);

  void go();

 private:
  static VectorFuzzer::Options getFuzzerOptions() {
    VectorFuzzer::Options opts;
    opts.vectorSize = FLAGS_batch_size;
    opts.stringLength = 10;
    opts.nullRatio = FLAGS_null_ratio;
    opts.timestampPrecision =
        VectorFuzzer::Options::TimestampPrecision::kMilliSeconds;
    return opts;
  }

  void seed(size_t seed) {
    currentSeed_ = seed;
    vectorFuzzer_.reSeed(seed);
    rng_.seed(currentSeed_);
  }

  void reSeed() {
    seed(rng_());
  }

  // Generates at least one and up to maxNumColumns columns to be
  // used as columns of table write.
  // Column names are generated using template '<prefix>N', where N is
  // zero-based ordinal number of the column.
  // Data types is chosen from '<columnTypes>' and for nested complex data type,
  // maxDepth limits the max layers of nesting.
  // Offset represents the number of columns which has already been generated.
  // The function will generate the remaining columns starting from this index.
  std::vector<std::string> generateColumns(
      int32_t maxNumColumns,
      const std::string& prefix,
      const std::vector<TypePtr>& dataTypes,
      int32_t maxDepth,
      std::vector<std::string>& names,
      std::vector<TypePtr>& types,
      int32_t offset = 0);

  // Generates at least one and up to maxNumColumns columns
  // with a random number of those columns overlapping as bucket by columns.
  // Returns sorted column names and the start offset of generated sort columns.
  // The overlapped bucketed columns are listed first.
  std::tuple<std::vector<std::string>, int> generateSortColumns(
      int32_t maxNumColumns,
      const std::vector<std::string>& bucketColumns,
      std::vector<std::string>& names,
      std::vector<TypePtr>& types);

  // Generates input data for table write.
  std::vector<RowVectorPtr> generateInputData(
      std::vector<std::string> names,
      std::vector<TypePtr> types,
      size_t partitionOffset);

  void verifyWriter(
      const std::vector<RowVectorPtr>& input,
      const std::vector<std::string>& names,
      const std::vector<TypePtr>& types,
      int32_t partitionOffset,
      const std::vector<std::string>& partitionKeys,
      int32_t bucketCount,
      const std::vector<std::string>& bucketColumns,
      int32_t sortColumnOffset,
      const std::vector<std::shared_ptr<const HiveSortingColumn>>& sortBy,
      const std::shared_ptr<TempDirectoryPath>& outputDirectoryPath);

  // Generates table column handles based on table column properties
  connector::ColumnHandleMap getTableColumnHandles(
      const std::vector<std::string>& names,
      const std::vector<TypePtr>& types,
      int32_t partitionOffset,
      const int32_t bucketCount);

  // Executes velox query plan and returns the result.
  RowVectorPtr execute(
      const core::PlanNodePtr& plan,
      const int32_t maxDrivers = 2,
      const std::vector<exec::Split>& splits = {});

  RowVectorPtr veloxToPrestoResult(const RowVectorPtr& result);

  // Query Presto to find out table's location on disk.
  std::string getReferenceOutputDirectoryPath(int32_t layers);

  // Compares if two directories have same partitions and each partition has
  // same number of buckets.
  void comparePartitionAndBucket(
      const std::string& outputDirectoryPath,
      const std::string& referenceOutputDirectoryPath,
      int32_t bucketCount);

  // Returns all the partition name and how many files in each partition.
  std::map<std::string, int32_t> getPartitionNameAndFilecount(
      const std::string& tableDirectoryPath);

  // Generates output data type based on table column properties.
  RowTypePtr generateOutputType(
      const std::vector<std::string>& names,
      const std::vector<TypePtr>& types,
      const int32_t bucketCount);

  // Generates a sql that reads sorted columns from a single split of a bucketed
  // and sorted table.
  // For example, for a table sorted by age, reading a split that belongs to ds
  // = 2022-01-01 and bucket 1:
  // SELECT age FROM temp_write where ds = '2022-01-01' and "$bucket" = 1
  std::string sortSql(
      const std::shared_ptr<HiveConnectorSplit>& split,
      const std::vector<std::string>& names,
      const std::vector<TypePtr>& types,
      int32_t partitionOffset,
      const std::vector<std::string>& partitionKeys,
      const std::vector<std::shared_ptr<const HiveSortingColumn>>& sortBy);

  // When concatenating a partition value, if it's non-varchar, no change, eg:
  // age = 10
  // If it's varchar, we need to add single quote and also escape the single
  // quote in partition value, eg:
  // city = '''SF'''
  std::string partitionToSql(const TypePtr& type, std::string partitionValue);

  const std::vector<TypePtr> kRegularColumnTypes_{
      BOOLEAN(),
      TINYINT(),
      SMALLINT(),
      INTEGER(),
      BIGINT(),
      VARCHAR(),
      VARBINARY(),
      TIMESTAMP(),
  };

  // Supported sorted column types:
  const std::vector<TypePtr> kSupportedSortColumnTypes_{
      BOOLEAN(),
      TINYINT(),
      SMALLINT(),
      INTEGER(),
      BIGINT(),
      VARCHAR(),
      TIMESTAMP(),
  };

  // Supported order types:
  // https://github.com/prestodb/presto/blob/c542429ba989887de6208daaed4d7b4e34b49b3b/presto-hive-metastore/src/main/java/com/facebook/presto/hive/metastore/SortingColumn.java#L101
  // ASCENDING(ASC_NULLS_FIRST, 1),
  // DESCENDING(DESC_NULLS_LAST, 0);
  const std::vector<core::SortOrder> kSortOrderTypes_{
      core::SortOrder{true, true},
      core::SortOrder{false, false},
  };

  // Supported bucket column types:
  // https://github.com/prestodb/presto/blob/10143be627beb2c61aba5b3d36af473d2a8ef65e/presto-hive/src/main/java/com/facebook/presto/hive/HiveBucketing.java#L133
  const std::vector<TypePtr> kSupportedBucketColumnTypes_{
      BOOLEAN(),
      TINYINT(),
      SMALLINT(),
      INTEGER(),
      BIGINT(),
      VARCHAR(),
      TIMESTAMP(),
  };

  // Supported partition key column types
  // According to VectorHasher::typeKindSupportsValueIds and
  // https://github.com/prestodb/presto/blob/10143be627beb2c61aba5b3d36af473d2a8ef65e/presto-hive/src/main/java/com/facebook/presto/hive/HiveUtil.java#L593
  const std::vector<TypePtr> kPartitionKeyTypes_{
      BOOLEAN(),
      TINYINT(),
      SMALLINT(),
      INTEGER(),
      BIGINT(),
      VARCHAR(),
      TIMESTAMP()};

  const std::shared_ptr<FaultyFileSystem> faultyFs_ =
      std::dynamic_pointer_cast<FaultyFileSystem>(
          filesystems::getFileSystem("faulty:/tmp", {}));
  const std::string injectedErrorMsg_{"Injected Faulty File Error"};
  std::atomic<uint64_t> injectedErrorCount_{0};

  FuzzerGenerator rng_;
  size_t currentSeed_{0};
  std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner_;
  std::shared_ptr<memory::MemoryPool> rootPool_{
      memory::memoryManager()->addRootPool()};
  std::shared_ptr<memory::MemoryPool> pool_{rootPool_->addLeafChild("leaf")};
  std::shared_ptr<memory::MemoryPool> writerPool_{
      rootPool_->addAggregateChild("writerFuzzerWriter")};
  VectorFuzzer vectorFuzzer_;
};
} // namespace

void writerFuzzer(
    size_t seed,
    std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner) {
  auto writerFuzzer = WriterFuzzer(seed, std::move(referenceQueryRunner));
  writerFuzzer.go();
}

std::vector<std::string> listFolders(std::string_view path) {
  std::vector<std::string> folders;
  auto fileSystem = filesystems::getFileSystem("/", nullptr);
  for (auto& p : std::filesystem::recursive_directory_iterator(
           fileSystem->extractPath(path))) {
    if (p.is_directory())
      folders.push_back(p.path().string());
  }
  return folders;
}

namespace {
template <typename T>
bool isDone(size_t i, T startTime) {
  if (FLAGS_duration_sec > 0) {
    std::chrono::duration<double> elapsed =
        std::chrono::system_clock::now() - startTime;
    return elapsed.count() >= FLAGS_duration_sec;
  }
  return i >= FLAGS_steps;
}

WriterFuzzer::WriterFuzzer(
    size_t initialSeed,
    std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner)
    : referenceQueryRunner_{std::move(referenceQueryRunner)},
      vectorFuzzer_{getFuzzerOptions(), pool_.get()} {
  seed(initialSeed);
}

void WriterFuzzer::go() {
  VELOX_CHECK(
      FLAGS_steps > 0 || FLAGS_duration_sec > 0,
      "Either --steps or --duration_sec needs to be greater than zero.");

  auto startTime = std::chrono::system_clock::now();
  size_t iteration = 0;

  // Faulty fs will generate file system write error with certain possibility
  if (FLAGS_file_system_error_injection) {
    faultyFs_->setFileInjectionHook([&](FaultFileOperation* op) {
      if (vectorFuzzer_.coinToss(0.01)) {
        ++injectedErrorCount_;
        VELOX_FAIL(injectedErrorMsg_);
      }
    });
  }

  while (!isDone(iteration, startTime)) {
    LOG(INFO) << "==============================> Started iteration "
              << iteration << " (seed: " << currentSeed_ << ")";

    std::vector<std::string> names;
    std::vector<TypePtr> types;
    int32_t partitionOffset = 0;
    std::vector<std::string> partitionKeys;
    int32_t bucketCount = 0;
    std::vector<std::string> bucketColumns;
    int32_t sortColumnOffset = 0;
    std::vector<std::shared_ptr<const HiveSortingColumn>> sortBy;

    // Regular table columns
    generateColumns(5, "c", kRegularColumnTypes_, 2, names, types);

    // 50% of times test partitioned write.
    if (vectorFuzzer_.coinToss(0.5)) {
      // 50% of times test bucketed write.
      if (vectorFuzzer_.coinToss(0.5)) {
        bucketColumns = generateColumns(
            5, "b", kSupportedBucketColumnTypes_, 1, names, types);
        bucketCount =
            boost::random::uniform_int_distribution<int32_t>(1, 3)(rng_);

        // 50% of times test ordered write.
        if (vectorFuzzer_.coinToss(0.5)) {
          sortColumnOffset = names.size();
          auto [sortColumns, offset] =
              generateSortColumns(3, bucketColumns, names, types);
          sortColumnOffset -= offset;
          sortBy.reserve(sortColumns.size());
          for (const auto& sortByColumn : sortColumns) {
            sortBy.push_back(std::make_shared<const HiveSortingColumn>(
                sortByColumn,
                kSortOrderTypes_.at(
                    boost::random::uniform_int_distribution<uint32_t>(
                        0, 1)(rng_))));
          }
        }
      }

      partitionOffset = names.size();
      partitionKeys =
          generateColumns(3, "p", kPartitionKeyTypes_, 1, names, types);
    }
    auto input = generateInputData(names, types, partitionOffset);

    const auto outputDirPath = exec::test::TempDirectoryPath::create(
        FLAGS_file_system_error_injection);

    verifyWriter(
        input,
        names,
        types,
        partitionOffset,
        partitionKeys,
        bucketCount,
        bucketColumns,
        sortColumnOffset,
        sortBy,
        outputDirPath);

    LOG(INFO) << "==============================> Done with iteration "
              << iteration++;
    reSeed();
  }
}

std::vector<std::string> WriterFuzzer::generateColumns(
    int32_t maxNumColumns,
    const std::string& prefix,
    const std::vector<TypePtr>& dataTypes,
    int32_t maxDepth,
    std::vector<std::string>& names,
    std::vector<TypePtr>& types,
    const int32_t offset) {
  const auto numColumns =
      boost::random::uniform_int_distribution<uint32_t>(1, maxNumColumns)(rng_);
  std::vector<std::string> columns;
  for (auto i = offset; i < numColumns; ++i) {
    columns.push_back(fmt::format("{}{}", prefix, i));

    // Pick random, possibly complex, type.
    types.push_back(vectorFuzzer_.randType(dataTypes, maxDepth));
    names.push_back(columns.back());
  }
  return columns;
}

std::tuple<std::vector<std::string>, int> WriterFuzzer::generateSortColumns(
    int32_t maxNumColumns,
    const std::vector<std::string>& bucketColumns,
    std::vector<std::string>& names,
    std::vector<TypePtr>& types) {
  // A random number of sort columns will overlap as bucket columns, which are
  // already generated
  const auto maxOverlapColumns = std::min<int32_t>(
      maxNumColumns, static_cast<int32_t>(bucketColumns.size()));
  const auto numOverlapColumns =
      static_cast<int32_t>(boost::random::uniform_int_distribution<uint32_t>(
          0, maxOverlapColumns)(rng_));

  std::vector<std::string> columns(
      bucketColumns.end() - numOverlapColumns, bucketColumns.end());

  // Remaining columns which do not overlap as bucket by columns are added as
  // new columns with prefix "s"
  const auto remainingColumns = maxNumColumns - numOverlapColumns;
  if (remainingColumns > 0) {
    auto nonOverlapColumns = generateColumns(
        remainingColumns,
        "s",
        kSupportedSortColumnTypes_,
        1,
        names,
        types,
        numOverlapColumns);
    columns.insert(
        columns.end(), nonOverlapColumns.begin(), nonOverlapColumns.end());
  }

  return {columns, numOverlapColumns};
}

std::vector<RowVectorPtr> WriterFuzzer::generateInputData(
    std::vector<std::string> names,
    std::vector<TypePtr> types,
    size_t partitionOffset) {
  const auto size = vectorFuzzer_.getOptions().vectorSize;
  auto inputType = ROW(std::move(names), std::move(types));
  std::vector<RowVectorPtr> input;

  // For partition keys, limit the distinct value to 4. Since we could have up
  // to 3 partition keys, it would generate up to 64 partitions.
  std::vector<VectorPtr> partitionValues;
  for (auto i = partitionOffset; i < inputType->size(); ++i) {
    partitionValues.push_back(vectorFuzzer_.fuzz(inputType->childAt(i), 4));
  }

  for (auto i = 0; i < FLAGS_num_batches; ++i) {
    std::vector<VectorPtr> children;
    for (auto j = children.size(); j < inputType->size(); ++j) {
      if (j < partitionOffset) {
        // For regular columns.
        children.push_back(vectorFuzzer_.fuzz(inputType->childAt(j), size));
      } else {
        // TODO Add other encoding support here besides DictionaryVector.
        children.push_back(vectorFuzzer_.fuzzDictionary(
            partitionValues.at(j - partitionOffset), size));
      }
    }
    input.push_back(std::make_shared<RowVector>(
        pool_.get(), inputType, nullptr, size, std::move(children)));
  }

  return input;
}

void WriterFuzzer::verifyWriter(
    const std::vector<RowVectorPtr>& input,
    const std::vector<std::string>& names,
    const std::vector<TypePtr>& types,
    const int32_t partitionOffset,
    const std::vector<std::string>& partitionKeys,
    const int32_t bucketCount,
    const std::vector<std::string>& bucketColumns,
    const int32_t sortColumnOffset,
    const std::vector<std::shared_ptr<const HiveSortingColumn>>& sortBy,
    const std::shared_ptr<TempDirectoryPath>& outputDirectoryPath) {
  const auto plan = PlanBuilder()
                        .values(input)
                        .tableWrite(
                            outputDirectoryPath->getPath(),
                            partitionKeys,
                            bucketCount,
                            bucketColumns,
                            sortBy)
                        .planNode();

  const auto maxDrivers =
      boost::random::uniform_int_distribution<int32_t>(1, 16)(rng_);
  RowVectorPtr result;
  const uint64_t prevInjectedErrorCount = injectedErrorCount_;
  try {
    result = veloxToPrestoResult(execute(plan, maxDrivers));
  } catch (VeloxRuntimeError& error) {
    if (injectedErrorCount_ == prevInjectedErrorCount) {
      throw error;
    }
    VELOX_CHECK_GT(
        injectedErrorCount_,
        prevInjectedErrorCount,
        "Unexpected writer fuzzer failure: {}",
        error.message());
    VELOX_CHECK_EQ(
        error.message(), injectedErrorMsg_, "Unexpected writer fuzzer failure");
    return;
  }

  try {
    referenceQueryRunner_->execute("DROP TABLE IF EXISTS tmp_write");
  } catch (...) {
    LOG(WARNING) << "Drop table query failed in the reference DB";
    return;
  }
  auto expectedResult = referenceQueryRunner_->execute(plan).first;
  if (!expectedResult.has_value()) {
    return;
  }

  // 1. Verifies the table writer output result: the inserted number of rows.
  VELOX_CHECK_EQ(
      expectedResult->size(), // Presto sql only produces one row which is
                              // how many rows are inserted.
      1,
      "Query returned unexpected result in the reference DB");
  VELOX_CHECK(
      assertEqualResults(*expectedResult, plan->outputType(), {result}),
      "Velox and reference DB results don't match");

  // 2. Verifies directory layout for partitioned (bucketed) table.
  if (!partitionKeys.empty()) {
    const auto referencedOutputDirectoryPath =
        getReferenceOutputDirectoryPath(partitionKeys.size());
    comparePartitionAndBucket(
        outputDirectoryPath->getDelegatePath(),
        referencedOutputDirectoryPath,
        bucketCount);
  }

  // 3. Verifies data itself.
  auto splits = makeSplits(outputDirectoryPath->getDelegatePath());
  auto columnHandles =
      getTableColumnHandles(names, types, partitionOffset, bucketCount);
  const auto rowType = generateOutputType(names, types, bucketCount);

  auto readPlan = PlanBuilder()
                      .tableScan(rowType, {}, "", rowType, columnHandles)
                      .planNode();
  auto actual = execute(readPlan, maxDrivers, splits);
  std::string bucketSql;
  if (bucketCount > 0) {
    bucketSql = ", \"$bucket\"";
  }
  try {
    auto referenceData = referenceQueryRunner_->execute(
        "SELECT *" + bucketSql + " FROM tmp_write");
    VELOX_CHECK(
        assertEqualResults(referenceData, {actual}),
        "Velox and reference DB results don't match");
  } catch (...) {
    LOG(WARNING) << "Query failed in the reference DB";
    return;
  }

  // 4. Verifies sorting.
  if (sortBy.size() > 0) {
    const std::vector<std::string> sortColumnNames = {
        names.begin() + sortColumnOffset,
        names.begin() + sortColumnOffset + sortBy.size()};
    const std::vector<TypePtr> sortColumnTypes = {
        types.begin() + sortColumnOffset,
        types.begin() + sortColumnOffset + sortBy.size()};

    // Read from each file and check if data is sorted as presto sorted
    // result.
    for (const auto& split : splits) {
      auto splitReadPlan = PlanBuilder()
                               .tableScan(generateOutputType(
                                   sortColumnNames, sortColumnTypes, 0))
                               .planNode();
      auto singleSplitData = execute(splitReadPlan, 1, {split});

      auto const singleSplitReferenceSql = sortSql(
          std::dynamic_pointer_cast<HiveConnectorSplit>(split.connectorSplit),
          names,
          types,
          partitionOffset,
          partitionKeys,
          sortBy);

      std::vector<velox::RowVectorPtr> referenceResult;
      try {
        referenceResult = referenceQueryRunner_->execute(
            singleSplitReferenceSql, "task_concurrency=1");
      } catch (...) {
        LOG(WARNING) << "Query failed in the reference DB";
        return;
      }
      const auto& referenceData = referenceResult.at(0);
      for (int i = 1; i < referenceResult.size(); ++i) {
        referenceData->append(referenceResult.at(i).get());
      }
      fuzzer::compareVectors(
          singleSplitData, referenceData, "velox", "prestoDB");
      LOG(INFO) << "Sort Verification succeed for split: "
                << split.connectorSplit->toString();
    }
  }

  LOG(INFO) << "Verified results against reference DB";
}

connector::ColumnHandleMap WriterFuzzer::getTableColumnHandles(
    const std::vector<std::string>& names,
    const std::vector<TypePtr>& types,
    const int32_t partitionOffset,
    const int32_t bucketCount) {
  connector::ColumnHandleMap columnHandle;
  for (int i = 0; i < names.size(); ++i) {
    HiveColumnHandle::ColumnType columnType;
    if (i < partitionOffset) {
      columnType = HiveColumnHandle::ColumnType::kRegular;
    } else {
      columnType = HiveColumnHandle::ColumnType::kPartitionKey;
    }
    columnHandle.insert(
        {names.at(i),
         std::make_shared<HiveColumnHandle>(
             names.at(i), columnType, types.at(i), types.at(i))});
  }
  // If table is bucketed, add synthesized $bucket column.
  if (bucketCount > 0) {
    columnHandle.insert(
        {"$bucket",
         std::make_shared<HiveColumnHandle>(
             "$bucket",
             HiveColumnHandle::ColumnType::kSynthesized,
             INTEGER(),
             INTEGER())});
  }
  return columnHandle;
}

RowVectorPtr WriterFuzzer::execute(
    const core::PlanNodePtr& plan,
    const int32_t maxDrivers,
    const std::vector<exec::Split>& splits) {
  LOG(INFO) << "Executing query plan: " << std::endl
            << plan->toString(true, true);
  fuzzer::ResultOrError resultOrError;
  AssertQueryBuilder builder(plan);
  if (!splits.empty()) {
    builder.splits(splits);
  }
  return builder.maxDrivers(maxDrivers)
      .connectorSessionProperty(
          kHiveConnectorId,
          connector::hive::HiveConfig::kMaxPartitionsPerWritersSession,
          "400")
      .copyResults(pool_.get());
}

RowVectorPtr WriterFuzzer::veloxToPrestoResult(const RowVectorPtr& result) {
  // Velox TableWrite node produces results of following layout
  // row     fragments     context
  // X         null          X
  // null       X            X
  // null       X            X
  // Extract inserted rows from velox execution result.
  std::vector<VectorPtr> insertedRows = {result->childAt(0)->slice(0, 1)};
  return std::make_shared<RowVector>(
      pool_.get(),
      ROW({"count"}, {insertedRows[0]->type()}),
      nullptr,
      1,
      insertedRows);
}

std::string WriterFuzzer::getReferenceOutputDirectoryPath(int32_t layers) {
  auto filePath =
      referenceQueryRunner_->execute("SELECT \"$path\" FROM tmp_write");
  auto tableDirectoryPath =
      fs::path(extractSingleValue<StringView>(filePath)).parent_path();
  while (layers-- > 0) {
    tableDirectoryPath = tableDirectoryPath.parent_path();
  }
  return tableDirectoryPath.string();
}

void WriterFuzzer::comparePartitionAndBucket(
    const std::string& outputDirectoryPath,
    const std::string& referenceOutputDirectoryPath,
    int32_t bucketCount) {
  LOG(INFO) << "Velox output directory:" << outputDirectoryPath << std::endl;
  const auto partitionNameAndFileCount =
      getPartitionNameAndFilecount(outputDirectoryPath);
  LOG(INFO) << "Partitions and file count:" << std::endl;
  std::vector<std::string> partitionNames;
  partitionNames.reserve(partitionNameAndFileCount.size());
  for (const auto& i : partitionNameAndFileCount) {
    LOG(INFO) << i.first << ":" << i.second << std::endl;
    partitionNames.emplace_back(i.first);
  }

  LOG(INFO) << "Presto output directory:" << referenceOutputDirectoryPath
            << std::endl;
  const auto referencedPartitionNameAndFileCount =
      getPartitionNameAndFilecount(referenceOutputDirectoryPath);
  LOG(INFO) << "Partitions and file count:" << std::endl;
  std::vector<std::string> referencePartitionNames;
  referencePartitionNames.reserve(referencedPartitionNameAndFileCount.size());
  for (const auto& i : referencedPartitionNameAndFileCount) {
    LOG(INFO) << i.first << ":" << i.second << std::endl;
    referencePartitionNames.emplace_back(i.first);
  }

  if (bucketCount == 0) {
    // If not bucketed, only verify if their partition names match
    VELOX_CHECK(
        partitionNames == referencePartitionNames,
        "Velox and reference DB output partitions don't match. Velox: [{}], Presto: [{}]",
        fmt::join(partitionNames, ", "),
        fmt::join(referencePartitionNames, ", "));
  } else if (partitionNameAndFileCount != referencedPartitionNameAndFileCount) {
    std::vector<std::string> partitionNameAndFileCountStrs;
    std::vector<std::string> referencedPartitionNameAndFileCountStrs;

    partitionNameAndFileCountStrs.reserve(partitionNameAndFileCount.size());
    referencedPartitionNameAndFileCountStrs.reserve(
        referencedPartitionNameAndFileCount.size());

    for (const auto& p : partitionNameAndFileCount) {
      partitionNameAndFileCountStrs.push_back(
          fmt::format("'{}': {}", p.first, p.second));
    }
    for (const auto& p : referencedPartitionNameAndFileCount) {
      referencedPartitionNameAndFileCountStrs.push_back(
          fmt::format("'{}': {}", p.first, p.second));
    }

    VELOX_FAIL(
        "Velox and reference DB output partition and bucket don't match. Velox: {{{}}}, Presto: {{{}}}",
        fmt::join(partitionNameAndFileCountStrs, ", "),
        fmt::join(referencedPartitionNameAndFileCountStrs, ", "));
  }
}

// static
std::map<std::string, int32_t> WriterFuzzer::getPartitionNameAndFilecount(
    const std::string& tableDirectoryPath) {
  auto fileSystem = filesystems::getFileSystem("/", nullptr);
  auto directories = listFolders(tableDirectoryPath);
  std::map<std::string, int32_t> partitionNameAndFileCount;

  for (std::string directory : directories) {
    // If it's a hidden directory, ignore
    if (directory.find("/.") != std::string::npos) {
      continue;
    }

    // Count non-empty non-hidden files
    const auto files = fileSystem->list(directory);
    int32_t fileCount = 0;
    for (const auto& file : files) {
      // Presto query runner sometime creates empty files, ignore those.
      if (file.find("/.") == std::string::npos &&
          fileSystem->openFileForRead(file)->size() > 0) {
        fileCount++;
      }
    }

    // Remove the path prefix to get the partition name
    // For example: /test/tmp_write/p0=1/p1=2020
    // partition name is /p0=1/p1=2020
    directory.erase(0, fileSystem->extractPath(tableDirectoryPath).length());

    partitionNameAndFileCount.emplace(directory, fileCount);
  }

  return partitionNameAndFileCount;
}

RowTypePtr WriterFuzzer::generateOutputType(
    const std::vector<std::string>& names,
    const std::vector<TypePtr>& types,
    const int32_t bucketCount) {
  std::vector<std::string> outputNames{names};
  std::vector<TypePtr> outputTypes;
  for (auto type : types) {
    outputTypes.emplace_back(type);
  }
  if (bucketCount > 0) {
    outputNames.emplace_back("$bucket");
    outputTypes.emplace_back(INTEGER());
  }

  return {ROW(std::move(outputNames), std::move(outputTypes))};
}

std::string WriterFuzzer::sortSql(
    const std::shared_ptr<HiveConnectorSplit>& split,
    const std::vector<std::string>& names,
    const std::vector<TypePtr>& types,
    int32_t partitionOffset,
    const std::vector<std::string>& partitionKeys,
    const std::vector<std::shared_ptr<const HiveSortingColumn>>& sortBy) {
  // For a split, extract the partition filters and bucket filters.
  std::stringstream whereSql;
  whereSql << "WHERE ";
  for (int i = partitionOffset; i < partitionOffset + partitionKeys.size();
       ++i) {
    const auto& partitionKey = names.at(i);
    auto partitionValue = split->partitionKeys.at(partitionKey);
    if (partitionValue.has_value()) {
      whereSql << partitionKey << " = "
               << partitionToSql(types.at(i), partitionValue.value());
    } else {
      whereSql << partitionKey << " IS NULL";
    }
    whereSql << " AND ";
  }
  whereSql << "\"$bucket\" = " << split->tableBucketNumber.value();

  std::stringstream selectedColumns;
  for (int i = 0; i < sortBy.size(); ++i) {
    if (i > 0) {
      selectedColumns << ", ";
    }
    selectedColumns << sortBy.at(i)->sortColumn();
  }
  return "SELECT " + selectedColumns.str() + " FROM tmp_write " +
      whereSql.str();
}

std::string WriterFuzzer::partitionToSql(
    const TypePtr& type,
    std::string partitionValue) {
  if (type->isVarchar()) {
    RE2::GlobalReplace(&partitionValue, "'", "''");
    return "'" + partitionValue + "'";
  }
  return partitionValue;
}

} // namespace
} // namespace facebook::velox::exec::test
