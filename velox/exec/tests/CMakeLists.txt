# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
add_subdirectory(utils)

add_executable(
  aggregate_companion_functions_test
  AggregateCompanionAdapterTest.cpp AggregateCompanionSignaturesTest.cpp
  DummyAggregateFunction.cpp)

add_test(
  NAME aggregate_companion_functions_test
  COMMAND aggregate_companion_functions_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(
  aggregate_companion_functions_test
  velox_exec
  velox_function_registry
  velox_exec_test_lib
  velox_presto_types
  GTest::gtest
  GTest::gtest_main)

add_executable(
  velox_exec_test
  AddressableNonNullValueListTest.cpp
  AggregationTest.cpp
  AggregateFunctionRegistryTest.cpp
  ArrowStreamTest.cpp
  AssignUniqueIdTest.cpp
  AsyncConnectorTest.cpp
  ConcatFilesSpillMergeStreamTest.cpp
  ContainerRowSerdeTest.cpp
  CustomJoinTest.cpp
  EnforceSingleRowTest.cpp
  ExchangeClientTest.cpp
  ExpandTest.cpp
  FilterProjectTest.cpp
  FunctionResolutionTest.cpp
  HashBitRangeTest.cpp
  HashJoinBridgeTest.cpp
  HashJoinTest.cpp
  HashPartitionFunctionTest.cpp
  HashTableTest.cpp
  IndexLookupJoinTest.cpp
  LimitTest.cpp
  LocalPartitionTest.cpp
  Main.cpp
  MarkDistinctTest.cpp
  MemoryReclaimerTest.cpp
  MergeJoinTest.cpp
  MergeTest.cpp
  MergerTest.cpp
  MultiFragmentTest.cpp
  NestedLoopJoinTest.cpp
  OrderByTest.cpp
  OperatorTraceTest.cpp
  OutputBufferManagerTest.cpp
  ParallelProjectTest.cpp
  PartitionedOutputTest.cpp
  PlanNodeSerdeTest.cpp
  PlanNodeToStringTest.cpp
  PlanNodeToSummaryStringTest.cpp
  PrefixSortTest.cpp
  PrintPlanWithStatsTest.cpp
  ProbeOperatorStateTest.cpp
  TraceUtilTest.cpp
  RoundRobinPartitionFunctionTest.cpp
  RowContainerTest.cpp
  RowNumberTest.cpp
  ScaledScanControllerTest.cpp
  ScaleWriterLocalPartitionTest.cpp
  SortBufferTest.cpp
  SpillerTest.cpp
  SpillTest.cpp
  SplitListenerTest.cpp
  SplitTest.cpp
  SqlTest.cpp
  StreamingAggregationTest.cpp
  TableScanTest.cpp
  TableWriterTest.cpp
  TaskListenerTest.cpp
  ThreadDebugInfoTest.cpp
  TopNRowNumberTest.cpp
  TopNTest.cpp
  UnnestTest.cpp
  UnorderedStreamReaderTest.cpp
  ValuesTest.cpp
  VectorHasherTest.cpp
  WindowFunctionRegistryTest.cpp
  WindowTest.cpp
  WriterFuzzerUtilTest.cpp)

add_executable(
  velox_exec_infra_test
  AssertQueryBuilderTest.cpp
  DriverTest.cpp
  FunctionSignatureBuilderTest.cpp
  GroupedExecutionTest.cpp
  Main.cpp
  OperatorUtilsTest.cpp
  PlanBuilderTest.cpp
  PrestoQueryRunnerTest.cpp
  QueryAssertionsTest.cpp
  TaskTest.cpp
  TreeOfLosersTest.cpp)

add_test(
  NAME velox_exec_test
  COMMAND velox_exec_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

# TODO: Revert back to 3000 once is fixed.
# https://github.com/facebookincubator/velox/issues/13879
set_tests_properties(velox_exec_test PROPERTIES TIMEOUT 6000)

add_test(
  NAME velox_exec_infra_test
  COMMAND velox_exec_infra_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(
  velox_exec_test
  velox_aggregates
  velox_dwio_common
  velox_dwio_common_exception
  velox_dwio_common_test_utils
  velox_dwio_parquet_reader
  velox_dwio_parquet_writer
  velox_exec
  velox_exec_test_lib
  velox_file_test_utils
  velox_functions_json
  velox_functions_lib
  velox_functions_prestosql
  velox_functions_test_lib
  velox_hive_connector
  velox_memory
  velox_serialization
  velox_test_util
  velox_type
  velox_type_test_lib
  velox_vector
  velox_vector_fuzzer
  velox_writer_fuzzer
  velox_window
  Boost::atomic
  Boost::context
  Boost::date_time
  Boost::filesystem
  Boost::program_options
  Boost::regex
  Boost::thread
  Boost::system
  GTest::gtest
  GTest::gtest_main
  GTest::gmock
  Folly::folly
  gflags::gflags
  glog::glog
  fmt::fmt)

target_link_libraries(
  velox_exec_infra_test
  velox_dwio_common
  velox_dwio_common_exception
  velox_dwio_common_test_utils
  velox_exec
  velox_exec_test_lib
  velox_fuzzer_util
  velox_functions_lib
  velox_functions_prestosql
  velox_functions_test_lib
  velox_window
  velox_hive_connector
  velox_memory
  velox_serialization
  velox_test_util
  velox_type
  velox_vector
  velox_vector_fuzzer
  velox_presto_serializer
  velox_dwio_dwrf_writer
  Boost::atomic
  Boost::context
  Boost::date_time
  Boost::filesystem
  Boost::program_options
  Boost::regex
  Boost::thread
  Boost::system
  GTest::gtest
  GTest::gtest_main
  GTest::gmock
  Folly::folly
  gflags::gflags
  glog::glog
  fmt::fmt)

add_executable(
  velox_exec_util_test
  Main.cpp
  PrestoQueryRunnerHyperLogLogTransformTest.cpp
  PrestoQueryRunnerIntermediateTypeTransformTestBase.cpp
  PrestoQueryRunnerTDigestTransformTest.cpp
  PrestoQueryRunnerQDigestTransformTest.cpp
  PrestoQueryRunnerJsonTransformTest.cpp
  PrestoQueryRunnerIntervalTransformTest.cpp
  PrestoQueryRunnerTimestampWithTimeZoneTransformTest.cpp)

add_test(velox_exec_util_test velox_exec_util_test)

target_link_libraries(
  velox_exec_util_test
  velox_fuzzer_util
  velox_exec_test_lib
  velox_functions_test_lib
  velox_presto_types
  velox_presto_types_fuzzer_utils
  GTest::gtest
  GTest::gtest_main)

add_executable(velox_in_10_min_demo VeloxIn10MinDemo.cpp)

target_link_libraries(
  velox_in_10_min_demo
  velox_aggregates
  velox_type
  velox_vector
  velox_vector_test_lib
  velox_exec
  velox_exec_test_lib
  velox_tpch_connector
  velox_memory)

add_executable(velox_table_evolution_fuzzer_test TableEvolutionFuzzerTest.cpp
                                                 TableEvolutionFuzzer.cpp)

target_link_libraries(
  velox_table_evolution_fuzzer_test
  velox_exec_test_lib
  velox_temp_path
  velox_vector_fuzzer
  GTest::gtest
  GTest::gtest_main)

add_test(
  NAME velox_table_evolution_fuzzer_test
  COMMAND velox_table_evolution_fuzzer_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_executable(velox_aggregation_runner_test AggregationRunnerTest.cpp)

target_link_libraries(
  velox_aggregation_runner_test
  velox_aggregation_fuzzer
  velox_fuzzer_util
  velox_aggregates
  velox_vector_test_lib
  GTest::gtest
  GTest::gtest_main)

add_library(velox_simple_aggregate SimpleAverageAggregate.cpp
                                   SimpleArrayAggAggregate.cpp)

target_link_libraries(
  velox_simple_aggregate
  velox_exec
  velox_expression
  velox_expression_functions
  velox_aggregates)

add_executable(velox_simple_aggregate_test SimpleAggregateAdapterTest.cpp
                                           Main.cpp)

target_link_libraries(
  velox_simple_aggregate_test
  velox_simple_aggregate
  velox_exec
  velox_functions_aggregates_test_lib
  GTest::gtest)

add_test(velox_simple_aggregate_test velox_simple_aggregate_test)

if(VELOX_ENABLE_BENCHMARKS)
  add_library(velox_spiller_join_benchmark_base JoinSpillInputBenchmarkBase.cpp
                                                SpillerBenchmarkBase.cpp)
  target_link_libraries(
    velox_spiller_join_benchmark_base
    velox_exec
    velox_exec_test_lib
    velox_memory
    velox_vector_fuzzer
    glog::glog
    gflags::gflags
    Folly::folly
    pthread)

  add_executable(velox_spiller_join_benchmark SpillerJoinInputBenchmarkTest.cpp)
  target_link_libraries(velox_spiller_join_benchmark velox_exec
                        velox_exec_test_lib velox_spiller_join_benchmark_base)

  add_library(velox_spiller_aggregate_benchmark_base
              AggregateSpillBenchmarkBase.cpp SpillerBenchmarkBase.cpp)
  target_link_libraries(
    velox_spiller_aggregate_benchmark_base
    velox_exec
    velox_exec_test_lib
    velox_memory
    velox_vector_fuzzer
    glog::glog
    gflags::gflags
    Folly::folly
    pthread)

  add_executable(velox_spiller_aggregate_benchmark
                 SpillerAggregateBenchmarkTest.cpp)
  target_link_libraries(
    velox_spiller_aggregate_benchmark velox_exec velox_exec_test_lib
    velox_spiller_aggregate_benchmark_base)
endif()

add_executable(cpr_http_client_test CprHttpClientTest.cpp)
add_test(
  NAME cpr_http_client_test
  COMMAND cpr_http_client_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(
  cpr_http_client_test cpr::cpr GTest::gtest GTest::gtest_main)

add_executable(velox_driver_test OperatorReplacementTest.cpp Main.cpp)
add_test(
  NAME velox_driver_test
  COMMAND velox_driver_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(
  velox_driver_test velox_exec velox_exec_test_lib GTest::gtest)
