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

add_executable(velox_gcs_file_test GcsUtilTest.cpp GcsFileSystemTest.cpp)
add_test(velox_gcs_file_test velox_gcs_file_test)
target_link_libraries(
  velox_gcs_file_test
  velox_core
  velox_dwio_common_exception
  velox_exec
  velox_exec_test_lib
  velox_file
  velox_gcs
  velox_hive_connector
  velox_temp_path
  GTest::gmock
  GTest::gtest
  GTest::gtest_main)

add_executable(velox_gcs_insert_test GcsInsertTest.cpp)
add_test(velox_gcs_insert_test velox_gcs_insert_test)
target_link_libraries(
  velox_gcs_insert_test
  velox_file
  velox_gcs
  velox_hive_config
  velox_core
  velox_exec_test_lib
  velox_dwio_common_exception
  velox_exec
  GTest::gmock
  GTest::gtest
  GTest::gtest_main)

add_executable(velox_gcs_multiendpoints_test GcsMultipleEndpointsTest.cpp)
add_test(velox_gcs_multiendpoints_test velox_gcs_multiendpoints_test)
target_link_libraries(
  velox_gcs_multiendpoints_test
  velox_file
  velox_gcs
  velox_hive_config
  velox_core
  velox_exec_test_lib
  velox_dwio_parquet_reader
  velox_dwio_parquet_writer
  velox_dwio_common_exception
  velox_exec
  GTest::gmock
  GTest::gtest
  GTest::gtest_main)
