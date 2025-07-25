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
velox_add_library(
  velox_presto_types_fuzzer_utils TimestampWithTimeZoneInputGenerator.cpp
  HyperLogLogInputGenerator.cpp SfmSketchInputGenerator.cpp)

velox_link_libraries(
  velox_presto_types_fuzzer_utils
  velox_type
  velox_common_fuzzer_util
  velox_presto_types
  velox_type_tz
  velox_functions_prestosql_aggregates_sfm)

if(${VELOX_BUILD_TESTING})
  add_subdirectory(tests)
endif()
