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

add_library(
  velox_wave_common
  GpuArena.cpp
  Buffer.cpp
  Compile.cu
  Cuda.cu
  Exception.cpp
  KernelCache.cpp
  Type.cpp
  ResultStaging.cpp)

target_link_libraries(
  velox_wave_common
  velox_exception
  velox_common_base
  velox_type
  Folly::folly
  CUDA::cuda_driver
  CUDA::nvrtc
  CUDA::cudart
  breeze_cuda)

target_compile_definitions(velox_wave_common PRIVATE VELOX_OSS_BUILD=1)

if(${VELOX_BUILD_TESTING})
  add_subdirectory(tests)
endif()
