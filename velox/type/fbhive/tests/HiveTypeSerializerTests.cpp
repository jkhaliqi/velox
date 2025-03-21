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

#include <memory>
#include <stdexcept>

#include "gtest/gtest-message.h"
#include "gtest/gtest-test-part.h"
#include "gtest/gtest.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/type/fbhive/HiveTypeSerializer.h"

using facebook::velox::TypeKind;

namespace facebook::velox::type::fbhive {

TEST(HiveTypeSerializer, primitive) {
  std::shared_ptr<const velox::Type> type = velox::BIGINT();
  auto result = HiveTypeSerializer::serialize(type);
  EXPECT_EQ(result, "bigint");
}

struct Foo {};
TEST(HiveTypeSerializer, opaque) {
  // Use a custom name to highlight this is just an alias.
  registerOpaqueType<Foo>("bar");

  std::shared_ptr<const Type> type = OPAQUE<Foo>();
  auto result = HiveTypeSerializer::serialize(type);
  EXPECT_EQ(result, "opaque<bar>");
  unregisterOpaqueType<Foo>("bar");
}

TEST(HiveTypeSerializer, unregisteredOpaque) {
  // did not call registerOpaqueType<Foo>("Foo")
  std::shared_ptr<const Type> type = OPAQUE<Foo>();
  VELOX_ASSERT_THROW(
      HiveTypeSerializer::serialize(type),
      fmt::format(
          "Could not find type index '{}'. Did you call registerOpaqueType?",
          type->asOpaque().typeIndex().name()));
}

TEST(HiveTypeSerializer, unsupported) {
  std::shared_ptr<const Type> type = UNKNOWN();
  VELOX_ASSERT_THROW(
      HiveTypeSerializer::serialize(type), "unsupported type: UNKNOWN");
}
} // namespace facebook::velox::type::fbhive
