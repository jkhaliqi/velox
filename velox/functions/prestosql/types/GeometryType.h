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

#include "velox/expression/CastExpr.h"
#include "velox/type/SimpleFunctionApi.h"
#include "velox/type/Type.h"

namespace facebook::velox {

class GeometryType : public VarbinaryType {
  GeometryType() = default;

 public:
  static const std::shared_ptr<const GeometryType>& get() {
    static const std::shared_ptr<const GeometryType> instance{
        new GeometryType()};
    return instance;
  }

  bool equivalent(const Type& other) const override {
    // Pointer comparison works since this type is a singleton.
    return this == &other;
  }

  const char* name() const override {
    return "GEOMETRY";
  }

  std::string toString() const override {
    return name();
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = "Type";
    obj["type"] = name();
    return obj;
  }

  bool isOrderable() const override {
    return false;
  }
};

FOLLY_ALWAYS_INLINE bool isGeometryType(const TypePtr& type) {
  // Pointer comparison works since this type is a singleton.
  return GeometryType::get() == type;
}

FOLLY_ALWAYS_INLINE std::shared_ptr<const GeometryType> GEOMETRY() {
  return GeometryType::get();
}

// Type used for function registration.
struct GeometryT {
  using type = Varbinary;
  static constexpr const char* typeName = "geometry";
};

using Geometry = CustomType<GeometryT>;

} // namespace facebook::velox
