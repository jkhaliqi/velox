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

// Adapted from Apache Arrow.

// Non-public Thrift schema serialization utilities

#pragma once

#include <memory>
#include <vector>

#include "velox/dwio/parquet/writer/arrow/Platform.h"
#include "velox/dwio/parquet/writer/arrow/Schema.h"
#include "velox/dwio/parquet/writer/arrow/Types.h"

namespace facebook::velox::parquet::arrow {

namespace format {
class SchemaElement;
}

namespace schema {

// ----------------------------------------------------------------------
// Conversion from Parquet Thrift metadata

PARQUET_EXPORT
std::shared_ptr<SchemaDescriptor> FromParquet(
    const std::vector<facebook::velox::parquet::thrift::SchemaElement>& schema);

PARQUET_EXPORT
std::unique_ptr<Node> Unflatten(
    const facebook::velox::parquet::thrift::SchemaElement* elements,
    int length);

// ----------------------------------------------------------------------
// Conversion to Parquet Thrift metadata

PARQUET_EXPORT
void ToParquet(
    const GroupNode* schema,
    std::vector<facebook::velox::parquet::thrift::SchemaElement>* out);

} // namespace schema
} // namespace facebook::velox::parquet::arrow
