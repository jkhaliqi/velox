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
#include "velox/expression/Expr.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/lib/LambdaFunctionUtil.h"
#include "velox/vector/FunctionVector.h"

namespace facebook::velox::functions {
namespace {

// See documentation at https://prestodb.io/docs/current/functions/map.html
// map_top_n_keys(x(K,V), n, (K, K) -> int) -> array(K)
// Returns the top N keys of the given map sorting its keys using the provided
// lambda comparator.
class MapTopNKeysLambdaFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VELOX_CHECK_EQ(args.size(), 3);

    // Flatten input map.
    exec::LocalDecodedVector mapDecoder(context, *args[0], rows);
    auto& decodedMap = *mapDecoder.get();
    auto flatMap = flattenMap(rows, args[0], decodedMap);
    VELOX_CHECK_NOT_NULL(flatMap);

    auto mapKeys = flatMap->mapKeys();
    auto numKeys = mapKeys->size();

    // Decode the n parameter.
    exec::LocalDecodedVector nDecoder(context, *args[1], rows);
    auto& decodedN = *nDecoder.get();

    // Validate n values.
    rows.applyToSelected([&](vector_size_t row) {
      if (!decodedMap.isNullAt(row) && !decodedN.isNullAt(row)) {
        int64_t n = decodedN.valueAt<int64_t>(row);
        VELOX_USER_CHECK_GE(n, 0, "n must be greater than or equal to 0");
      }
    });

    // Create pairs of keys for comparison: for each map, generate all pairs
    // (key[i], key[j]) where i < j, then evaluate the comparator lambda.
    // This approach evaluates the lambda once per pair instead of multiple
    // times during sorting.

    // For now, use a simpler approach similar to ArraySortLambdaFunction:
    // Create a vector with all keys and use FunctionVector iterator.

    // Calculate total number of key pairs needed for comparison.
    vector_size_t totalPairs = 0;
    rows.applyToSelected([&](vector_size_t row) {
      if (!decodedMap.isNullAt(row)) {
        auto mapIndex = decodedMap.index(row);
        auto mapSize = flatMap->sizeAt(mapIndex);
        // For sorting, we need to compare pairs, but we'll use a different
        // approach: evaluate comparator for each key to get a sort key.
        totalPairs += mapSize;
      }
    });

    // Build indices buffer for sorting.
    BufferPtr indices = allocateIndices(numKeys, context.pool());
    auto* rawIndices = indices->asMutable<vector_size_t>();
    for (vector_size_t i = 0; i < numKeys; ++i) {
      rawIndices[i] = i;
    }

    // Loop over lambda functions and apply comparator.
    // In most cases there will be only one function and the loop will run once.
    auto it = args[2]->asUnchecked<FunctionVector>()->iterator(&rows);
    while (auto entry = it.next()) {
      // For each map, sort its keys using the comparator.
      entry.rows->applyToSelected([&](vector_size_t row) {
        if (decodedMap.isNullAt(row)) {
          return;
        }

        auto mapIndex = decodedMap.index(row);
        auto mapOffset = flatMap->offsetAt(mapIndex);
        auto mapSize = flatMap->sizeAt(mapIndex);

        if (mapSize == 0) {
          return;
        }

        // Sort keys using the lambda comparator.
        // The lambda returns: negative if a < b, 0 if equal, positive if a > b.
        // We want descending order (top N), so reverse the comparison.
        std::sort(
            rawIndices + mapOffset,
            rawIndices + mapOffset + mapSize,
            [&](vector_size_t leftIdx, vector_size_t rightIdx) {
              if (leftIdx == rightIdx) {
                return false;
              }

              // Create single-row vectors for the lambda inputs.
              auto leftKey = BaseVector::wrapInConstant(1, leftIdx, mapKeys);
              auto rightKey = BaseVector::wrapInConstant(1, rightIdx, mapKeys);

              std::vector<VectorPtr> lambdaArgs = {leftKey, rightKey};
              SelectivityVector singleRow(1);

              VectorPtr lambdaResult;
              exec::EvalErrorsPtr elementErrors;
              entry.callable->applyNoThrow(
                  singleRow,
                  nullptr,
                  nullptr,
                  &context,
                  lambdaArgs,
                  elementErrors,
                  &lambdaResult);

              // The comparator returns an integer.
              auto resultValue =
                  lambdaResult->as<SimpleVector<int64_t>>()->valueAt(0);
              // For descending order (top N), reverse the comparison.
              return resultValue > 0;
            });
      });
    }

    // Build result array with top N keys.
    vector_size_t totalElements = 0;
    rows.applyToSelected([&](vector_size_t row) {
      if (decodedMap.isNullAt(row) || decodedN.isNullAt(row)) {
        return;
      }

      auto mapIndex = decodedMap.index(row);
      auto mapSize = flatMap->sizeAt(mapIndex);
      int64_t n = decodedN.valueAt<int64_t>(row);

      auto resultSize = std::min(static_cast<vector_size_t>(n), mapSize);
      totalElements += resultSize;
    });

    // Create result array vector.
    auto elements = BaseVector::create(
        outputType->asArray().elementType(), totalElements, context.pool());

    auto arrayVector = std::make_shared<ArrayVector>(
        context.pool(),
        outputType,
        nullptr,
        rows.end(),
        allocateOffsets(rows.end(), context.pool()),
        allocateSizes(rows.end(), context.pool()),
        elements);

    auto* rawOffsets =
        arrayVector->mutableOffsets(rows.end())->asMutable<vector_size_t>();
    auto* rawSizes =
        arrayVector->mutableSizes(rows.end())->asMutable<vector_size_t>();

    vector_size_t elemIdx = 0;
    rows.applyToSelected([&](vector_size_t row) {
      if (decodedMap.isNullAt(row) || decodedN.isNullAt(row)) {
        arrayVector->setNull(row, true);
        rawOffsets[row] = elemIdx;
        rawSizes[row] = 0;
        return;
      }

      auto mapIndex = decodedMap.index(row);
      auto mapOffset = flatMap->offsetAt(mapIndex);
      auto mapSize = flatMap->sizeAt(mapIndex);
      int64_t n = decodedN.valueAt<int64_t>(row);

      auto resultSize = std::min(static_cast<vector_size_t>(n), mapSize);
      rawOffsets[row] = elemIdx;
      rawSizes[row] = resultSize;

      // Copy top N sorted keys to result.
      for (vector_size_t i = 0; i < resultSize; ++i) {
        elements->copy(mapKeys.get(), elemIdx++, rawIndices[mapOffset + i], 1);
      }
    });

    context.moveOrCopyResult(arrayVector, rows, result);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // map_top_n_keys(map(K,V), bigint, function(K,K,bigint)) -> array(K)
    return {exec::FunctionSignatureBuilder()
                .typeVariable("K")
                .typeVariable("V")
                .returnType("array(K)")
                .argumentType("map(K,V)")
                .argumentType("bigint")
                .argumentType("function(K,K,bigint)")
                .build()};
  }
};
} // namespace

VELOX_DECLARE_VECTOR_FUNCTION_WITH_METADATA(
    udf_map_top_n_keys,
    MapTopNKeysLambdaFunction::signatures(),
    exec::VectorFunctionMetadataBuilder().defaultNullBehavior(false).build(),
    std::make_unique<MapTopNKeysLambdaFunction>());

} // namespace facebook::velox::functions
