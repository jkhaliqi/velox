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
#include "velox/functions/Registerer.h"
#include "velox/functions/lib/Re2Functions.h"
#include "velox/functions/prestosql/RegexpReplace.h"
#include "velox/functions/prestosql/RegexpSplit.h"
#include "velox/functions/prestosql/SplitPart.h"
#include "velox/functions/prestosql/SplitToMap.h"
#include "velox/functions/prestosql/SplitToMultiMap.h"
#include "velox/functions/prestosql/StringFunctions.h"
#include "velox/functions/prestosql/WordStem.h"

namespace facebook::velox::functions {

namespace {
std::shared_ptr<exec::VectorFunction> makeRegexExtract(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config) {
  return makeRe2Extract(name, inputArgs, config, /*emptyNoMatch=*/false);
}

void registerSimpleFunctions(const std::string& prefix) {
  using namespace stringImpl;

  // Register string functions.
  registerFunction<ChrFunction, Varchar, int64_t>({prefix + "chr"});
  registerFunction<CodePointFunction, int32_t, Varchar>({prefix + "codepoint"});
  registerFunction<HammingDistanceFunction, int64_t, Varchar, Varchar>(
      {prefix + "hamming_distance"});
  registerFunction<LevenshteinDistanceFunction, int64_t, Varchar, Varchar>(
      {prefix + "levenshtein_distance"});
  registerFunction<LengthFunction, int64_t, Varchar>({prefix + "length"});
  registerFunction<XxHash64StringFunction, int64_t, Varchar>(
      {prefix + "xxhash64_internal"});

  // Length for varbinary have different semantics.
  registerFunction<LengthVarbinaryFunction, int64_t, Varbinary>(
      {prefix + "length"});

  registerFunction<StartsWithFunction, bool, Varchar, Varchar>(
      {prefix + "starts_with"});
  registerFunction<EndsWithFunction, bool, Varchar, Varchar>(
      {prefix + "ends_with"});
  registerFunction<EndsWithFunction, bool, Varchar, UnknownValue>(
      {prefix + "ends_with"});

  registerFunction<TrailFunction, Varchar, Varchar, int32_t>(
      {prefix + "trail"});

  registerFunction<SubstrFunction, Varchar, Varchar, int64_t>(
      {prefix + "substr", prefix + "substring"});
  registerFunction<SubstrFunction, Varchar, Varchar, int64_t, int64_t>(
      {prefix + "substr", prefix + "substring"});

  // TODO Presto doesn't allow INTEGER types for 2nd and 3rd arguments. Remove
  // these signatures.
  registerFunction<SubstrFunction, Varchar, Varchar, int32_t>(
      {prefix + "substr", prefix + "substring"});
  registerFunction<SubstrFunction, Varchar, Varchar, int32_t, int32_t>(
      {prefix + "substr", prefix + "substring"});

  registerFunction<SubstrVarbinaryFunction, Varbinary, Varbinary, int64_t>(
      {prefix + "substr"});
  registerFunction<
      SubstrVarbinaryFunction,
      Varbinary,
      Varbinary,
      int64_t,
      int64_t>({prefix + "substr"});

  registerFunction<SplitPart, Varchar, Varchar, Varchar, int64_t>(
      {prefix + "split_part"});

  registerFunction<TrimFunction, Varchar, Varchar>({prefix + "trim"});
  registerFunction<TrimFunction, Varchar, Varchar, Varchar>({prefix + "trim"});
  registerFunction<LTrimFunction, Varchar, Varchar>({prefix + "ltrim"});
  registerFunction<LTrimFunction, Varchar, Varchar, Varchar>(
      {prefix + "ltrim"});
  registerFunction<RTrimFunction, Varchar, Varchar>({prefix + "rtrim"});
  registerFunction<RTrimFunction, Varchar, Varchar, Varchar>(
      {prefix + "rtrim"});

  registerFunction<LPadFunction, Varchar, Varchar, int64_t, Varchar>(
      {prefix + "lpad"});
  registerFunction<RPadFunction, Varchar, Varchar, int64_t, Varchar>(
      {prefix + "rpad"});

  exec::registerStatefulVectorFunction(
      prefix + "like", likeSignatures(), makeLike);

  registerFunction<Re2RegexpReplacePresto, Varchar, Varchar, Varchar>(
      {prefix + "regexp_replace"});
  registerFunction<Re2RegexpReplacePresto, Varchar, Varchar, Varchar, Varchar>(
      {prefix + "regexp_replace"});
  exec::registerStatefulVectorFunction(
      prefix + "regexp_replace",
      regexpReplaceWithLambdaSignatures(),
      makeRegexpReplaceWithLambda,
      exec::VectorFunctionMetadataBuilder().defaultNullBehavior(false).build());

  registerFunction<Re2RegexpSplit, Array<Varchar>, Varchar, Varchar>(
      {prefix + "regexp_split"});
}

void registerSplitToMultiMap(const std::string& prefix) {
  registerFunction<
      SplitToMultiMapFunction,
      Map<Varchar, Array<Varchar>>,
      Varchar,
      Varchar,
      Varchar>({prefix + "split_to_multimap"});
  registerFunction<
      SplitToMultiMapFunction,
      Map<Varchar, Array<Varchar>>,
      Varchar,
      UnknownValue,
      Varchar>({prefix + "split_to_multimap"});
  registerFunction<
      SplitToMultiMapFunction,
      Map<Varchar, Array<Varchar>>,
      Varchar,
      Varchar,
      UnknownValue>({prefix + "split_to_multimap"});
  registerFunction<
      SplitToMultiMapFunction,
      Map<Varchar, Array<Varchar>>,
      Varchar,
      UnknownValue,
      UnknownValue>({prefix + "split_to_multimap"});
}

void registerSplitToMap(const std::string& prefix) {
  registerFunction<
      SplitToMapFunction,
      Map<Varchar, Varchar>,
      Varchar,
      Varchar,
      Varchar>({prefix + "split_to_map"});

  registerFunction<
      SplitToMapFunction,
      Map<Varchar, Array<Varchar>>,
      Varchar,
      UnknownValue,
      Varchar>({prefix + "split_to_map"});
  registerFunction<
      SplitToMapFunction,
      Map<Varchar, Array<Varchar>>,
      Varchar,
      Varchar,
      UnknownValue>({prefix + "split_to_map"});
  registerFunction<
      SplitToMapFunction,
      Map<Varchar, Array<Varchar>>,
      Varchar,
      UnknownValue,
      UnknownValue>({prefix + "split_to_map"});

  exec::registerVectorFunction(
      prefix + "split_to_map",
      {
          exec::FunctionSignatureBuilder()
              .returnType("map(varchar,varchar)")
              .argumentType("varchar")
              .argumentType("varchar")
              .argumentType("varchar")
              .argumentType("function(varchar,varchar,varchar,varchar)")
              .build(),
      },
      std::make_unique<exec::ApplyNeverCalled>());
  registerFunction<
      SplitToMapFunction,
      Map<Varchar, Varchar>,
      Varchar,
      Varchar,
      Varchar,
      bool>({"$internal$split_to_map"});
  exec::registerExpressionRewrite([prefix](const auto& expr) {
    return rewriteSplitToMapCall(prefix, expr);
  });
}
} // namespace

void registerStringFunctions(const std::string& prefix) {
  registerSimpleFunctions(prefix);

  VELOX_REGISTER_VECTOR_FUNCTION(udf_lower, prefix + "lower");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_upper, prefix + "upper");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_split, prefix + "split");

  registerSplitToMap(prefix);
  registerSplitToMultiMap(prefix);

  VELOX_REGISTER_VECTOR_FUNCTION(udf_concat, prefix + "concat");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_replaceFirst, prefix + "replace_first");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_replace, prefix + "replace");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_reverse, prefix + "reverse");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_to_utf8, prefix + "to_utf8");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_from_utf8, prefix + "from_utf8");

  // Regex functions
  exec::registerStatefulVectorFunction(
      prefix + "regexp_extract", re2ExtractSignatures(), makeRegexExtract);
  exec::registerStatefulVectorFunction(
      prefix + "regexp_extract_all",
      re2ExtractAllSignatures(),
      makeRe2ExtractAll);
  exec::registerStatefulVectorFunction(
      prefix + "regexp_like", re2SearchSignatures(), makeRe2Search);

  registerFunction<StrLPosFunction, int64_t, Varchar, Varchar>(
      {prefix + "strpos"});
  registerFunction<StrLPosFunction, int64_t, Varchar, Varchar, int64_t>(
      {prefix + "strpos"});
  registerFunction<StrRPosFunction, int64_t, Varchar, Varchar>(
      {prefix + "strrpos"});
  registerFunction<StrRPosFunction, int64_t, Varchar, Varchar, int64_t>(
      {prefix + "strrpos"});

  registerFunction<NormalizeFunction, Varchar, Varchar>({prefix + "normalize"});
  registerFunction<NormalizeFunction, Varchar, Varchar, Varchar>(
      {prefix + "normalize"});

  // word_stem function
  registerFunction<WordStemFunction, Varchar, Varchar>({prefix + "word_stem"});
  registerFunction<WordStemFunction, Varchar, Varchar, Varchar>(
      {prefix + "word_stem"});
}
} // namespace facebook::velox::functions
