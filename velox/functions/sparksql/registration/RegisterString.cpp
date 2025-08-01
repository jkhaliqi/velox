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
#include "velox/expression/SpecialFormRegistry.h"
#include "velox/functions/lib/Re2Functions.h"
#include "velox/functions/lib/UpperLower.h"
#include "velox/functions/prestosql/StringFunctions.h"
#include "velox/functions/prestosql/URLFunctions.h"
#include "velox/functions/sparksql/Base64Function.h"
#include "velox/functions/sparksql/CharTypeWriteSideCheck.h"
#include "velox/functions/sparksql/ConcatWs.h"
#include "velox/functions/sparksql/InitcapFunction.h"
#include "velox/functions/sparksql/LuhnCheckFunction.h"
#include "velox/functions/sparksql/MaskFunction.h"
#include "velox/functions/sparksql/Split.h"
#include "velox/functions/sparksql/String.h"
#include "velox/functions/sparksql/StringToMap.h"
#include "velox/functions/sparksql/UnBase64Function.h"
#include "velox/functions/sparksql/VarcharTypeWriteSideCheck.h"

namespace facebook::velox::functions {
void registerSparkStringFunctions(const std::string& prefix) {
  VELOX_REGISTER_VECTOR_FUNCTION(udf_concat, prefix + "concat");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_reverse, prefix + "reverse");
}

namespace sparksql {
void registerStringFunctions(const std::string& prefix) {
  registerSparkStringFunctions(prefix);
  registerFunction<StartsWithFunction, bool, Varchar, Varchar>(
      {prefix + "startswith"});
  registerFunction<EndsWithFunction, bool, Varchar, Varchar>(
      {prefix + "endswith"});
  registerFunction<ContainsFunction, bool, Varchar, Varchar>(
      {prefix + "contains"});
  registerFunction<LocateFunction, int32_t, Varchar, Varchar, int32_t>(
      {prefix + "locate"});
  registerFunction<TrimSpaceFunction, Varchar, Varchar>({prefix + "trim"});
  registerFunction<TrimFunction, Varchar, Varchar, Varchar>({prefix + "trim"});
  registerFunction<LTrimSpaceFunction, Varchar, Varchar>({prefix + "ltrim"});
  registerFunction<LTrimFunction, Varchar, Varchar, Varchar>(
      {prefix + "ltrim"});
  registerFunction<RTrimSpaceFunction, Varchar, Varchar>({prefix + "rtrim"});
  registerFunction<RTrimFunction, Varchar, Varchar, Varchar>(
      {prefix + "rtrim"});
  registerFunction<TranslateFunction, Varchar, Varchar, Varchar, Varchar>(
      {prefix + "translate"});
  registerFunction<ConvFunction, Varchar, Varchar, int32_t, int32_t>(
      {prefix + "conv"});
  registerFunction<ReplaceFunction, Varchar, Varchar, Varchar>(
      {prefix + "replace"});
  registerFunction<ReplaceFunction, Varchar, Varchar, Varchar, Varchar>(
      {prefix + "replace"});
  registerFunction<FindInSetFunction, int32_t, Varchar, Varchar>(
      {prefix + "find_in_set"});
  registerFunction<sparksql::ChrFunction, Varchar, int64_t>({prefix + "chr"});
  registerFunction<AsciiFunction, int32_t, Varchar>({prefix + "ascii"});
  registerFunction<sparksql::LPadFunction, Varchar, Varchar, int32_t, Varchar>(
      {prefix + "lpad"});
  registerFunction<sparksql::RPadFunction, Varchar, Varchar, int32_t, Varchar>(
      {prefix + "rpad"});
  registerFunction<sparksql::LPadFunction, Varchar, Varchar, int32_t>(
      {prefix + "lpad"});
  registerFunction<sparksql::RPadFunction, Varchar, Varchar, int32_t>(
      {prefix + "rpad"});
  registerFunction<sparksql::SubstrFunction, Varchar, Varchar, int32_t>(
      {prefix + "substring"});
  registerFunction<
      sparksql::SubstrFunction,
      Varchar,
      Varchar,
      int32_t,
      int32_t>({prefix + "substring"});
  registerFunction<
      sparksql::OverlayVarcharFunction,
      Varchar,
      Varchar,
      Varchar,
      int32_t,
      int32_t>({prefix + "overlay"});
  registerFunction<
      sparksql::OverlayVarbinaryFunction,
      Varbinary,
      Varbinary,
      Varbinary,
      int32_t,
      int32_t>({prefix + "overlay"});
  registerFunction<
      sparksql::StringToMapFunction,
      Map<Varchar, Varchar>,
      Varchar,
      Varchar,
      Varchar>({prefix + "str_to_map"});
  registerFunction<sparksql::LeftFunction, Varchar, Varchar, int32_t>(
      {prefix + "left"});
  registerFunction<sparksql::BitLengthFunction, int32_t, Varchar>(
      {prefix + "bit_length"});
  registerFunction<sparksql::BitLengthFunction, int32_t, Varbinary>(
      {prefix + "bit_length"});
  exec::registerStatefulVectorFunction(
      prefix + "instr", instrSignatures(), makeInstr);
  exec::registerStatefulVectorFunction(
      prefix + "length", lengthSignatures(), makeLength);
  registerFunction<SubstringIndexFunction, Varchar, Varchar, Varchar, int32_t>(
      {prefix + "substring_index"});
  registerFunction<Empty2NullFunction, Varchar, Varchar>(
      {prefix + "empty2null"});
  registerFunction<
      LevenshteinDistanceFunction,
      int32_t,
      Varchar,
      Varchar,
      int32_t>({prefix + "levenshtein"});
  registerFunction<LevenshteinDistanceFunction, int32_t, Varchar, Varchar>(
      {prefix + "levenshtein"});
  registerFunction<RepeatFunction, Varchar, Varchar, int32_t>(
      {prefix + "repeat"});
  registerFunction<SoundexFunction, Varchar, Varchar>({prefix + "soundex"});
  registerFunction<Split, Array<Varchar>, Varchar, Varchar>({prefix + "split"});
  registerFunction<Split, Array<Varchar>, Varchar, Varchar, int32_t>(
      {prefix + "split"});
  registerFunction<MaskFunction, Varchar, Varchar>({prefix + "mask"});
  registerFunction<MaskFunction, Varchar, Varchar, Varchar>({prefix + "mask"});
  registerFunction<MaskFunction, Varchar, Varchar, Varchar, Varchar>(
      {prefix + "mask"});
  registerFunction<MaskFunction, Varchar, Varchar, Varchar, Varchar, Varchar>(
      {prefix + "mask"});
  registerFunction<
      MaskFunction,
      Varchar,
      Varchar,
      Varchar,
      Varchar,
      Varchar,
      Varchar>({prefix + "mask"});
  registerFunctionCallToSpecialForm(
      ConcatWsCallToSpecialForm::kConcatWs,
      std::make_unique<ConcatWsCallToSpecialForm>());
  registerFunction<LuhnCheckFunction, bool, Varchar>({prefix + "luhn_check"});

  using SparkUpperFunction = UpperLowerTemplateFunction<
      /*isLower=*/false,
      /*turkishCasing=*/true,
      /*greekFinalSigma=*/true>;
  using SparkLowerFunction = UpperLowerTemplateFunction<
      /*isLower=*/true,
      /*turkishCasing=*/true,
      /*greekFinalSigma=*/true>;
  exec::registerVectorFunction(
      prefix + "upper",
      SparkUpperFunction::signatures(),
      std::make_unique<SparkUpperFunction>());
  exec::registerVectorFunction(
      prefix + "lower",
      SparkLowerFunction::signatures(),
      std::make_unique<SparkLowerFunction>());
  registerFunction<
      VarcharTypeWriteSideCheckFunction,
      Varchar,
      Varchar,
      int32_t>({prefix + "varchar_type_write_side_check"});

  registerFunction<CharTypeWriteSideCheckFunction, Varchar, Varchar, int32_t>(
      {prefix + "char_type_write_side_check"});

  registerFunction<Base64Function, Varchar, Varbinary>({prefix + "base64"});
  registerFunction<UnBase64Function, Varbinary, Varchar>({prefix + "unbase64"});

  registerFunction<InitCapFunction, Varchar, Varchar>({prefix + "initcap"});
}
} // namespace sparksql
} // namespace facebook::velox::functions
