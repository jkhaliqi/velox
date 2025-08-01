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

namespace facebook::velox::aggregate {

const char* const kApproxDistinct = "approx_distinct";
const char* const kApproxMostFrequent = "approx_most_frequent";
const char* const kApproxPercentile = "approx_percentile";
const char* const kApproxSet = "approx_set";
const char* const kQDigestAgg = "qdigest_agg";
const char* const kArbitrary = "arbitrary";
const char* const kAnyValue = "any_value";
const char* const kArrayAgg = "array_agg";
const char* const kAvg = "avg";
const char* const kBitwiseAnd = "bitwise_and_agg";
const char* const kBitwiseOr = "bitwise_or_agg";
const char* const kBitwiseXor = "bitwise_xor_agg";
const char* const kBoolAnd = "bool_and";
const char* const kBoolOr = "bool_or";
const char* const kChecksum = "checksum";
const char* const kClassificationFallout = "classification_fall_out";
const char* const kClassificationPrecision = "classification_precision";
const char* const kClassificationRecall = "classification_recall";
const char* const kClassificationMissRate = "classification_miss_rate";
const char* const kClassificationThreshold = "classification_thresholds";
const char* const kCorr = "corr";
const char* const kCount = "count";
const char* const kCountIf = "count_if";
const char* const kCovarPop = "covar_pop";
const char* const kCovarSamp = "covar_samp";
const char* const kEntropy = "entropy";
const char* const kEvery = "every";
const char* const kGeometricMean = "geometric_mean";
const char* const kHistogram = "histogram";
const char* const kKurtosis = "kurtosis";
const char* const kMapAgg = "map_agg";
const char* const kMapUnion = "map_union";
const char* const kMapUnionSum = "map_union_sum";
const char* const kMax = "max";
const char* const kMaxBy = "max_by";
const char* const kMerge = "merge";
const char* const kMin = "min";
const char* const kMinBy = "min_by";
const char* const kMultiMapAgg = "multimap_agg";
const char* const kNoisyAvgGaussian = "noisy_avg_gaussian";
const char* const kNoisyCountIfGaussian = "noisy_count_if_gaussian";
const char* const kNoisyCountGaussian = "noisy_count_gaussian";
const char* const kNoisySumGaussian = "noisy_sum_gaussian";
const char* const kReduceAgg = "reduce_agg";
const char* const kRegrIntercept = "regr_intercept";
const char* const kRegrSlop = "regr_slope";
const char* const kRegrCount = "regr_count";
const char* const kRegrAvgy = "regr_avgy";
const char* const kRegrAvgx = "regr_avgx";
const char* const kRegrSxy = "regr_sxy";
const char* const kRegrSxx = "regr_sxx";
const char* const kRegrSyy = "regr_syy";
const char* const kRegrR2 = "regr_r2";
const char* const kSetAgg = "set_agg";
const char* const kSetUnion = "set_union";
const char* const kSkewness = "skewness";
const char* const kStdDev = "stddev"; // Alias for stddev_samp.
const char* const kStdDevPop = "stddev_pop";
const char* const kStdDevSamp = "stddev_samp";
const char* const kSum = "sum";
const char* const kTDigestAgg = "tdigest_agg";
const char* const kVariance = "variance"; // Alias for var_samp.
const char* const kVarPop = "var_pop";
const char* const kVarSamp = "var_samp";
const char* const kMaxSizeForStats = "max_data_size_for_stats";
const char* const kSumDataSizeForStats = "sum_data_size_for_stats";
const char* const kNoisyApproxSetSfm = "noisy_approx_set_sfm";
const char* const kNoisyApproxDistinctSfm = "noisy_approx_distinct_sfm";
const char* const kNoisyApproxSetSfmFromIndexAndZeros =
    "noisy_approx_set_sfm_from_index_and_zeros";
} // namespace facebook::velox::aggregate
