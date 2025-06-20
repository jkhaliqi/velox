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

#include "velox/dwio/common/SelectiveColumnReaderInternal.h"

namespace facebook::velox::dwio::common {

/// Abstract class for format and encoding-independent parts of reading integer
/// columns.
class SelectiveIntegerColumnReader : public SelectiveColumnReader {
 public:
  SelectiveIntegerColumnReader(
      const TypePtr& requestedType,
      dwio::common::FormatParams& params,
      velox::common::ScanSpec& scanSpec,
      std::shared_ptr<const dwio::common::TypeWithId> fileType)
      : SelectiveColumnReader(
            requestedType,
            std::move(fileType),
            params,
            scanSpec) {}

  void getValues(const RowSet& rows, VectorPtr* result) override {
    getIntValues(rows, requestedType_, result);
  }

 protected:
  // Switches based on filter type between different readHelper instantiations.
  template <
      typename Reader,
      bool isDense,
      bool kEncodingHasNulls,
      typename ExtractValues>
  void processFilter(
      const velox::common::Filter* filter,
      ExtractValues extractValues,
      const RowSet& rows);

  // Switches based on the type of ValueHook between different readWithVisitor
  // instantiations.
  template <typename Reader, bool isDense>
  void processValueHook(const RowSet& rows, ValueHook* hook);

  // Instantiates a Visitor based on type, isDense, value processing.
  template <
      typename Reader,
      typename TFilter,
      bool isDense,
      typename ExtractValues>
  void readHelper(
      const velox::common::Filter* filter,
      const RowSet& rows,
      ExtractValues extractValues);

  // The common part of integer reading. calls the appropriate
  // instantiation of processValueHook or processFilter based on
  // possible value hook, filter and denseness.
  template <typename Reader, bool kEncodingHasNulls>
  void readCommon(const RowSet& rows);
};

template <
    typename Reader,
    typename TFilter,
    bool isDense,
    typename ExtractValues>
void SelectiveIntegerColumnReader::readHelper(
    const velox::common::Filter* filter,
    const RowSet& rows,
    ExtractValues extractValues) {
  switch (valueSize_) {
    case 2:
      reinterpret_cast<Reader*>(this)->Reader::readWithVisitor(
          rows,
          ColumnVisitor<int16_t, TFilter, ExtractValues, isDense>(
              *static_cast<const TFilter*>(filter), this, rows, extractValues));
      break;

    case 4:
      reinterpret_cast<Reader*>(this)->Reader::readWithVisitor(
          rows,
          ColumnVisitor<int32_t, TFilter, ExtractValues, isDense>(
              *static_cast<const TFilter*>(filter), this, rows, extractValues));
      break;

    case 8:
      reinterpret_cast<Reader*>(this)->Reader::readWithVisitor(
          rows,
          ColumnVisitor<int64_t, TFilter, ExtractValues, isDense>(
              *static_cast<const TFilter*>(filter), this, rows, extractValues));
      break;

    case 16:
      reinterpret_cast<Reader*>(this)->Reader::readWithVisitor(
          rows,
          ColumnVisitor<int128_t, TFilter, ExtractValues, isDense>(
              *static_cast<const TFilter*>(filter), this, rows, extractValues));
      break;

    default:
      VELOX_FAIL("Unsupported valueSize_ {}", valueSize_);
  }
}

template <
    typename Reader,
    bool isDense,
    bool kEncodingHasNulls,
    typename ExtractValues>
void SelectiveIntegerColumnReader::processFilter(
    const velox::common::Filter* filter,
    ExtractValues extractValues,
    const RowSet& rows) {
  if (filter == nullptr) {
    static_cast<Reader*>(this)
        ->template readHelper<Reader, velox::common::AlwaysTrue, isDense>(
            &dwio::common::alwaysTrue(), rows, extractValues);
    return;
  }

  switch (filter->kind()) {
    case velox::common::FilterKind::kAlwaysTrue:
      static_cast<Reader*>(this)
          ->template readHelper<Reader, velox::common::AlwaysTrue, isDense>(
              filter, rows, extractValues);
      break;
    case velox::common::FilterKind::kIsNull:
      if constexpr (kEncodingHasNulls) {
        filterNulls<int64_t>(
            rows, true, !std::is_same_v<decltype(extractValues), DropValues>);
      } else {
        static_cast<Reader*>(this)
            ->template readHelper<Reader, velox::common::IsNull, isDense>(
                filter, rows, extractValues);
      }
      break;
    case velox::common::FilterKind::kIsNotNull:
      if constexpr (
          kEncodingHasNulls &&
          std::is_same_v<decltype(extractValues), DropValues>) {
        filterNulls<int64_t>(rows, false, false);
      } else {
        static_cast<Reader*>(this)
            ->template readHelper<Reader, velox::common::IsNotNull, isDense>(
                filter, rows, extractValues);
      }
      break;
    case velox::common::FilterKind::kBigintRange:
      static_cast<Reader*>(this)
          ->template readHelper<Reader, velox::common::BigintRange, isDense>(
              filter, rows, extractValues);
      break;
    case velox::common::FilterKind::kNegatedBigintRange:
      static_cast<Reader*>(this)
          ->template readHelper<
              Reader,
              velox::common::NegatedBigintRange,
              isDense>(filter, rows, extractValues);
      break;
    case velox::common::FilterKind::kBigintValuesUsingHashTable:
      static_cast<Reader*>(this)
          ->template readHelper<
              Reader,
              velox::common::BigintValuesUsingHashTable,
              isDense>(filter, rows, extractValues);
      break;
    case velox::common::FilterKind::kBigintValuesUsingBitmask:
      static_cast<Reader*>(this)
          ->template readHelper<
              Reader,
              velox::common::BigintValuesUsingBitmask,
              isDense>(filter, rows, extractValues);
      break;
    case velox::common::FilterKind::kNegatedBigintValuesUsingHashTable:
      static_cast<Reader*>(this)
          ->template readHelper<
              Reader,
              velox::common::NegatedBigintValuesUsingHashTable,
              isDense>(filter, rows, extractValues);
      break;
    case velox::common::FilterKind::kNegatedBigintValuesUsingBitmask:
      static_cast<Reader*>(this)
          ->template readHelper<
              Reader,
              velox::common::NegatedBigintValuesUsingBitmask,
              isDense>(filter, rows, extractValues);
      break;
    default:
      static_cast<Reader*>(this)
          ->template readHelper<Reader, velox::common::Filter, isDense>(
              filter, rows, extractValues);
      break;
  }
}

template <typename Reader, bool isDense>
void SelectiveIntegerColumnReader::processValueHook(
    const RowSet& rows,
    ValueHook* hook) {
  switch (hook->kind()) {
    case aggregate::AggregationHook::kBigintSum:
      readHelper<Reader, velox::common::AlwaysTrue, isDense>(
          &alwaysTrue(),
          rows,
          ExtractToHook<aggregate::SumHook<int64_t, false>>(hook));
      break;
    case aggregate::AggregationHook::kBigintSumOverflow:
      readHelper<Reader, velox::common::AlwaysTrue, isDense>(
          &alwaysTrue(),
          rows,
          ExtractToHook<aggregate::SumHook<int64_t, true>>(hook));
      break;
    case aggregate::AggregationHook::kBigintMax:
      readHelper<Reader, velox::common::AlwaysTrue, isDense>(
          &dwio::common::alwaysTrue(),
          rows,
          ExtractToHook<aggregate::MinMaxHook<int64_t, false>>(hook));
      break;
    case aggregate::AggregationHook::kBigintMin:
      readHelper<Reader, velox::common::AlwaysTrue, isDense>(
          &alwaysTrue(),
          rows,
          ExtractToHook<aggregate::MinMaxHook<int64_t, true>>(hook));
      break;
    default:
      readHelper<Reader, velox::common::AlwaysTrue, isDense>(
          &alwaysTrue(), rows, ExtractToGenericHook(hook));
  }
}

template <typename Reader, bool kEncodingHasNulls>
void SelectiveIntegerColumnReader::readCommon(const RowSet& rows) {
  const bool isDense = rows.back() == rows.size() - 1;
  auto* filter = scanSpec_->filter() ? scanSpec_->filter() : &alwaysTrue();
  if (scanSpec_->keepValues()) {
    if (scanSpec_->valueHook()) {
      if (isDense) {
        processValueHook<Reader, true>(rows, scanSpec_->valueHook());
      } else {
        processValueHook<Reader, false>(rows, scanSpec_->valueHook());
      }
    } else {
      if (isDense) {
        processFilter<Reader, true, kEncodingHasNulls>(
            filter, ExtractToReader(this), rows);
      } else {
        processFilter<Reader, false, kEncodingHasNulls>(
            filter, ExtractToReader(this), rows);
      }
    }
  } else {
    if (isDense) {
      processFilter<Reader, true, kEncodingHasNulls>(
          filter, DropValues(), rows);
    } else {
      processFilter<Reader, false, kEncodingHasNulls>(
          filter, DropValues(), rows);
    }
  }
}

} // namespace facebook::velox::dwio::common
