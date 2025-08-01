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

#include "velox/vector/BaseVector.h"
#include "velox/type/StringView.h"
#include "velox/type/Type.h"
#include "velox/type/Variant.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/DictionaryVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/LazyVector.h"
#include "velox/vector/SequenceVector.h"
#include "velox/vector/TypeAliases.h"
#include "velox/vector/VectorEncoding.h"
#include "velox/vector/VectorPool.h"
#include "velox/vector/VectorTypeUtils.h"

namespace facebook::velox {

BaseVector::BaseVector(
    velox::memory::MemoryPool* pool,
    std::shared_ptr<const Type> type,
    VectorEncoding::Simple encoding,
    BufferPtr nulls,
    size_t length,
    std::optional<vector_size_t> distinctValueCount,
    std::optional<vector_size_t> nullCount,
    std::optional<ByteCount> representedByteCount,
    std::optional<ByteCount> storageByteCount)
    : type_(std::move(type)),
      typeKind_(type_ ? type_->kind() : TypeKind::INVALID),
      typeUsesCustomComparison_(
          type_ ? type_->providesCustomComparison() : false),
      encoding_(encoding),
      nulls_(std::move(nulls)),
      rawNulls_(nulls_.get() ? nulls_->as<uint64_t>() : nullptr),
      pool_(pool),
      length_(length),
      nullCount_(nullCount),
      distinctValueCount_(distinctValueCount),
      representedByteCount_(representedByteCount),
      storageByteCount_(storageByteCount) {
  VELOX_CHECK_NOT_NULL(type_, "Vector creation requires a non-null type.");
  VELOX_CHECK_LE(
      length,
      std::numeric_limits<vector_size_t>::max(),
      "Length must be smaller or equal to max(vector_size_t).");

  if (nulls_) {
    int32_t bytes = byteSize<bool>(length_);
    VELOX_CHECK_GE(nulls_->capacity(), bytes);
    if (nulls_->size() < bytes) {
      // Set the size so that values get preserved by resize. Do not
      // set if already large enough, so that it is safe to take a
      // second reference to an immutable 'nulls_'.
      nulls_->setSize(bytes);
    }
    inMemoryBytes_ += nulls_->size();
  }
}

void BaseVector::ensureNullsCapacity(
    vector_size_t minimumSize,
    bool setNotNull) {
  const auto fill = setNotNull ? bits::kNotNull : bits::kNull;
  // Ensure the size of nulls_ is always at least as large as length_.
  const auto size = std::max<vector_size_t>(minimumSize, length_);
  if (nulls_ && !nulls_->isView() && nulls_->unique()) {
    if (nulls_->capacity() < bits::nbytes(size)) {
      AlignedBuffer::reallocate<bool>(&nulls_, size, fill);
    }
    // ensure that the newly added positions have the right initial value for
    // the case where changes in size don't result in change in the size of
    // the underlying buffer.
    // TODO: move this inside reallocate.
    rawNulls_ = nulls_->as<uint64_t>();
    if (setNotNull && length_ < size) {
      bits::fillBits(
          const_cast<uint64_t*>(rawNulls_), length_, size, bits::kNotNull);
    }
  } else {
    auto newNulls = AlignedBuffer::allocate<bool>(size, pool_, fill);
    if (nulls_) {
      ::memcpy(
          newNulls->asMutable<char>(),
          nulls_->as<char>(),
          byteSize<bool>(std::min<vector_size_t>(length_, size)));
    }
    nulls_ = std::move(newNulls);
    rawNulls_ = nulls_->as<uint64_t>();
  }
}

template <>
uint64_t BaseVector::byteSize<bool>(vector_size_t count) {
  return bits::nbytes(count);
}

void BaseVector::resize(vector_size_t size, bool setNotNull) {
  VELOX_CHECK_GE(size, 0, "Size must be non-negative.");
  if (nulls_) {
    const auto bytes = byteSize<bool>(size);
    if (length_ < size || nulls_->isView()) {
      ensureNullsCapacity(size, setNotNull);
    }
    nulls_->setSize(bytes);
  }
  length_ = size;
}

template <TypeKind kind>
static VectorPtr addDictionary(
    BufferPtr nulls,
    BufferPtr indices,
    size_t size,
    VectorPtr vector) {
  auto pool = vector->pool();
  return std::make_shared<
      DictionaryVector<typename KindToFlatVector<kind>::WrapperType>>(
      pool, std::move(nulls), size, std::move(vector), std::move(indices));
}

// static
VectorPtr BaseVector::wrapInDictionary(
    BufferPtr nulls,
    BufferPtr indices,
    vector_size_t size,
    VectorPtr vector,
    bool flattenIfRedundant) {
  // Dictionary that doesn't add nulls over constant is same as constant. Just
  // make sure to adjust the size.
  if (vector->encoding() == VectorEncoding::Simple::CONSTANT && !nulls) {
    if (size == vector->size()) {
      return vector;
    }
    return BaseVector::wrapInConstant(size, 0, std::move(vector));
  }

  bool shouldFlatten = false;
  if (flattenIfRedundant) {
    auto base = vector;
    while (base->encoding() == VectorEncoding::Simple::DICTIONARY) {
      base = base->valueVector();
    }
    shouldFlatten = !isLazyNotLoaded(*base) && (base->size() / 8) > size;
  }

  const auto kind = vector->typeKind();
  auto result = VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
      addDictionary,
      kind,
      std::move(nulls),
      std::move(indices),
      size,
      std::move(vector));

  if (shouldFlatten) {
    BaseVector::flattenVector(result);
  }

  return result;
}

// static
VectorPtr BaseVector::wrapInSequence(
    BufferPtr lengths,
    vector_size_t size,
    VectorPtr vector) {
  const auto numLengths = lengths->size() / sizeof(vector_size_t);
  int64_t numIndices = 0;
  auto* rawLengths = lengths->as<vector_size_t>();
  for (auto i = 0; i < numLengths; ++i) {
    numIndices += rawLengths[i];
  }
  VELOX_CHECK_LT(numIndices, std::numeric_limits<int32_t>::max());
  BufferPtr indices =
      AlignedBuffer::allocate<vector_size_t>(numIndices, vector->pool());
  auto* rawIndices = indices->asMutable<vector_size_t>();
  int32_t fill = 0;
  for (auto i = 0; i < numLengths; ++i) {
    std::fill(rawIndices + fill, rawIndices + fill + rawLengths[i], i);
    fill += rawLengths[i];
  }
  return wrapInDictionary(nullptr, indices, numIndices, vector);
}

template <TypeKind kind>
static VectorPtr
addConstant(vector_size_t size, vector_size_t index, VectorPtr vector) {
  using T = typename KindToFlatVector<kind>::WrapperType;

  auto* pool = vector->pool();

  if (vector->isNullAt(index)) {
    if constexpr (std::is_same_v<T, ComplexType>) {
      auto singleNull = BaseVector::create(vector->type(), 1, pool);
      singleNull->setNull(0, true);
      return std::make_shared<ConstantVector<T>>(
          pool, size, 0, singleNull, SimpleVectorStats<T>{});
    } else {
      return std::make_shared<ConstantVector<T>>(
          pool, size, true, vector->type(), T());
    }
  }

  for (;;) {
    if (vector->isConstantEncoding()) {
      auto constVector = vector->as<ConstantVector<T>>();
      if constexpr (!std::is_same_v<T, ComplexType>) {
        if (!vector->valueVector()) {
          T value = constVector->valueAt(0);
          return std::make_shared<ConstantVector<T>>(
              pool, size, false, vector->type(), std::move(value));
        }
      }

      index = constVector->index();
      vector = vector->valueVector();
    } else if (vector->encoding() == VectorEncoding::Simple::DICTIONARY) {
      const BufferPtr& indices = vector->as<DictionaryVector<T>>()->indices();
      index = indices->as<vector_size_t>()[index];
      vector = vector->valueVector();
    } else {
      break;
    }
  }

  return std::make_shared<ConstantVector<T>>(
      pool, size, index, std::move(vector), SimpleVectorStats<T>{});
}

// static
VectorPtr BaseVector::wrapInConstant(
    vector_size_t length,
    vector_size_t index,
    VectorPtr vector) {
  const auto kind = vector->typeKind();
  return VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
      addConstant, kind, length, index, std::move(vector));
}

std::optional<bool> BaseVector::equalValueAt(
    const BaseVector* other,
    vector_size_t index,
    vector_size_t otherIndex,
    CompareFlags::NullHandlingMode nullHandlingMode) const {
  const CompareFlags compareFlags = CompareFlags::equality(nullHandlingMode);
  std::optional<int32_t> result =
      compare(other, index, otherIndex, compareFlags);
  if (result.has_value()) {
    return result.value() == 0;
  }

  return std::nullopt;
}

template <TypeKind kind>
static VectorPtr createEmpty(
    vector_size_t size,
    velox::memory::MemoryPool* pool,
    const TypePtr& type) {
  using T = typename TypeTraits<kind>::NativeType;

  BufferPtr values;
  if constexpr (std::is_same_v<T, StringView>) {
    // Make sure to initialize StringView values so they can be safely accessed.
    values = AlignedBuffer::allocate<T>(size, pool, T());
  } else {
    values = AlignedBuffer::allocate<T>(size, pool);
  }

  return std::make_shared<FlatVector<T>>(
      pool,
      type,
      BufferPtr(nullptr),
      size,
      std::move(values),
      std::vector<BufferPtr>());
}

// static
VectorPtr BaseVector::createInternal(
    const TypePtr& type,
    vector_size_t size,
    velox::memory::MemoryPool* pool) {
  VELOX_CHECK_NOT_NULL(type, "Vector creation requires a non-null type.");
  auto kind = type->kind();
  switch (kind) {
    case TypeKind::ROW: {
      std::vector<VectorPtr> children;
      auto& rowType = type->as<TypeKind::ROW>();
      // Children are reserved the parent size and accessible for those rows.
      for (int32_t i = 0; i < rowType.size(); ++i) {
        children.push_back(create(rowType.childAt(i), size, pool));
      }
      return std::make_shared<RowVector>(
          pool, type, nullptr, size, std::move(children));
    }
    case TypeKind::ARRAY: {
      BufferPtr sizes = allocateSizes(size, pool);
      BufferPtr offsets = allocateOffsets(size, pool);
      const auto& elementType = type->as<TypeKind::ARRAY>().elementType();
      auto elements = create(elementType, 0, pool);
      return std::make_shared<ArrayVector>(
          pool,
          type,
          nullptr,
          size,
          std::move(offsets),
          std::move(sizes),
          std::move(elements));
    }
    case TypeKind::MAP: {
      BufferPtr sizes = allocateSizes(size, pool);
      BufferPtr offsets = allocateOffsets(size, pool);
      const auto& keyType = type->as<TypeKind::MAP>().keyType();
      const auto& valueType = type->as<TypeKind::MAP>().valueType();
      auto keys = create(keyType, 0, pool);
      auto values = create(valueType, 0, pool);
      return std::make_shared<MapVector>(
          pool,
          type,
          nullptr,
          size,
          std::move(offsets),
          std::move(sizes),
          std::move(keys),
          std::move(values));
    }
    case TypeKind::UNKNOWN: {
      BufferPtr nulls = allocateNulls(size, pool, bits::kNull);
      return std::make_shared<FlatVector<UnknownValue>>(
          pool,
          UNKNOWN(),
          std::move(nulls),
          size,
          nullptr,
          std::vector<BufferPtr>());
    }
    default:
      return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
          createEmpty, kind, size, pool, type);
  }
}

// static
void BaseVector::setNulls(
    uint64_t* rawNulls,
    const folly::Range<const CopyRange*>& ranges,
    bool isNull) {
  const auto nullBits = isNull ? bits::kNull : bits::kNotNull;
  applyToEachRange(
      ranges, [&](auto targetIndex, auto /*sourceIndex*/, auto count) {
        bits::fillBits(rawNulls, targetIndex, targetIndex + count, nullBits);
      });
}

// static
void BaseVector::copyNulls(
    uint64_t* targetRawNulls,
    const uint64_t* sourceRawNulls,
    const folly::Range<const CopyRange*>& ranges) {
  applyToEachRange(ranges, [&](auto targetIndex, auto sourceIndex, auto count) {
    bits::copyBits(
        sourceRawNulls, sourceIndex, targetRawNulls, targetIndex, count);
  });
}

void BaseVector::addNulls(const uint64_t* bits, const SelectivityVector& rows) {
  if (bits == nullptr || !rows.hasSelections()) {
    return;
  }
  VELOX_CHECK(isNullsWritable());
  VELOX_CHECK_GE(length_, rows.end());
  ensureNulls();
  auto target = nulls_->asMutable<uint64_t>();
  const uint64_t* selected = rows.asRange().bits();
  // A 0 in bits with a 1 in rows makes a 0 in nulls.
  bits::forEachWord(
      rows.begin(),
      rows.end(),
      [target, bits, selected](int32_t idx, uint64_t mask) {
        target[idx] &= ~mask | (bits[idx] | ~selected[idx]);
      },
      [target, bits, selected](int32_t idx) {
        target[idx] &= bits[idx] | ~selected[idx];
      });
}

void BaseVector::addNulls(const SelectivityVector& nullRows) {
  if (!nullRows.hasSelections()) {
    return;
  }
  VELOX_CHECK(isNullsWritable());
  VELOX_CHECK_GE(length_, nullRows.end());
  ensureNulls();
  auto target = nulls_->asMutable<uint64_t>();
  const uint64_t* selected = nullRows.asRange().bits();
  // A 1 in rows makes a 0 in nulls.
  bits::andWithNegatedBits(target, selected, nullRows.begin(), nullRows.end());
  return;
}

void BaseVector::clearNulls(const SelectivityVector& nonNullRows) {
  VELOX_CHECK(isNullsWritable());
  if (!nulls_) {
    return;
  }

  if (nonNullRows.isAllSelected() && nonNullRows.end() == length_) {
    nulls_ = nullptr;
    rawNulls_ = nullptr;
    nullCount_ = 0;
    return;
  }

  auto rawNulls = nulls_->asMutable<uint64_t>();
  bits::orBits(
      rawNulls,
      nonNullRows.asRange().bits(),
      std::min<vector_size_t>(length_, nonNullRows.begin()),
      std::min<vector_size_t>(length_, nonNullRows.end()));
  nullCount_ = std::nullopt;
}

void BaseVector::clearNulls(vector_size_t begin, vector_size_t end) {
  VELOX_CHECK(isNullsWritable());
  if (!nulls_) {
    return;
  }

  if (begin == 0 && end == length_) {
    nulls_ = nullptr;
    rawNulls_ = nullptr;
    nullCount_ = 0;
    return;
  }

  auto* rawNulls = nulls_->asMutable<uint64_t>();
  bits::fillBits(rawNulls, begin, end, bits::kNotNull);
  nullCount_ = std::nullopt;
}

void BaseVector::setNulls(const BufferPtr& nulls) {
  if (nulls) {
    VELOX_DCHECK_GE(nulls->size(), bits::nbytes(length_));
    nulls_ = nulls;
    rawNulls_ = nulls->as<uint64_t>();
    nullCount_ = std::nullopt;
  } else {
    nulls_ = nullptr;
    rawNulls_ = nullptr;
    nullCount_ = 0;
  }
}

// static
void BaseVector::resizeIndices(
    vector_size_t currentSize,
    vector_size_t newSize,
    velox::memory::MemoryPool* pool,
    BufferPtr& indices,
    const vector_size_t** rawIndices) {
  const auto newNumBytes = byteSize<vector_size_t>(newSize);
  if (indices != nullptr && !indices->isView() && indices->unique()) {
    if (indices->size() < newNumBytes) {
      AlignedBuffer::reallocate<vector_size_t>(&indices, newSize, 0);
    }
    // indices->size() may cover more indices than currentSize.
    if (newSize > currentSize) {
      auto* raw = indices->asMutable<vector_size_t>();
      std::fill(raw + currentSize, raw + newSize, 0);
    }
  } else {
    auto newIndices = AlignedBuffer::allocate<vector_size_t>(newSize, pool, 0);
    if (indices != nullptr) {
      auto* dst = newIndices->asMutable<vector_size_t>();
      const auto* src = indices->as<vector_size_t>();
      const auto numCopyBytes = std::min<vector_size_t>(
          byteSize<vector_size_t>(currentSize), newNumBytes);
      memcpy(dst, src, numCopyBytes);
    }
    indices = newIndices;
  }
  *rawIndices = indices->asMutable<vector_size_t>();
}

std::string BaseVector::toSummaryString() const {
  std::stringstream out;
  out << "[" << encoding() << " " << type_->toString() << ": " << length_
      << " elements, ";
  if (!nulls_) {
    out << "no nulls";
  } else {
    out << countNulls(nulls_, 0, length_) << " nulls";
  }
  out << "]";
  return out.str();
}

std::string BaseVector::toString(bool recursive) const {
  std::stringstream out;
  out << toSummaryString();

  if (recursive) {
    switch (encoding()) {
      case VectorEncoding::Simple::DICTIONARY:
      case VectorEncoding::Simple::SEQUENCE:
      case VectorEncoding::Simple::CONSTANT:
        if (valueVector() != nullptr) {
          out << ", " << valueVector()->toString(true);
        }
        break;
      default:
        break;
    }
  }

  return out.str();
}

std::string BaseVector::toString(vector_size_t index) const {
  VELOX_CHECK_LT(index, length_, "Vector index should be less than length.");
  std::stringstream out;
  if (!nulls_) {
    out << "no nulls";
  } else if (isNullAt(index)) {
    out << kNullValueString;
  } else {
    out << "not null";
  }
  return out.str();
}

std::string BaseVector::toString(
    vector_size_t from,
    vector_size_t to,
    const char* delimiter,
    bool includeRowNumbers) const {
  const auto start = std::max(0, std::min<int32_t>(from, length_));
  const auto end = std::max(0, std::min<int32_t>(to, length_));

  std::stringstream out;
  for (auto i = start; i < end; ++i) {
    if (i > start) {
      out << delimiter;
    }
    if (includeRowNumbers) {
      out << i << ": ";
    }
    out << toString(i);
  }
  return out.str();
}

void BaseVector::ensureWritable(const SelectivityVector& rows) {
  auto newSize = std::max<vector_size_t>(rows.end(), length_);
  if (nulls_ && !nulls_->isMutable()) {
    BufferPtr newNulls = AlignedBuffer::allocate<bool>(newSize, pool_);
    auto rawNewNulls = newNulls->asMutable<uint64_t>();
    memcpy(rawNewNulls, rawNulls_, bits::nbytes(length_));

    nulls_ = std::move(newNulls);
    rawNulls_ = nulls_->as<uint64_t>();
  }

  this->resize(newSize);
  this->resetDataDependentFlags(&rows);
}

// static
void BaseVector::ensureWritable(
    const SelectivityVector& rows,
    const TypePtr& type,
    velox::memory::MemoryPool* pool,
    VectorPtr& result,
    VectorPool* vectorPool) {
  if (!result) {
    if (vectorPool) {
      result = vectorPool->get(type, rows.end());
    } else {
      result = BaseVector::create(type, rows.end(), pool);
    }
    return;
  }

  if (result->encoding() == VectorEncoding::Simple::LAZY) {
    result = BaseVector::loadedVectorShared(result);
  }

  const auto& resultType = result->type();
  const bool isUnknownType = resultType->containsUnknown();

  // Check if ensure writable can work in place.
  if (result.use_count() == 1 && !isUnknownType) {
    switch (result->encoding()) {
      case VectorEncoding::Simple::FLAT:
      case VectorEncoding::Simple::ROW:
      case VectorEncoding::Simple::ARRAY:
      case VectorEncoding::Simple::MAP:
      case VectorEncoding::Simple::FLAT_MAP:
      case VectorEncoding::Simple::FUNCTION: {
        result->ensureWritable(rows);
        return;
      }
      default:
        break; /** NOOP **/
    }
  }

  // Otherwise, allocate a new vector and copy the remaining values over.

  // The copy-on-write size is the max of the writable row set and the
  // vector.
  auto targetSize = std::max<vector_size_t>(rows.end(), result->size());

  VectorPtr copy;
  if (vectorPool) {
    copy = vectorPool->get(isUnknownType ? type : resultType, targetSize);
  } else {
    copy =
        BaseVector::create(isUnknownType ? type : resultType, targetSize, pool);
  }

  SelectivityVector copyRows(result->size());
  copyRows.deselect(rows);

  if (copyRows.hasSelections()) {
    copy->copy(result.get(), copyRows, nullptr);
  }
  result = std::move(copy);
}

template <TypeKind kind>
VectorPtr newConstant(
    const TypePtr& type,
    const Variant& value,
    vector_size_t size,
    velox::memory::MemoryPool* pool) {
  using T = typename KindToFlatVector<kind>::WrapperType;

  if (value.isNull()) {
    return std::make_shared<ConstantVector<T>>(pool, size, true, type, T());
  }

  T copy;
  if constexpr (std::is_same_v<T, StringView>) {
    copy = StringView(value.value<kind>());
  } else {
    copy = value.value<T>();
  }

  return std::make_shared<ConstantVector<T>>(
      pool, size, false, type, std::move(copy));
}

template <>
VectorPtr newConstant<TypeKind::OPAQUE>(
    const TypePtr& type,
    const Variant& value,
    vector_size_t size,
    velox::memory::MemoryPool* pool) {
  const auto& capsule = value.value<TypeKind::OPAQUE>();

  return std::make_shared<ConstantVector<std::shared_ptr<void>>>(
      pool, size, value.isNull(), type, std::shared_ptr<void>(capsule.obj));
}

// static
VectorPtr BaseVector::createConstant(
    const TypePtr& type,
    const Variant value,
    vector_size_t size,
    velox::memory::MemoryPool* pool) {
  VELOX_CHECK_EQ(type->kind(), value.kind());
  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
      newConstant, value.kind(), type, value, size, pool);
}

// static
std::vector<BaseVector::CopyRange> BaseVector::toCopyRanges(
    const SelectivityVector& rows) {
  if (rows.isAllSelected()) {
    return {{0, 0, rows.size()}};
  }

  std::vector<BaseVector::CopyRange> ranges;
  ranges.reserve(rows.end());

  vector_size_t prevRow = rows.begin();
  auto bits = rows.asRange().bits();
  bits::forEachUnsetBit(bits, rows.begin(), rows.end(), [&](vector_size_t row) {
    if (row > prevRow) {
      ranges.push_back({prevRow, prevRow, row - prevRow});
    }
    prevRow = row + 1;
  });

  if (rows.end() > prevRow) {
    ranges.push_back({prevRow, prevRow, rows.end() - prevRow});
  }

  return ranges;
}

void BaseVector::copy(
    const BaseVector* source,
    const SelectivityVector& rows,
    const vector_size_t* toSourceRow) {
  if (!rows.hasSelections()) {
    return;
  }
  std::vector<CopyRange> ranges;
  if (toSourceRow == nullptr) {
    VELOX_CHECK_GE(source->size(), rows.end());
    ranges = toCopyRanges(rows);
  } else {
    ranges.reserve(rows.end());
    rows.applyToSelected([&](vector_size_t row) {
      const auto sourceRow = toSourceRow[row];
      VELOX_DCHECK_GT(source->size(), sourceRow);
      ranges.push_back({sourceRow, row, 1});
    });
  }
  copyRanges(source, ranges);
}

namespace {

template <TypeKind kind>
VectorPtr newNullConstant(
    const TypePtr& type,
    vector_size_t size,
    velox::memory::MemoryPool* pool) {
  using T = typename KindToFlatVector<kind>::WrapperType;
  return std::make_shared<ConstantVector<T>>(pool, size, true, type, T());
}
} // namespace

VectorPtr BaseVector::createNullConstant(
    const TypePtr& type,
    vector_size_t size,
    velox::memory::MemoryPool* pool) {
  VELOX_CHECK_NOT_NULL(type, "Vector creation requires a non-null type.");
  return VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
      newNullConstant, type->kind(), type, size, pool);
}

// static
const VectorPtr& BaseVector::loadedVectorShared(const VectorPtr& vector) {
  if (vector->encoding() != VectorEncoding::Simple::LAZY) {
    // If 'vector' is a wrapper, we load any wrapped LazyVector.
    vector->loadedVector();
    return vector;
  }
  return vector->asUnchecked<LazyVector>()->loadedVectorShared();
}

// static
VectorPtr BaseVector::transpose(BufferPtr indices, VectorPtr&& source) {
  // TODO: Reuse the indices if 'source' is already a dictionary and
  // there are no other users of its indices.
  vector_size_t size = indices->size() / sizeof(vector_size_t);
  return wrapInDictionary(
      BufferPtr(nullptr), std::move(indices), size, std::move(source));
}

// static
const VectorPtr& BaseVector::wrappedVectorShared(const VectorPtr& vector) {
  switch (vector->encoding()) {
    case VectorEncoding::Simple::CONSTANT:
    case VectorEncoding::Simple::DICTIONARY:
    case VectorEncoding::Simple::SEQUENCE:
      return vector->valueVector() ? wrappedVectorShared(vector->valueVector())
                                   : vector;
    case VectorEncoding::Simple::LAZY:
      return wrappedVectorShared(loadedVectorShared(vector));
    default:
      return vector;
  }
}

bool isLazyNotLoaded(const BaseVector& vector) {
  switch (vector.encoding()) {
    case VectorEncoding::Simple::LAZY:
      if (!vector.asUnchecked<LazyVector>()->isLoaded()) {
        return true;
      }

      // Lazy Vectors may wrap lazy vectors (e.g. nested Rows) so we need to go
      // deeper.
      return isLazyNotLoaded(*vector.asUnchecked<LazyVector>()->loadedVector());
    case VectorEncoding::Simple::DICTIONARY:
      [[fallthrough]];
    case VectorEncoding::Simple::SEQUENCE:
      return isLazyNotLoaded(*vector.valueVector());
    case VectorEncoding::Simple::CONSTANT:
      return vector.valueVector() ? isLazyNotLoaded(*vector.valueVector())
                                  : false;
    case VectorEncoding::Simple::ROW:
      return vector.asUnchecked<RowVector>()->containsLazyNotLoaded();
    default:
      return false;
  }
}

uint64_t BaseVector::estimateFlatSize() const {
  if (length_ == 0) {
    return 0;
  }

  if (isLazyNotLoaded(*this)) {
    return 0;
  }

  auto leaf = wrappedVector();
  // If underlying vector is empty we should return the leaf's single element
  // size times this vector's size plus any nulls of this vector.
  if (UNLIKELY(leaf->size() == 0)) {
    const auto& leafType = leaf->type();
    return length_ *
        (leafType->isFixedWidth() ? leafType->cppSizeInBytes() : 0) +
        BaseVector::retainedSize();
  }

  auto avgRowSize = 1.0 * leaf->retainedSize() / leaf->size();
  return length_ * avgRowSize;
}

namespace {
bool isReusableEncoding(VectorEncoding::Simple encoding) {
  return encoding == VectorEncoding::Simple::FLAT ||
      encoding == VectorEncoding::Simple::ARRAY ||
      encoding == VectorEncoding::Simple::MAP ||
      encoding == VectorEncoding::Simple::ROW;
}
} // namespace

// static
void BaseVector::flattenVector(VectorPtr& vector) {
  if (!vector) {
    return;
  }
  switch (vector->encoding()) {
    case VectorEncoding::Simple::FLAT:
      return;
    case VectorEncoding::Simple::ROW: {
      auto* rowVector = vector->asUnchecked<RowVector>();
      for (auto& child : rowVector->children()) {
        BaseVector::flattenVector(child);
      }
      return;
    }
    case VectorEncoding::Simple::ARRAY: {
      auto* arrayVector = vector->asUnchecked<ArrayVector>();
      BaseVector::flattenVector(arrayVector->elements());
      return;
    }
    case VectorEncoding::Simple::MAP: {
      auto* mapVector = vector->asUnchecked<MapVector>();
      BaseVector::flattenVector(mapVector->mapKeys());
      BaseVector::flattenVector(mapVector->mapValues());
      return;
    }
    case VectorEncoding::Simple::LAZY: {
      auto& loadedVector =
          vector->asUnchecked<LazyVector>()->loadedVectorShared();
      BaseVector::flattenVector(loadedVector);
      return;
    }
    default:
      BaseVector::ensureWritable(
          SelectivityVector::empty(), vector->type(), vector->pool(), vector);
  }
}

void BaseVector::prepareForReuse(VectorPtr& vector, vector_size_t size) {
  if (vector.use_count() != 1 || !isReusableEncoding(vector->encoding())) {
    vector = BaseVector::create(vector->type(), size, vector->pool());
    return;
  }

  vector->prepareForReuse();
  vector->resize(size);
}

void BaseVector::reuseNulls() {
  // Check nulls buffer. Keep the buffer if singly-referenced and mutable and
  // there is at least one null bit set. Reset otherwise.
  if (nulls_) {
    if (nulls_->isMutable()) {
      if (BaseVector::countNulls(nulls_, length_) == 0) {
        nulls_ = nullptr;
        rawNulls_ = nullptr;
      }
    } else {
      nulls_ = nullptr;
      rawNulls_ = nullptr;
    }
  }
}

void BaseVector::prepareForReuse() {
  reuseNulls();
  this->resetDataDependentFlags(nullptr);
}

void BaseVector::validate(const VectorValidateOptions& options) const {
  if (nulls_ != nullptr) {
    auto bytes = byteSize<bool>(size());
    VELOX_CHECK_GE(nulls_->size(), bytes);
  }
  if (options.callback) {
    options.callback(*this);
  }
}

std::optional<vector_size_t> BaseVector::findDuplicateValue(
    vector_size_t start,
    vector_size_t size,
    CompareFlags flags) {
  if (length_ == 0 || size == 0) {
    return std::nullopt;
  }

  VELOX_DCHECK_GE(start, 0, "Start index must not be negative");
  VELOX_DCHECK_LT(start, length_, "Start index is too large");
  VELOX_DCHECK_GT(size, 0, "Size must not be negative");
  VELOX_DCHECK_LE(start + size, length_, "Size is too large");

  std::vector<vector_size_t> indices(size);
  std::iota(indices.begin(), indices.end(), start);
  sortIndices(indices, flags);

  for (auto i = 1; i < size; ++i) {
    if (equalValueAt(this, indices[i], indices[i - 1])) {
      return indices[i];
    }
  }

  return std::nullopt;
}

std::string printNulls(const BufferPtr& nulls, vector_size_t maxBitsToPrint) {
  VELOX_CHECK_GE(maxBitsToPrint, 0);

  vector_size_t totalCount = nulls->size() * 8;
  auto* rawNulls = nulls->as<uint64_t>();
  auto nullCount = bits::countNulls(rawNulls, 0, totalCount);

  std::stringstream out;
  out << nullCount << " out of " << totalCount << " rows are null";

  if (nullCount) {
    out << ": ";
    for (auto i = 0; i < maxBitsToPrint && i < totalCount; ++i) {
      out << (bits::isBitNull(rawNulls, i) ? "n" : ".");
    }
  }

  return out.str();
}

std::string printIndices(
    const BufferPtr& indices,
    vector_size_t maxIndicesToPrint) {
  VELOX_CHECK_GE(maxIndicesToPrint, 0);

  auto* rawIndices = indices->as<vector_size_t>();

  vector_size_t size = indices->size() / sizeof(vector_size_t);

  std::unordered_set<vector_size_t> uniqueIndices;
  for (auto i = 0; i < size; ++i) {
    uniqueIndices.insert(rawIndices[i]);
  }

  std::stringstream out;
  out << uniqueIndices.size() << " unique indices out of " << size << ": ";
  for (auto i = 0; i < maxIndicesToPrint && i < size; ++i) {
    if (i > 0) {
      out << ", ";
    }
    out << rawIndices[i];
  }

  return out.str();
}

// static
void BaseVector::transposeIndices(
    const vector_size_t* baseIndices,
    vector_size_t wrapSize,
    const vector_size_t* wrapIndices,
    vector_size_t* resultIndices) {
#if XSIMD_WITH_AVX2

  constexpr int32_t kBatch = xsimd::batch<int32_t>::size;
  static_assert(kBatch == 8);
  static_assert(sizeof(vector_size_t) == sizeof(int32_t));
  int32_t i = 0;
  for (; i + kBatch <= wrapSize; i += kBatch) {
    auto indexBatch = xsimd::load_unaligned(wrapIndices + i);
    simd::gather(baseIndices, indexBatch).store_unaligned(resultIndices + i);
  }
  if (i < wrapSize) {
    auto indexBatch = xsimd::load_unaligned(wrapIndices + i);
    auto mask = simd::leadingMask<int32_t>(wrapSize - i);
    simd::maskGather(
        xsimd::batch<int32_t>::broadcast(0), mask, baseIndices, indexBatch)
        .store_unaligned(resultIndices + i);
  }
#else
  VELOX_NYI();
#endif
}

// static
void BaseVector::transposeIndicesWithNulls(
    const vector_size_t* baseIndices,
    const uint64_t* baseNulls,
    vector_size_t wrapSize,
    const vector_size_t* wrapIndices,
    const uint64_t* wrapNulls,
    vector_size_t* resultIndices,
    uint64_t* resultNulls) {
#if XSIMD_WITH_AVX2

  constexpr int32_t kBatch = xsimd::batch<int32_t>::size;
  static_assert(kBatch == 8);
  static_assert(sizeof(vector_size_t) == sizeof(int32_t));
  for (auto i = 0; i < wrapSize; i += kBatch) {
    auto indexBatch = xsimd::load_unaligned(wrapIndices + i);
    uint8_t wrapNullsByte =
        i + kBatch > wrapSize ? bits::lowMask(wrapSize - i) : 0xff;
    if (wrapNulls) {
      wrapNullsByte &= reinterpret_cast<const uint8_t*>(wrapNulls)[i / 8];
    }
    if (wrapNullsByte != 0xff) {
      // Zero out indices at null positions.
      auto mask = simd::fromBitMask<int32_t>(wrapNullsByte);
      indexBatch = indexBatch &
          xsimd::load_unaligned(reinterpret_cast<const vector_size_t*>(&mask));
    }
    if (baseNulls) {
      uint8_t baseNullBits = simd::gather8Bits(baseNulls, indexBatch, 8);
      wrapNullsByte &= baseNullBits;
    }
    reinterpret_cast<uint8_t*>(resultNulls)[i / 8] = wrapNullsByte;
    simd::gather<int32_t>(baseIndices, indexBatch)
        .store_unaligned(resultIndices + i);
  }
#else
  VELOX_NYI();
#endif
}

// static
void BaseVector::transposeDictionaryValues(
    vector_size_t wrapSize,
    BufferPtr& wrapNulls,
    BufferPtr& wrapIndices,
    std::shared_ptr<BaseVector>& dictionaryValues) {
  if (!wrapIndices->unique()) {
    wrapIndices = AlignedBuffer::copy(dictionaryValues->pool(), wrapIndices);
  }
  auto* rawBaseNulls = dictionaryValues->rawNulls();
  auto baseIndices = dictionaryValues->wrapInfo();
  if (!rawBaseNulls && !wrapNulls) {
    transposeIndices(
        baseIndices->as<vector_size_t>(),
        wrapSize,
        wrapIndices->as<vector_size_t>(),
        wrapIndices->asMutable<vector_size_t>());
  } else {
    BufferPtr newNulls;
    if (!wrapNulls || !wrapNulls->unique()) {
      newNulls = AlignedBuffer::allocate<bool>(
          wrapSize, dictionaryValues->pool(), bits::kNull);
    } else {
      newNulls = wrapNulls;
    }
    transposeIndicesWithNulls(
        baseIndices->as<vector_size_t>(),
        rawBaseNulls,
        wrapSize,
        wrapIndices->as<vector_size_t>(),
        wrapNulls ? wrapNulls->as<uint64_t>() : nullptr,
        wrapIndices->asMutable<vector_size_t>(),
        newNulls->asMutable<uint64_t>());
  }
  dictionaryValues = dictionaryValues->valueVector();
}

template <TypeKind Kind>
bool isAllSameFlat(const BaseVector& vector, vector_size_t size) {
  using T = typename KindToFlatVector<Kind>::WrapperType;
  auto flat = vector.asUnchecked<FlatVector<T>>();
  auto rawValues = flat->rawValues();
  if (vector.size() == 0) {
    return false;
  }
  T first = rawValues[0];
  for (auto i = 1; i < size; ++i) {
    if (first != rawValues[i]) {
      return false;
    }
  }
  return true;
}

template <>
bool isAllSameFlat<TypeKind::BOOLEAN>(
    const BaseVector& vector,
    vector_size_t size) {
  auto& values = vector.asUnchecked<FlatVector<bool>>()->values();
  auto* bits = values->as<uint64_t>();
  // Check the all true and all false separately. Easier for compiler if the
  // last argument is constant.
  if ((bits[0] & 1) == 1) {
    return bits ::isAllSet(bits, 0, size, true);
  }
  return bits ::isAllSet(bits, 0, size, false);
}

// static
VectorPtr BaseVector::constantify(const VectorPtr& input, DecodedVector* temp) {
  auto& vector = BaseVector::loadedVectorShared(input);

  // If this is already a constant or empty or single element, it can stay as
  // is.
  if (vector->encoding() == VectorEncoding::Simple::CONSTANT ||
      vector->size() < 2) {
    return nullptr;
  }
  // If there is a null, values will either not all be the same or all be null,
  // which can just as well be left as is.
  if (vector->isNullAt(0)) {
    return nullptr;
  }
  // Quick return if first and last are different.
  if (!vector->equalValueAt(vector.get(), 0, vector->size() - 1)) {
    return nullptr;
  }
  DecodedVector localDecoded;
  DecodedVector* decoded = temp ? temp : &localDecoded;
  decoded->decode(*vector);
  if (!decoded->isIdentityMapping()) {
    auto indices = decoded->indices();
    auto first = indices[0];
    for (auto i = 1; i < vector->size(); ++i) {
      if (indices[i] != first) {
        if (decoded->isNullAt(i)) {
          return nullptr;
        }
        if (!decoded->base()->equalValueAt(
                decoded->base(), first, indices[i])) {
          return nullptr;
        }
      }
    }
    return BaseVector::wrapInConstant(vector->size(), 0, vector);
  }
  if (vector->mayHaveNulls()) {
    return nullptr;
  }
  if (vector->encoding() == VectorEncoding::Simple::FLAT) {
    if (!VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
            isAllSameFlat, vector->typeKind(), *vector, vector->size() - 1)) {
      return nullptr;
    }
  } else {
    for (auto i = 1; i < vector->size() - 1; ++i) {
      if (!vector->equalValueAt(vector.get(), 0, i)) {
        return nullptr;
      }
    }
  }

  return BaseVector::wrapInConstant(vector->size(), 0, vector);
}

} // namespace facebook::velox
