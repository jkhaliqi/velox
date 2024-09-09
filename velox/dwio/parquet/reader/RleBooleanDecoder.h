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
// #include <arrow/util/rle_encoding.h> // @manual
// #include <parquet/encoding.h>

namespace facebook::velox::parquet {

class RleBooleanDecoder {
 public:
  RleBooleanDecoder(const char* start, const char* end, BufferPtr decompressedData_)
      : bufferStart_(start), bufferEnd_(end) {
            
            const uint8_t* data = decompressedData_->asMutable<uint8_t>(); 
            int64_t size = decompressedData_->size();

            // Print each byte in the buffer
            for (int64_t i = 0; i < size; ++i) {
                std::cout << "Byte " << i << ": " << static_cast<int>(data[i]) << std::endl;

                // iterate through bits
                for (int bit = 0; bit < 8; ++bit) {
                    bool value = (data[i] & (1 << bit)) != 0;
                    std::cout << "Bit " << (i * 8 + bit) << ": " << (value ? "true" : "false") << " ";
                }
                std::cout << std::endl;
            }
        const uint8_t* rleData = reinterpret_cast<const uint8_t*>(bufferStart_);
      // You would need the number of bytes in the RLE encoded data.
      size_t numBytes = end - start;  // Adjust this based on actual data.
      // You might need to calculate the bit width as needed for booleans.
      int bitWidth = 1;  // For boolean values.

      // // Initialize the RLE decoder.
      // decoder_ = std::make_shared<::arrow::util::RleDecoder>(
      //     rleData, numBytes, bitWidth);
      }

  // void setData(int num_values, const uint8_t* data, int len) {
  //   num_values_ = num_values;
  //   uint32_t num_bytes = 0;

  //   VELOX_CHECK_LT(len,
  //   4,
  //   "Received invalid length : " + std::to_string(len) + " (corrupt data page?)");

  //   // Load the first 4 bytes in little-endian, which indicates the length
  //   num_bytes = ::arrow::bit_util::FromLittleEndian(::arrow::util::SafeLoadAs<uint32_t>(data));
  //   VELOX_CHECK_LT(num_bytes,
  //   0,
  //   "Received invalid number of bytes : " + std::to_string(num_bytes) + " (corrupt data page?)");
  //   VELOX_CHECK_GT(num_bytes,
  //   static_cast<uint32_t>(len - 4),
  //   "Received invalid number of bytes : " + std::to_string(num_bytes) + " (corrupt data page?)");
  //   auto decoder_data = data + 4;
  //   if (decoder_ == nullptr) {
  //     decoder_ = std::make_shared<::arrow::util::RleDecoder>(decoder_data, num_bytes,
  //                                                            /*bit_width=*/1);
  //   } else {
  //     decoder_->Reset(decoder_data, num_bytes, /*bit_width=*/1);
  //   }
  // }
  //   int Decode(bool* buffer, int max_values) {
  //   max_values = std::min(max_values, num_values_);

  //   if (decoder_->GetBatch(buffer, max_values) != max_values) {
  //     VELOX_FAIL("End of file exception");
  //   }
  //   num_values_ -= max_values;
  //   return max_values;
  // }

  // int Decode(uint8_t* buffer, int max_values) {
  //   VELOX_NYI("Decode(uint8_t*, int) for RleBooleanDecoder");
  // }

  // int DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
  //                 int64_t valid_bits_offset,
  //                 typename EncodingTraits<BooleanType>::Accumulator* out) {
  //   if (null_count == num_values) {
  //     VELOX_CHECK(out->AppendNulls(null_count));
  //     return 0;
  //   }
  //   constexpr int kBatchSize = 1024;
  //   std::array<bool, kBatchSize> values;
  //   const int num_non_null_values = num_values - null_count;
  //   // Remaining non-null boolean values to read from decoder.
  //   // We decode from `decoder_` with maximum 1024 size batches.
  //   int num_remain_non_null_values = num_non_null_values;
  //   int current_index_in_batch = 0;
  //   int current_batch_size = 0;
  //   auto next_boolean_batch = [&]() {
  //     DCHECK_GT(num_remain_non_null_values, 0);
  //     DCHECK_EQ(current_index_in_batch, current_batch_size);
  //     current_batch_size = std::min(num_remain_non_null_values, kBatchSize);
  //     int decoded_count = decoder_->GetBatch(values.data(), current_batch_size);
  //     if (ARROW_PREDICT_FALSE(decoded_count != current_batch_size)) {
  //       // required values is more than values in decoder.
  //       VELOX_FAIL("End of file exception");
  //     }
  //     num_remain_non_null_values -= current_batch_size;
  //     current_index_in_batch = 0;
  //   };

  //   // Reserve all values including nulls first
  //   VELOX_CHECK(out->Reserve(num_values));
  //   if (null_count == 0) {
  //     // Fast-path for not having nulls.
  //     do {
  //       next_boolean_batch();
  //       VELOX_CHECK(
  //           out->AppendValues(values.begin(), values.begin() + current_batch_size));
  //       num_values -= current_batch_size;
  //       // set current_index_in_batch to current_batch_size means
  //       // the whole batch is totally consumed.
  //       current_index_in_batch = current_batch_size;
  //     } while (num_values > 0);
  //     return num_non_null_values;
  //   }
  //   auto next_value = [&]() -> bool {
  //     if (current_index_in_batch == current_batch_size) {
  //       next_boolean_batch();
  //       DCHECK_GT(current_batch_size, 0);
  //     }
  //     DCHECK_LT(current_index_in_batch, current_batch_size);
  //     bool value = values[current_index_in_batch];
  //     ++current_index_in_batch;
  //     return value;
  //   };
  //   // VisitNullBitmapInline(
  //   //     valid_bits, valid_bits_offset, num_values, null_count,
  //   //     [&]() { out->UnsafeAppend(next_value()); }, [&]() { out->UnsafeAppendNull(); });
  //   return num_non_null_values;
  // }

  // template <typename ValidFunc, typename NullFunc>
  // typename internal::call_traits::enable_if_return<ValidFunc, void>::type
  // VisitNullBitmapInline(const uint8_t* valid_bits, int64_t valid_bits_offset,
  //                       int64_t num_values, int64_t null_count, ValidFunc&& valid_func,
  //                       NullFunc&& null_func) {
  //   internal::OptionalBitBlockCounter bit_counter(null_count == 0 ? NULLPTR : valid_bits,
  //                                                 valid_bits_offset, num_values);
  //   int64_t position = 0;
  //   int64_t offset_position = valid_bits_offset;
  //   while (position < num_values) {
  //     internal::BitBlockCount block = bit_counter.NextBlock();
  //     if (block.AllSet()) {
  //       for (int64_t i = 0; i < block.length; ++i) {
  //         valid_func();
  //       }
  //     } else if (block.NoneSet()) {
  //       for (int64_t i = 0; i < block.length; ++i) {
  //         null_func();
  //       }
  //     } else {
  //       for (int64_t i = 0; i < block.length; ++i) {
  //         bit_util::GetBit(valid_bits, offset_position + i) ? valid_func() : null_func();
  //       }
  //     }
  //     position += block.length;
  //     offset_position += block.length;
  //   }
  // }

  //Repetitive unless we want to call this again without creating class so can just call setData instead of class??can delete tho...
  void SetData(const char* start, const char* end) {
    // VELOX_CHECK_LE(
    //   end-start,
    //   0,
    //   "Received invalid length: " + std::to_string(end - start) + " (corrupt data page?)");
    bufferStart_ = start;
    bufferEnd_ = end;
  }

  void skip(uint64_t numValues) {
    skip<false>(numValues, 0, nullptr);
  }

  template <bool hasNulls>
  inline void skip(int32_t numValues, int32_t current, const uint64_t* nulls) {
    // values stored as bytes together so no need to go into the bits...
    // just look at each byte as it is...
    while (numValues > 0) {
      if (remainingCount_ > 0) {
        if (numValues >= remainingCount_) {
          numValues -= remainingCount_;
          remainingCount_ = 0;
          decode(); // move to the next run
        } else {
          remainingCount_ -= numValues;
          numValues = 0; // All values skipped
        }
      } else {
        decode(); // Prepare the next run
      }
    }
  }

  template <bool hasNulls, typename Visitor>
  void readWithVisitor(const uint64_t* nulls, Visitor visitor) {
    // std::vector<T> outputValues_;
    // outputValues_.resize(8, 0); // we have 8 falses...
    // T* output = outputValues_.data();
    // arrow::util::RleDecoder arrowDecoder(
    //     reinterpret_cast<uint8_t*>(encodedValues_.data()),
    //     1000,
    //     1);
    // int numOfDecodedElements = arrowDecoder.GetBatch(output, numValues_);

    // IF SETTING THE DATA TO THE BELOW WE CAN SEE THAT IT ENCODES THINGS CORRECTLY(Not a case for nulls yet)...
    // BUT SOMEHOW THE BUFFER VALUES COMING IN FROM THE PAGE DATA IS INCORRECT SO IT DOES NOT DO IT WHAT WE WANT FOR THAT CASE....
    // std::vector<unsigned char> buffer = { 3, 1, 2, 0, 1, 1 }; // 3 true, 2 false, 1 true
    // std::cout << "Buffer contents:" << std::endl;
    // for (size_t i = 0; i < buffer.size(); ++i) {
    //     std::cout << "Buffer[" << i << "] = " << static_cast<int>(buffer[i]) << std::endl;
    // }
    // const char* start = reinterpret_cast<const char*>(buffer.data());
    // const char* end = start + buffer.size();
    // SetData(start, end);
    int32_t current = visitor.start();
    // POINTER IS STILL THE SAME SPOT...
    // std::cout << "Start address: " << static_cast<const void*>(bufferStart_) << std::endl;
    // std::cout << "End address: " << static_cast<const void*>(bufferEnd_) << std::endl;
    skip<hasNulls>(current, 0, nulls);
    // std::cout << "Start address: " << static_cast<const void*>(bufferStart_) << std::endl;
    // std::cout << "End address: " << static_cast<const void*>(bufferEnd_) << std::endl;
    
    while (true) {
      bool atEnd = (remainingCount_ == 0 && bufferStart_ >= bufferEnd_);
      if (atEnd) {
        return; // End of data
      }

      if (remainingCount_ == 0) {
        decode(); // Load the next run if needed
        if (remainingCount_ == 0) {
          return; // Still no more data
        }
      }

      bool value = currentValue_; // Get the current boolean value
      --remainingCount_; // Decrease the count for the current run

      // Process the value using the visitor, passing `atEnd`
      int32_t toSkip = visitor.process(value, atEnd);
      current += toSkip;

      // Skip additional values if needed
      if (toSkip > 0) {
        skip<hasNulls>(toSkip, current, nulls);
      }
    }
  }

 private:
  // bool readBoolean() {
  //   if (remainingBits_ == 0) {
  //     remainingBits_ = 7;
  //     reversedLastByte_ = *reinterpret_cast<const uint8_t*>(bufferStart_);
  //     bufferStart_++;
  //     return reversedLastByte_ & 0x1;
  //   } else {
  //     return reversedLastByte_ & (1 << (8 - (remainingBits_--)));
  //   }
  // }

  void decode() {
    if (bufferStart_ >= bufferEnd_) {
      remainingCount_ = 0; // No more runs
      return;
    }

    // Read the run length (first byte)
    uint8_t runLength = *bufferStart_++;
    std::cout << "Read runLength: " << static_cast<int>(runLength) << std::endl;

    
    // Read the boolean value (next byte)
    if (bufferStart_ >= bufferEnd_) {
      remainingCount_ = 0; // Not enough data for value
      return;
    }

    currentValue_ = (*bufferStart_++ != 0); // 0 for false, non-zero for true
    std::cout << "Read currentValue: " << currentValue_ << std::endl;
    remainingCount_ = runLength; // Set the remaining count
  }

  size_t remainingCount_{0}; // Count of remaining booleans in the current run
  bool currentValue_{true}; // Current boolean value being decoded
  const char* bufferStart_;
  const char* bufferEnd_;
  // int32_t num_values_;
  // std::shared_ptr<::arrow::util::RleDecoder> decoder_;
};

} // namespace facebook::velox::parquet