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

#include <string>
#include <vector>
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/Status.h"
#include "velox/type/Timestamp.h"

namespace facebook::velox::functions {

enum class DateTimeFormatterType {
  JODA,
  MYSQL,
  // Corresponding to java.text.SimpleDateFormat in lenient mode. It is used by
  // the 'date_format', 'from_unixtime', 'unix_timestamp' and
  // 'to_unix_timestamp' Spark functions.
  // TODO: this is currently no different from STRICT_SIMPLE.
  LENIENT_SIMPLE,
  // Corresponding to java.text.SimpleDateFormat in strict(lenient=false) mode.
  // It is used by Spark 'cast date to string'.
  STRICT_SIMPLE,
  UNKNOWN
};

enum class DateTimeFormatSpecifier : uint8_t {
  // Era, e.g: "AD"
  ERA = 0,

  // Century of era (>=0), e.g: 20
  CENTURY_OF_ERA = 1,

  // Year of era (>=0), e.g: 1996
  YEAR_OF_ERA = 2,

  // Week year based on ISO week date, e.g: 1996
  WEEK_YEAR = 3,

  // Week of week year based on ISO week date, e.g: 27
  WEEK_OF_WEEK_YEAR = 4,

  // Day of week, 0 ~ 6 with 0 representing Sunday
  DAY_OF_WEEK_0_BASED = 5,

  // Day of week, 1 ~ 7
  DAY_OF_WEEK_1_BASED = 6,

  // Day of week, e.g: "Tuesday" or "Tue", depending on number of times provided
  // formatter character repeats
  DAY_OF_WEEK_TEXT = 7,

  // Year, can be negative e.g: 1996, -2000
  YEAR = 8,

  // Day of year, 1 ~ 366 e.g: 189
  DAY_OF_YEAR = 9,

  // Month of year, e.g: 07, or 7 depending on number of times provided
  // formatter character repeats
  MONTH_OF_YEAR = 10,

  // Month of year, e.g. Dec, December depending on number of times provided
  // formatter character repeats
  MONTH_OF_YEAR_TEXT = 11,

  // Day of month, e.g: 10, 01, 001, with/without padding 0s depending on number
  // of times provided formatter character repeats
  DAY_OF_MONTH = 12,

  // Halfday of day, e.g: "PM"
  HALFDAY_OF_DAY = 13,

  // Hour of halfday (0~11)
  HOUR_OF_HALFDAY = 14,

  // Clockhour of halfday (1~12)
  CLOCK_HOUR_OF_HALFDAY = 15,

  // Hour of day (0~23)
  HOUR_OF_DAY = 16,

  // Clockhour of day (1~24)
  CLOCK_HOUR_OF_DAY = 17,

  // Minute of hour, e.g: 30
  MINUTE_OF_HOUR = 18,

  // Second of minute, e.g: 55
  SECOND_OF_MINUTE = 19,

  // Decimal fraction of a second, e.g: The fraction of 00:00:01.987 is 987
  FRACTION_OF_SECOND = 20,

  // Timezone, e.g: "Pacific Standard Time" or "PST"
  TIMEZONE = 21,

  // Timezone offset/id, e.g: "-0800", "-08:00" or "America/Los_Angeles"
  TIMEZONE_OFFSET_ID = 22,

  // A literal % character
  LITERAL_PERCENT = 23,

  // Week of month based on java.text.SimpleDateFormat, e.g: 2
  WEEK_OF_MONTH = 24
};

enum class DateTimeUnit {
  kMicrosecond,
  kMillisecond,
  kSecond,
  kMinute,
  kHour,
  kDay,
  kWeek,
  kMonth,
  kQuarter,
  kYear
};

FOLLY_ALWAYS_INLINE bool isTimeUnit(const DateTimeUnit unit) {
  return unit < DateTimeUnit::kDay;
}

struct FormatPattern {
  DateTimeFormatSpecifier specifier;

  // The minimum number of digits the formatter is going to use to represent a
  // field. The formatter is assumed to use as few digits as possible for the
  // representation. E.g: For text representation of March, with
  // minRepresentDigits being 2 or 3 it will be 'Mar'. And with
  // minRepresentDigits being 4 or 5 it will be 'March'.
  size_t minRepresentDigits;
};

struct DateTimeToken {
  enum class Type { kPattern, kLiteral } type;
  union {
    FormatPattern pattern;
    std::string_view literal;
  };

  explicit DateTimeToken(const FormatPattern& pattern)
      : type(Type::kPattern), pattern(pattern) {}

  explicit DateTimeToken(const std::string_view& literal)
      : type(Type::kLiteral), literal(literal) {}

  bool operator==(const DateTimeToken& right) const {
    if (type == right.type) {
      if (type == Type::kLiteral) {
        return literal == right.literal;
      } else {
        return pattern.specifier == right.pattern.specifier &&
            pattern.minRepresentDigits == right.pattern.minRepresentDigits;
      }
    }
    return false;
  }
};

struct DateTimeResult {
  Timestamp timestamp;
  const tz::TimeZone* timezone = nullptr;
};

/// A user defined formatter that formats/parses time to/from user provided
/// format. User can use DateTimeFormatterBuilder to build desired formatter.
/// E.g. In MySQL standard a formatter will have '%Y' '%d' and etc. as its
/// specifiers. But in Joda standard a formatter will have 'YYYY' 'dd' and etc.
/// as its specifiers. Both standards can be configured using this formatter.
class DateTimeFormatter {
 public:
  explicit DateTimeFormatter(
      std::unique_ptr<char[]>&& literalBuf,
      size_t bufSize,
      std::vector<DateTimeToken>&& tokens,
      DateTimeFormatterType type)
      : literalBuf_(std::move(literalBuf)),
        bufSize_(bufSize),
        tokens_(std::move(tokens)),
        type_(type) {}

  const std::unique_ptr<char[]>& literalBuf() const {
    return literalBuf_;
  }

  size_t bufSize() const {
    return bufSize_;
  }

  const std::vector<DateTimeToken>& tokens() const {
    return tokens_;
  }

  // Returns an Expected<DateTimeResult> object containing the parsed
  // Timestamp and timezone information if parsing succeeded. Otherwise,
  // Returns Unexpected with UserError status if parsing failed.
  Expected<DateTimeResult> parse(const std::string_view& input) const;

  /// Returns max size of the formatted string. Can be used to preallocate
  /// memory before calling format() to avoid extra copy.
  uint32_t maxResultSize(const tz::TimeZone* timezone) const;

  /// Result buffer is pre-allocated according to maxResultSize.
  /// Returns actual size.
  ///
  /// The timestamp will be firstly converted to millisecond then to
  /// std::chrono::time_point. If allowOverflow is true, integer overflow is
  /// allowed in converting to milliseconds. If zeroOffsetText is set, that
  /// string will be used to represent the zero offset timezone, other time
  /// zones will still be represented based on the pattern this was initialized
  /// with.
  int32_t format(
      const Timestamp& timestamp,
      const tz::TimeZone* timezone,
      const uint32_t maxResultSize,
      char* result,
      bool allowOverflow = false,
      const std::optional<std::string>& zeroOffsetText = std::nullopt) const;

 private:
  std::unique_ptr<char[]> literalBuf_;
  size_t bufSize_;
  std::vector<DateTimeToken> tokens_;
  DateTimeFormatterType type_;
};

Expected<std::shared_ptr<DateTimeFormatter>> buildMysqlDateTimeFormatter(
    const std::string_view& format);

Expected<std::shared_ptr<DateTimeFormatter>> buildJodaDateTimeFormatter(
    const std::string_view& format);

Expected<std::shared_ptr<DateTimeFormatter>> buildSimpleDateTimeFormatter(
    const std::string_view& format,
    bool lenient);

} // namespace facebook::velox::functions

template <>
struct fmt::formatter<facebook::velox::functions::DateTimeFormatterType>
    : formatter<int> {
  auto format(
      facebook::velox::functions::DateTimeFormatterType s,
      format_context& ctx) const {
    return formatter<int>::format(static_cast<int>(s), ctx);
  }
};

template <>
struct fmt::formatter<facebook::velox::functions::DateTimeFormatSpecifier>
    : formatter<int> {
  auto format(
      facebook::velox::functions::DateTimeFormatSpecifier s,
      format_context& ctx) const {
    return formatter<int>::format(static_cast<int>(s), ctx);
  }
};
