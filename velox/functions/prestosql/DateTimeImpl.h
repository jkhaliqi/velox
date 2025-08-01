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

#include <boost/numeric/conversion/cast.hpp>
#include <chrono>
#include <optional>
#include "velox/common/base/Doubles.h"
#include "velox/external/date/date.h"
#include "velox/functions/lib/DateTimeFormatter.h"
#include "velox/functions/lib/DateTimeUtil.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/type/Timestamp.h"
#include "velox/type/TimestampConversion.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox::functions {
namespace {
constexpr double kNanosecondsInSecond = 1'000'000'000;
constexpr int64_t kNanosecondsInMillisecond = 1'000'000;
constexpr int64_t kMillisecondsInSecond = 1'000;
} // namespace

FOLLY_ALWAYS_INLINE double toUnixtime(const Timestamp& timestamp) {
  double result = timestamp.getSeconds();
  result += static_cast<double>(timestamp.getNanos()) / kNanosecondsInSecond;
  return result;
}

FOLLY_ALWAYS_INLINE Timestamp fromUnixtime(double unixtime) {
  if (FOLLY_UNLIKELY(std::isnan(unixtime))) {
    return Timestamp(0, 0);
  }

  static const int64_t kMin = std::numeric_limits<int64_t>::min();

  if (FOLLY_UNLIKELY(unixtime >= kMinDoubleAboveInt64Max)) {
    return Timestamp::maxMillis();
  }

  if (FOLLY_UNLIKELY(unixtime <= kMin)) {
    return Timestamp::minMillis();
  }

  if (FOLLY_UNLIKELY(std::isinf(unixtime))) {
    return unixtime < 0 ? Timestamp::minMillis() : Timestamp::maxMillis();
  }

  auto seconds = std::floor(unixtime);
  auto milliseconds = std::llround((unixtime - seconds) * kMillisInSecond);
  if (FOLLY_UNLIKELY(milliseconds == kMillisInSecond)) {
    ++seconds;
    milliseconds = 0;
  }
  return Timestamp(seconds, milliseconds * kNanosecondsInMillisecond);
}

FOLLY_ALWAYS_INLINE boost::int64_t fromUnixtime(
    double unixtime,
    int16_t timeZoneId) {
  if (FOLLY_UNLIKELY(std::isnan(unixtime))) {
    return pack(0, timeZoneId);
  }

  static const int64_t kMin = std::numeric_limits<int64_t>::min();

  if (FOLLY_UNLIKELY(unixtime >= kMinDoubleAboveInt64Max)) {
    return pack(std::numeric_limits<int64_t>::max(), timeZoneId);
  }

  if (FOLLY_UNLIKELY(unixtime <= kMin)) {
    return pack(std::numeric_limits<int64_t>::min(), timeZoneId);
  }

  if (FOLLY_UNLIKELY(std::isinf(unixtime))) {
    return unixtime < 0 ? pack(std::numeric_limits<int64_t>::min(), timeZoneId)
                        : pack(std::numeric_limits<int64_t>::max(), timeZoneId);
  }

  return pack(std::llround(unixtime * kMillisecondsInSecond), timeZoneId);
}

// Year, quarter or month are not uniformly incremented. Months have different
// total days, and leap years have more days than the rest. If the new year,
// quarter or month has less total days than the given one, it will be coerced
// to use the valid last day of the new month. This could result in weird
// arithmetic behavior. For example,
//
// 2022-01-30 + (1 month) = 2022-02-28
// 2022-02-28 - (1 month) = 2022-01-28
//
// 2022-08-31 + (1 quarter) = 2022-11-30
// 2022-11-30 - (1 quarter) = 2022-08-30
//
// 2020-02-29 + (1 year) = 2021-02-28
// 2021-02-28 - (1 year) = 2020-02-28
FOLLY_ALWAYS_INLINE
int32_t
addToDate(const int32_t input, const DateTimeUnit unit, const int32_t value) {
  // TODO(gaoge): Handle overflow and underflow with 64-bit representation
  if (value == 0) {
    return input;
  }

  const std::chrono::time_point<std::chrono::system_clock, date::days> inDate{
      date::days(input)};
  std::chrono::time_point<std::chrono::system_clock, date::days> outDate;

  if (unit == DateTimeUnit::kDay) {
    outDate = inDate + date::days(value);
  } else if (unit == DateTimeUnit::kWeek) {
    outDate = inDate + date::days(value * 7);
  } else {
    const date::year_month_day inCalDate(inDate);
    date::year_month_day outCalDate;

    if (unit == DateTimeUnit::kMonth) {
      outCalDate = inCalDate + date::months(value);
    } else if (unit == DateTimeUnit::kQuarter) {
      outCalDate = inCalDate + date::months(3 * value);
    } else if (unit == DateTimeUnit::kYear) {
      outCalDate = inCalDate + date::years(value);
    } else {
      VELOX_UNREACHABLE();
    }

    if (!outCalDate.ok()) {
      outCalDate = outCalDate.year() / outCalDate.month() / date::last;
    }
    outDate = date::sys_days{outCalDate};
  }

  return outDate.time_since_epoch().count();
}

FOLLY_ALWAYS_INLINE Timestamp addToTimestamp(
    const Timestamp& timestamp,
    const DateTimeUnit unit,
    const int32_t value) {
  // TODO(gaoge): Handle overflow and underflow with 64-bit representation
  if (value == 0) {
    return timestamp;
  }

  const std::chrono::
      time_point<std::chrono::system_clock, std::chrono::milliseconds>
          inTimestamp(std::chrono::milliseconds(timestamp.toMillis()));
  std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>
      outTimestamp;

  switch (unit) {
    // Year, quarter or month are not uniformly incremented in terms of number
    // of days. So we treat them differently.
    case DateTimeUnit::kYear:
    case DateTimeUnit::kQuarter:
    case DateTimeUnit::kMonth:
    case DateTimeUnit::kDay: {
      const int32_t inDate =
          std::chrono::duration_cast<date::days>(inTimestamp.time_since_epoch())
              .count();
      const int32_t outDate = addToDate(inDate, unit, value);
      outTimestamp =
          inTimestamp + date::days(checkedMinus<int32_t>(outDate, inDate));
      break;
    }
    case DateTimeUnit::kHour: {
      outTimestamp = inTimestamp + std::chrono::hours(value);
      break;
    }
    case DateTimeUnit::kMinute: {
      outTimestamp = inTimestamp + std::chrono::minutes(value);
      break;
    }
    case DateTimeUnit::kSecond: {
      outTimestamp = inTimestamp + std::chrono::seconds(value);
      break;
    }
    case DateTimeUnit::kMillisecond: {
      outTimestamp = inTimestamp + std::chrono::milliseconds(value);
      break;
    }
    case DateTimeUnit::kWeek: {
      const int32_t inDate =
          std::chrono::duration_cast<date::days>(inTimestamp.time_since_epoch())
              .count();
      const int32_t outDate = addToDate(inDate, DateTimeUnit::kDay, 7 * value);

      outTimestamp = inTimestamp + date::days(outDate - inDate);
      break;
    }
    default:
      VELOX_UNREACHABLE("Unsupported datetime unit");
  }

  Timestamp milliTimestamp =
      Timestamp::fromMillis(outTimestamp.time_since_epoch().count());
  return Timestamp(
      milliTimestamp.getSeconds(),
      milliTimestamp.getNanos() +
          timestamp.getNanos() % kNanosecondsInMillisecond);
}

// If time zone is provided, use it for the arithmetic operation (convert to it,
// apply operation, then convert back to UTC).
FOLLY_ALWAYS_INLINE Timestamp addToTimestamp(
    const Timestamp& timestamp,
    const DateTimeUnit unit,
    const int32_t value,
    const tz::TimeZone* timeZone) {
  if (timeZone == nullptr) {
    return addToTimestamp(timestamp, unit, value);
  } else {
    Timestamp zonedTimestamp = timestamp;
    zonedTimestamp.toTimezone(*timeZone);
    auto resultTimestamp = addToTimestamp(zonedTimestamp, unit, value);
    resultTimestamp.toGMT(*timeZone);
    return resultTimestamp;
  }
}

FOLLY_ALWAYS_INLINE int64_t addToTimestampWithTimezone(
    int64_t timestampWithTimezone,
    const DateTimeUnit unit,
    const int32_t value) {
  {
    int64_t finalSysMs;
    if (unit < DateTimeUnit::kDay) {
      auto originalTimestamp = unpackTimestampUtc(timestampWithTimezone);
      finalSysMs =
          addToTimestamp(originalTimestamp, unit, (int32_t)value).toMillis();
    } else {
      // Use local time to handle crossing daylight savings time boundaries.
      // E.g. the "day" when the clock moves back an hour is 25 hours long, and
      // the day it moves forward is 23 hours long. Daylight savings time
      // doesn't affect time units less than a day, and will produce incorrect
      // results if we use local time.
      const tz::TimeZone* timeZone =
          tz::locateZone(unpackZoneKeyId(timestampWithTimezone));
      auto originalTimestamp =
          Timestamp::fromMillis(timeZone
                                    ->to_local(std::chrono::milliseconds(
                                        unpackMillisUtc(timestampWithTimezone)))
                                    .count());
      auto updatedTimeStamp =
          addToTimestamp(originalTimestamp, unit, (int32_t)value);
      updatedTimeStamp = Timestamp(
          timeZone
              ->correct_nonexistent_time(
                  std::chrono::seconds(updatedTimeStamp.getSeconds()))
              .count(),
          updatedTimeStamp.getNanos());
      finalSysMs =
          timeZone
              ->to_sys(
                  std::chrono::milliseconds(updatedTimeStamp.toMillis()),
                  tz::TimeZone::TChoose::kEarliest)
              .count();
    }

    return pack(finalSysMs, unpackZoneKeyId(timestampWithTimezone));
  }
}

FOLLY_ALWAYS_INLINE int64_t diffTimestampWithTimeZone(
    const DateTimeUnit unit,
    const int64_t fromTimestampWithTimeZone,
    const int64_t toTimestampWithTimeZone) {
  auto fromTimeZoneId = unpackZoneKeyId(fromTimestampWithTimeZone);
  auto toTimeZoneId = unpackZoneKeyId(toTimestampWithTimeZone);
  VELOX_CHECK_EQ(
      fromTimeZoneId,
      toTimeZoneId,
      "diffTimestampWithTimeZone must receive timestamps in the same time zone.");

  Timestamp fromTimestamp;
  Timestamp toTimestamp;

  if (unit < DateTimeUnit::kDay) {
    fromTimestamp = unpackTimestampUtc(fromTimestampWithTimeZone);
    toTimestamp = unpackTimestampUtc(toTimestampWithTimeZone);
  } else {
    // Use local time to handle crossing daylight savings time boundaries.
    // E.g. the "day" when the clock moves back an hour is 25 hours long, and
    // the day it moves forward is 23 hours long. Daylight savings time
    // doesn't affect time units less than a day, and will produce incorrect
    // results if we use local time.
    const tz::TimeZone* timeZone = tz::locateZone(fromTimeZoneId);
    fromTimestamp = Timestamp::fromMillis(
        timeZone
            ->to_local(std::chrono::milliseconds(
                unpackMillisUtc(fromTimestampWithTimeZone)))
            .count());
    toTimestamp =
        Timestamp::fromMillis(timeZone
                                  ->to_local(std::chrono::milliseconds(
                                      unpackMillisUtc(toTimestampWithTimeZone)))
                                  .count());
  }

  return diffTimestamp(unit, fromTimestamp, toTimestamp);
}

FOLLY_ALWAYS_INLINE
int64_t diffDate(
    const DateTimeUnit unit,
    const int32_t fromDate,
    const int32_t toDate) {
  if (fromDate == toDate) {
    return 0;
  }
  return diffTimestamp(
      unit,
      // prevent overflow
      Timestamp((int64_t)fromDate * util::kSecsPerDay, 0),
      Timestamp((int64_t)toDate * util::kSecsPerDay, 0));
}

FOLLY_ALWAYS_INLINE int64_t
valueOfTimeUnitToMillis(const double value, std::string_view unit) {
  double convertedValue = value;
  if (unit == "ns") {
    convertedValue = convertedValue * std::milli::den / std::nano::den;
  } else if (unit == "us") {
    convertedValue = convertedValue * std::milli::den / std::micro::den;
  } else if (unit == "ms") {
  } else if (unit == "s") {
    convertedValue = convertedValue * std::milli::den;
  } else if (unit == "m") {
    convertedValue = convertedValue * 60 * std::milli::den;
  } else if (unit == "h") {
    convertedValue = convertedValue * 3600 * std::milli::den;
  } else if (unit == "d") {
    convertedValue = convertedValue * 86400 * std::milli::den;
  } else {
    VELOX_USER_FAIL("Unknown time unit: {}", unit);
  }
  try {
    return boost::numeric_cast<int64_t>(std::round(convertedValue));
  } catch (const boost::bad_numeric_cast&) {
    VELOX_USER_FAIL(
        "Value in {} unit is too large to be represented in ms unit as an int64_t",
        unit,
        value);
  }
  VELOX_UNREACHABLE();
}

} // namespace facebook::velox::functions
