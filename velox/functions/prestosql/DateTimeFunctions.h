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

#define XXH_INLINE_ALL
#include <boost/regex.hpp>
#include <xxhash.h>
#include <string_view>
#include "velox/expression/ComplexViewTypes.h"
#include "velox/functions/lib/DateTimeFormatter.h"
#include "velox/functions/lib/TimeUtils.h"
#include "velox/functions/prestosql/DateTimeImpl.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/type/TimestampConversion.h"
#include "velox/type/Type.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox::functions {

template <typename T>
struct ToUnixtimeFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      double& result,
      const arg_type<Timestamp>& timestamp) {
    result = toUnixtime(timestamp);
  }

  FOLLY_ALWAYS_INLINE void call(
      double& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    const auto milliseconds = unpackMillisUtc(*timestampWithTimezone);
    result = (double)milliseconds / kMillisecondsInSecond;
  }
};

template <typename T>
struct FromUnixtimeFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // (double) -> timestamp
  FOLLY_ALWAYS_INLINE void call(
      Timestamp& result,
      const arg_type<double>& unixtime) {
    result = fromUnixtime(unixtime);
  }

  // (double, varchar) -> timestamp with time zone
  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<double>* /*unixtime*/,
      const arg_type<Varchar>* timezone) {
    if (timezone != nullptr) {
      tzID_ = tz::getTimeZoneID((std::string_view)(*timezone));
    }
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<TimestampWithTimezone>& result,
      const arg_type<double>& unixtime,
      const arg_type<Varchar>& timeZone) {
    int16_t timeZoneId =
        tzID_.value_or(tz::getTimeZoneID((std::string_view)timeZone));
    result = fromUnixtime(unixtime, timeZoneId);
  }

  // (double, bigint, bigint) -> timestamp with time zone
  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<double>* /*unixtime*/,
      const arg_type<int64_t>* hours,
      const arg_type<int64_t>* minutes) {
    if (hours != nullptr && minutes != nullptr) {
      tzID_ = tz::getTimeZoneID(
          checkedPlus(checkedMultiply<int64_t>(*hours, 60), *minutes));
    }
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<TimestampWithTimezone>& result,
      const arg_type<double>& unixtime,
      const arg_type<int64_t>& hours,
      const arg_type<int64_t>& minutes) {
    int16_t timezoneId = tzID_.value_or(tz::getTimeZoneID(
        checkedPlus(checkedMultiply<int64_t>(hours, 60), minutes)));
    result = pack(fromUnixtime(unixtime).toMillis(), timezoneId);
  }

 private:
  std::optional<int64_t> tzID_;
};

namespace {

template <typename T>
struct TimestampWithTimezoneSupport {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // Convert timestampWithTimezone to a timestamp representing the moment at the
  // zone in timestampWithTimezone. If `asGMT` is set to true, return the GMT
  // time at the same moment.
  FOLLY_ALWAYS_INLINE
  Timestamp toTimestamp(
      const arg_type<TimestampWithTimezone>& timestampWithTimezone,
      bool asGMT = false) {
    auto timestamp = unpackTimestampUtc(*timestampWithTimezone);
    if (!asGMT) {
      timestamp.toTimezone(
          *tz::locateZone(unpackZoneKeyId(*timestampWithTimezone)));
    }

    return timestamp;
  }

  // Get offset in seconds with GMT from timestampWithTimezone.
  FOLLY_ALWAYS_INLINE
  int64_t getGMTOffsetSec(
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    Timestamp inputTimeStamp = this->toTimestamp(timestampWithTimezone);
    // Create a copy of inputTimeStamp and convert it to GMT
    auto gmtTimeStamp = inputTimeStamp;
    gmtTimeStamp.toGMT(
        *tz::locateZone(unpackZoneKeyId(*timestampWithTimezone)));

    // Get offset in seconds with GMT and convert to hour
    return (inputTimeStamp.getSeconds() - gmtTimeStamp.getSeconds());
  }
};

} // namespace

template <typename T>
struct DateFunction : public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* date) {
    timeZone_ = getTimeZoneFromConfig(config);
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Timestamp>* timestamp) {
    timeZone_ = getTimeZoneFromConfig(config);
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<TimestampWithTimezone>* timestampWithTimezone) {
    // Do nothing. Session timezone doesn't affect the result.
  }

  FOLLY_ALWAYS_INLINE Status
  call(out_type<Date>& result, const arg_type<Varchar>& date) {
    auto days = util::fromDateString(date, util::ParseMode::kPrestoCast);
    if (days.hasError()) {
      return days.error();
    }

    result = days.value();
    return Status::OK();
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<Timestamp>& timestamp) {
    result = util::toDate(timestamp, timeZone_);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    result = util::toDate(this->toTimestamp(timestampWithTimezone), nullptr);
  }

 private:
  const tz::TimeZone* timeZone_ = nullptr;
};

template <typename T>
struct WeekFunction : public InitSessionTimezone<T>,
                      public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getWeek(timestamp, this->timeZone_, false);
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, const arg_type<Date>& date) {
    result = getWeek(Timestamp::fromDate(date), nullptr, false);
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    result = getWeek(timestamp, nullptr, false);
  }
};

template <typename T>
struct YearFunction : public InitSessionTimezone<T>,
                      public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE int64_t getYear(const std::tm& time) {
    return 1900 + time.tm_year;
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getYear(getDateTime(timestamp, this->timeZone_));
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, const arg_type<Date>& date) {
    result = getYear(getDateTime(date));
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    result = getYear(getDateTime(timestamp, nullptr));
  }
};

template <typename T>
struct YearFromIntervalFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<IntervalYearMonth>& months) {
    result = months / 12;
  }
};

template <typename T>
struct QuarterFunction : public InitSessionTimezone<T>,
                         public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE int64_t getQuarter(const std::tm& time) {
    return time.tm_mon / 3 + 1;
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getQuarter(getDateTime(timestamp, this->timeZone_));
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, const arg_type<Date>& date) {
    result = getQuarter(getDateTime(date));
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    result = getQuarter(getDateTime(timestamp, nullptr));
  }
};

template <typename T>
struct MonthFunction : public InitSessionTimezone<T>,
                       public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE int64_t getMonth(const std::tm& time) {
    return 1 + time.tm_mon;
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getMonth(getDateTime(timestamp, this->timeZone_));
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, const arg_type<Date>& date) {
    result = getMonth(getDateTime(date));
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    result = getMonth(getDateTime(timestamp, nullptr));
  }
};

template <typename T>
struct MonthFromIntervalFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<IntervalYearMonth>& months) {
    result = months % 12;
  }
};

template <typename T>
struct DayFunction : public InitSessionTimezone<T>,
                     public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getDateTime(timestamp, this->timeZone_).tm_mday;
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, const arg_type<Date>& date) {
    result = getDateTime(date).tm_mday;
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    result = getDateTime(timestamp, nullptr).tm_mday;
  }
};

template <typename T>
struct DayFromIntervalFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<IntervalDayTime>& interval) {
    result = interval / kMillisInDay;
  }
};

template <typename T>
struct LastDayOfMonthFunction : public InitSessionTimezone<T>,
                                public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<Timestamp>& timestamp) {
    auto dt = getDateTime(timestamp, this->timeZone_);
    Expected<int64_t> daysSinceEpochFromDate =
        util::lastDayOfMonthSinceEpochFromDate(dt);
    if (daysSinceEpochFromDate.hasError()) {
      VELOX_DCHECK(daysSinceEpochFromDate.error().isUserError());
      VELOX_USER_FAIL(daysSinceEpochFromDate.error().message());
    }
    result = daysSinceEpochFromDate.value();
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<Date>& date) {
    auto dt = getDateTime(date);
    Expected<int64_t> lastDayOfMonthSinceEpoch =
        util::lastDayOfMonthSinceEpochFromDate(dt);
    if (lastDayOfMonthSinceEpoch.hasError()) {
      VELOX_DCHECK(lastDayOfMonthSinceEpoch.error().isUserError());
      VELOX_USER_FAIL(lastDayOfMonthSinceEpoch.error().message());
    }
    result = lastDayOfMonthSinceEpoch.value();
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    auto dt = getDateTime(timestamp, nullptr);
    Expected<int64_t> lastDayOfMonthSinceEpoch =
        util::lastDayOfMonthSinceEpochFromDate(dt);
    if (lastDayOfMonthSinceEpoch.hasError()) {
      VELOX_DCHECK(lastDayOfMonthSinceEpoch.error().isUserError());
      VELOX_USER_FAIL(lastDayOfMonthSinceEpoch.error().message());
    }
    result = lastDayOfMonthSinceEpoch.value();
  }
};

namespace {

bool isIntervalWholeDays(int64_t milliseconds) {
  return (milliseconds % kMillisInDay) == 0;
}

int64_t intervalDays(int64_t milliseconds) {
  return milliseconds / kMillisInDay;
}

} // namespace

template <typename T>
struct DateMinusInterval {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<Date>& date,
      const arg_type<IntervalDayTime>& interval) {
    VELOX_USER_CHECK(
        isIntervalWholeDays(interval),
        "Cannot subtract hours, minutes, seconds or milliseconds from a date");
    result = addToDate(date, DateTimeUnit::kDay, -intervalDays(interval));
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<Date>& date,
      const arg_type<IntervalYearMonth>& interval) {
    result = addToDate(date, DateTimeUnit::kMonth, -interval);
  }
};

template <typename T>
struct DatePlusInterval {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<Date>& date,
      const arg_type<IntervalDayTime>& interval) {
    VELOX_USER_CHECK(
        isIntervalWholeDays(interval),
        "Cannot add hours, minutes, seconds or milliseconds to a date");
    result = addToDate(date, DateTimeUnit::kDay, intervalDays(interval));
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<Date>& date,
      const arg_type<IntervalYearMonth>& interval) {
    result = addToDate(date, DateTimeUnit::kMonth, interval);
  }
};

template <typename T>
struct TimestampMinusFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<IntervalDayTime>& result,
      const arg_type<Timestamp>& a,
      const arg_type<Timestamp>& b) {
    result = a.toMillis() - b.toMillis();
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<IntervalDayTime>& result,
      const arg_type<TimestampWithTimezone>& a,
      const arg_type<TimestampWithTimezone>& b) {
    result = unpackMillisUtc(*a) - unpackMillisUtc(*b);
  }
};

template <typename T>
struct TimestampPlusInterval {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Timestamp>& result,
      const arg_type<Timestamp>& a,
      const arg_type<IntervalDayTime>& b)
#if defined(__has_feature)
#if __has_feature(__address_sanitizer__)
      __attribute__((__no_sanitize__("signed-integer-overflow")))
#endif
#endif
  {
    result = Timestamp::fromMillisNoError(a.toMillis() + b);
  }

  // We only need to capture the time zone session config if we are operating on
  // a timestamp and a IntervalYearMonth.
  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Timestamp>*,
      const arg_type<IntervalYearMonth>*) {
    sessionTimeZone_ = getTimeZoneFromConfig(config);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Timestamp>& result,
      const arg_type<Timestamp>& timestamp,
      const arg_type<IntervalYearMonth>& interval) {
    result = addToTimestamp(
        timestamp, DateTimeUnit::kMonth, interval, sessionTimeZone_);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<TimestampWithTimezone>& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone,
      const arg_type<IntervalDayTime>& interval) {
    result = addToTimestampWithTimezone(
        *timestampWithTimezone, DateTimeUnit::kMillisecond, interval);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<TimestampWithTimezone>& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone,
      const arg_type<IntervalYearMonth>& interval) {
    result = addToTimestampWithTimezone(
        *timestampWithTimezone, DateTimeUnit::kMonth, interval);
  }

 private:
  // Only set if the parameters are timestamp and IntervalYearMonth.
  const tz::TimeZone* sessionTimeZone_ = nullptr;
};

template <typename T>
struct IntervalPlusTimestamp {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Timestamp>& result,
      const arg_type<IntervalDayTime>& a,
      const arg_type<Timestamp>& b)
#if defined(__has_feature)
#if __has_feature(__address_sanitizer__)
      __attribute__((__no_sanitize__("signed-integer-overflow")))
#endif
#endif
  {
    result = Timestamp::fromMillisNoError(a + b.toMillis());
  }

  // We only need to capture the time zone session config if we are operating on
  // a timestamp and a IntervalYearMonth.
  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<IntervalYearMonth>*,
      const arg_type<Timestamp>*) {
    sessionTimeZone_ = getTimeZoneFromConfig(config);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Timestamp>& result,
      const arg_type<IntervalYearMonth>& interval,
      const arg_type<Timestamp>& timestamp) {
    result = addToTimestamp(
        timestamp, DateTimeUnit::kMonth, interval, sessionTimeZone_);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<TimestampWithTimezone>& result,
      const arg_type<IntervalDayTime>& interval,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    result = addToTimestampWithTimezone(
        *timestampWithTimezone, DateTimeUnit::kMillisecond, interval);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<TimestampWithTimezone>& result,
      const arg_type<IntervalYearMonth>& interval,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    result = addToTimestampWithTimezone(
        *timestampWithTimezone, DateTimeUnit::kMonth, interval);
  }

 private:
  // Only set if the parameters are timestamp and IntervalYearMonth.
  const tz::TimeZone* sessionTimeZone_ = nullptr;
};

template <typename T>
struct TimestampMinusInterval {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Timestamp>& result,
      const arg_type<Timestamp>& a,
      const arg_type<IntervalDayTime>& b)
#if defined(__has_feature)
#if __has_feature(__address_sanitizer__)
      __attribute__((__no_sanitize__("signed-integer-overflow")))
#endif
#endif
  {
    result = Timestamp::fromMillisNoError(a.toMillis() - b);
  }

  // We only need to capture the time zone session config if we are operating on
  // a timestamp and a IntervalYearMonth.
  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Timestamp>*,
      const arg_type<IntervalYearMonth>*) {
    sessionTimeZone_ = getTimeZoneFromConfig(config);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Timestamp>& result,
      const arg_type<Timestamp>& timestamp,
      const arg_type<IntervalYearMonth>& interval) {
    result = addToTimestamp(
        timestamp, DateTimeUnit::kMonth, -interval, sessionTimeZone_);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<TimestampWithTimezone>& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone,
      const arg_type<IntervalDayTime>& interval) {
    result = addToTimestampWithTimezone(
        *timestampWithTimezone, DateTimeUnit::kMillisecond, -interval);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<TimestampWithTimezone>& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone,
      const arg_type<IntervalYearMonth>& interval) {
    result = addToTimestampWithTimezone(
        *timestampWithTimezone, DateTimeUnit::kMonth, -interval);
  }

 private:
  // Only set if the parameters are timestamp and IntervalYearMonth.
  const tz::TimeZone* sessionTimeZone_ = nullptr;
};

template <typename T>
struct DayOfWeekFunction : public InitSessionTimezone<T>,
                           public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE int64_t getDayOfWeek(const std::tm& time) {
    return time.tm_wday == 0 ? 7 : time.tm_wday;
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getDayOfWeek(getDateTime(timestamp, this->timeZone_));
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, const arg_type<Date>& date) {
    result = getDayOfWeek(getDateTime(date));
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    result = getDayOfWeek(getDateTime(timestamp, nullptr));
  }
};

template <typename T>
struct DayOfYearFunction : public InitSessionTimezone<T>,
                           public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE int64_t getDayOfYear(const std::tm& time) {
    return time.tm_yday + 1;
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getDayOfYear(getDateTime(timestamp, this->timeZone_));
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, const arg_type<Date>& date) {
    result = getDayOfYear(getDateTime(date));
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    result = getDayOfYear(getDateTime(timestamp, nullptr));
  }
};

template <typename T>
struct YearOfWeekFunction : public InitSessionTimezone<T>,
                            public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE int64_t computeYearOfWeek(const std::tm& dateTime) {
    int isoWeekDay = dateTime.tm_wday == 0 ? 7 : dateTime.tm_wday;
    // The last few days in December may belong to the next year if they are
    // in the same week as the next January 1 and this January 1 is a Thursday
    // or before.
    if (UNLIKELY(
            dateTime.tm_mon == 11 && dateTime.tm_mday >= 29 &&
            dateTime.tm_mday - isoWeekDay >= 31 - 3)) {
      return 1900 + dateTime.tm_year + 1;
    }
    // The first few days in January may belong to the last year if they are
    // in the same week as January 1 and January 1 is a Friday or after.
    else if (UNLIKELY(
                 dateTime.tm_mon == 0 && dateTime.tm_mday <= 3 &&
                 isoWeekDay - (dateTime.tm_mday - 1) >= 5)) {
      return 1900 + dateTime.tm_year - 1;
    } else {
      return 1900 + dateTime.tm_year;
    }
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = computeYearOfWeek(getDateTime(timestamp, this->timeZone_));
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, const arg_type<Date>& date) {
    result = computeYearOfWeek(getDateTime(date));
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    result = computeYearOfWeek(getDateTime(timestamp, nullptr));
  }
};

template <typename T>
struct HourFunction : public InitSessionTimezone<T>,
                      public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getDateTime(timestamp, this->timeZone_).tm_hour;
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, const arg_type<Date>& date) {
    result = getDateTime(date).tm_hour;
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    result = getDateTime(timestamp, nullptr).tm_hour;
  }
};

template <typename T>
struct HourFromIntervalFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<IntervalDayTime>& millis) {
    result = (millis % kMillisInDay) / kMillisInHour;
  }
};

template <typename T>
struct MinuteFunction : public InitSessionTimezone<T>,
                        public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getDateTime(timestamp, this->timeZone_).tm_min;
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, const arg_type<Date>& date) {
    result = getDateTime(date).tm_min;
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    result = getDateTime(timestamp, nullptr).tm_min;
  }
};

template <typename T>
struct MinuteFromIntervalFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<IntervalDayTime>& millis) {
    result = (millis % kMillisInHour) / kMillisInMinute;
  }
};

template <typename T>
struct SecondFunction : public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getDateTime(timestamp, nullptr).tm_sec;
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, const arg_type<Date>& date) {
    result = getDateTime(date).tm_sec;
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    result = getDateTime(timestamp, nullptr).tm_sec;
  }
};

template <typename T>
struct SecondFromIntervalFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<IntervalDayTime>& millis) {
    result = (millis % kMillisInMinute) / kMillisInSecond;
  }
};

template <typename T>
struct MillisecondFunction : public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = timestamp.getNanos() / kNanosecondsInMillisecond;
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Date>& /*date*/) {
    // Dates do not have millisecond granularity.
    result = 0;
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    result = timestamp.getNanos() / kNanosecondsInMillisecond;
  }
};

template <typename T>
struct MillisecondFromIntervalFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<IntervalDayTime>& millis) {
    result = millis % kMillisecondsInSecond;
  }
};

namespace {
inline bool isDateUnit(const DateTimeUnit unit) {
  return unit == DateTimeUnit::kDay || unit == DateTimeUnit::kMonth ||
      unit == DateTimeUnit::kQuarter || unit == DateTimeUnit::kYear ||
      unit == DateTimeUnit::kWeek;
}

inline std::optional<DateTimeUnit> getDateUnit(
    const StringView& unitString,
    bool throwIfInvalid) {
  std::optional<DateTimeUnit> unit =
      fromDateTimeUnitString(unitString, throwIfInvalid);
  if (unit.has_value() && !isDateUnit(unit.value())) {
    if (throwIfInvalid) {
      VELOX_USER_FAIL("{} is not a valid DATE field", unitString);
    }
    return std::nullopt;
  }
  return unit;
}

inline std::optional<DateTimeUnit> getTimestampUnit(
    const StringView& unitString) {
  std::optional<DateTimeUnit> unit =
      fromDateTimeUnitString(unitString, /*throwIfInvalid=*/false);
  VELOX_USER_CHECK(
      !(unit.has_value() && unit.value() == DateTimeUnit::kMillisecond),
      "{} is not a valid TIMESTAMP field",
      unitString);

  return unit;
}

} // namespace

template <typename T>
struct DateTruncFunction : public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  const tz::TimeZone* timeZone_ = nullptr;
  std::optional<DateTimeUnit> unit_;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* unitString,
      const arg_type<Timestamp>* /*timestamp*/) {
    timeZone_ = getTimeZoneFromConfig(config);

    if (unitString != nullptr) {
      unit_ = getTimestampUnit(*unitString);
    }
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<Varchar>* unitString,
      const arg_type<Date>* /*date*/) {
    if (unitString != nullptr) {
      unit_ = getDateUnit(*unitString, false);
    }
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<Varchar>* unitString,
      const arg_type<TimestampWithTimezone>* /*timestamp*/) {
    if (unitString != nullptr) {
      unit_ = getTimestampUnit(*unitString);
    }
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Timestamp>& result,
      const arg_type<Varchar>& unitString,
      const arg_type<Timestamp>& timestamp) {
    DateTimeUnit unit;
    if (unit_.has_value()) {
      unit = unit_.value();
    } else {
      unit = getTimestampUnit(unitString).value();
    }
    result = truncateTimestamp(timestamp, unit, timeZone_);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<Varchar>& unitString,
      const arg_type<Date>& date) {
    DateTimeUnit unit = unit_.has_value()
        ? unit_.value()
        : getDateUnit(unitString, true).value();

    if (unit == DateTimeUnit::kDay) {
      result = date;
      return;
    }

    auto dateTime = getDateTime(date);
    adjustDateTime(dateTime, unit);

    result = Timestamp::calendarUtcToEpoch(dateTime) / kSecondsInDay;
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<TimestampWithTimezone>& result,
      const arg_type<Varchar>& unitString,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    DateTimeUnit unit;
    if (unit_.has_value()) {
      unit = unit_.value();
    } else {
      unit = getTimestampUnit(unitString).value();
    }

    if (unit == DateTimeUnit::kSecond) {
      const auto utcTimestamp = unpackTimestampUtc(*timestampWithTimezone);
      result = pack(
          utcTimestamp.getSeconds() * 1000,
          unpackZoneKeyId(*timestampWithTimezone));
      return;
    }

    const auto timestamp = this->toTimestamp(timestampWithTimezone);
    auto dateTime = getDateTime(timestamp, nullptr);
    adjustDateTime(dateTime, unit);

    uint64_t resultMillis;

    if (unit < DateTimeUnit::kDay) {
      // If the unit is less than a day, we compute the difference in
      // milliseconds between the local timestamp and the truncated local
      // timestamp. We then subtract this difference from the UTC timestamp,
      // this handles things like ambiguous timestamps in the local time zone.
      const auto millisDifference =
          timestamp.toMillis() - Timestamp::calendarUtcToEpoch(dateTime) * 1000;

      resultMillis = unpackMillisUtc(*timestampWithTimezone) - millisDifference;
    } else {
      // If the unit is at least a day, we do the truncation on the local
      // timestamp and then convert it to a system time directly. This handles
      // cases like when a time zone has daylight savings time, a "day" can be
      // 25 or 23 hours at the transition points.
      auto updatedTimestamp =
          Timestamp::fromMillis(Timestamp::calendarUtcToEpoch(dateTime) * 1000);
      updatedTimestamp.toGMT(
          *tz::locateZone(unpackZoneKeyId(*timestampWithTimezone)));

      resultMillis = updatedTimestamp.toMillis();
    }

    result = pack(resultMillis, unpackZoneKeyId(*timestampWithTimezone));
  }
};

template <typename T>
struct DateAddFunction : public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  const tz::TimeZone* sessionTimeZone_ = nullptr;
  std::optional<DateTimeUnit> unit_ = std::nullopt;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* unitString,
      const int64_t* /*value*/,
      const arg_type<Timestamp>* /*timestamp*/) {
    sessionTimeZone_ = getTimeZoneFromConfig(config);
    if (unitString != nullptr) {
      unit_ = fromDateTimeUnitString(*unitString, /*throwIfInvalid=*/false);
    }
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<Varchar>* unitString,
      const int64_t* /*value*/,
      const arg_type<Date>* /*date*/) {
    if (unitString != nullptr) {
      unit_ = getDateUnit(*unitString, false);
    }
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Timestamp>& result,
      const arg_type<Varchar>& unitString,
      const int64_t value,
      const arg_type<Timestamp>& timestamp) {
    const auto unit = unit_.has_value()
        ? unit_.value()
        : fromDateTimeUnitString(unitString, /*throwIfInvalid=*/true).value();

    if (value != (int32_t)value) {
      VELOX_UNSUPPORTED("integer overflow");
    }

    if (LIKELY(sessionTimeZone_ != nullptr)) {
      // sessionTimeZone not null means that the config
      // adjust_timestamp_to_timezone is on.
      Timestamp zonedTimestamp = timestamp;
      zonedTimestamp.toTimezone(*sessionTimeZone_);

      Timestamp resultTimestamp =
          addToTimestamp(zonedTimestamp, unit, (int32_t)value);

      if (isTimeUnit(unit)) {
        const int64_t offset = static_cast<Timestamp>(timestamp).getSeconds() -
            zonedTimestamp.getSeconds();
        result = Timestamp(
            resultTimestamp.getSeconds() + offset, resultTimestamp.getNanos());
      } else {
        result = Timestamp(
            sessionTimeZone_
                ->correct_nonexistent_time(
                    std::chrono::seconds(resultTimestamp.getSeconds()))
                .count(),
            resultTimestamp.getNanos());
        result.toGMT(*sessionTimeZone_);
      }
    } else {
      result = addToTimestamp(timestamp, unit, (int32_t)value);
    }
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<TimestampWithTimezone>& result,
      const arg_type<Varchar>& unitString,
      const int64_t value,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    const auto unit = unit_.value_or(
        fromDateTimeUnitString(unitString, /*throwIfInvalid=*/true).value());

    if (value != (int32_t)value) {
      VELOX_UNSUPPORTED("integer overflow");
    }

    result = addToTimestampWithTimezone(
        *timestampWithTimezone, unit, (int32_t)value);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<Varchar>& unitString,
      const int64_t value,
      const arg_type<Date>& date) {
    DateTimeUnit unit = unit_.has_value()
        ? unit_.value()
        : getDateUnit(unitString, true).value();

    if (value != (int32_t)value) {
      VELOX_UNSUPPORTED("integer overflow");
    }

    result = addToDate(date, unit, (int32_t)value);
  }
};

template <typename T>
struct DateDiffFunction : public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  const tz::TimeZone* sessionTimeZone_ = nullptr;
  std::optional<DateTimeUnit> unit_ = std::nullopt;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* unitString,
      const arg_type<Timestamp>* /*timestamp1*/,
      const arg_type<Timestamp>* /*timestamp2*/) {
    if (unitString != nullptr) {
      unit_ = fromDateTimeUnitString(*unitString, /*throwIfInvalid=*/false);
    }

    sessionTimeZone_ = getTimeZoneFromConfig(config);
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<Varchar>* unitString,
      const arg_type<Date>* /*date1*/,
      const arg_type<Date>* /*date2*/) {
    if (unitString != nullptr) {
      unit_ = getDateUnit(*unitString, false);
    }
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* unitString,
      const arg_type<TimestampWithTimezone>* /*timestampWithTimezone1*/,
      const arg_type<TimestampWithTimezone>* /*timestampWithTimezone2*/) {
    if (unitString != nullptr) {
      unit_ = fromDateTimeUnitString(*unitString, /*throwIfInvalid=*/false);
    }
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Varchar>& unitString,
      const arg_type<Timestamp>& timestamp1,
      const arg_type<Timestamp>& timestamp2) {
    const auto unit = unit_.value_or(
        fromDateTimeUnitString(unitString, /*throwIfInvalid=*/true).value());
    result = diffTimestamp(unit, timestamp1, timestamp2, sessionTimeZone_);
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Varchar>& unitString,
      const arg_type<Date>& date1,
      const arg_type<Date>& date2) {
    DateTimeUnit unit = unit_.has_value()
        ? unit_.value()
        : getDateUnit(unitString, true).value();

    result = diffDate(unit, date1, date2);
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Varchar>& unitString,
      const arg_type<TimestampWithTimezone>& timestampWithTz1,
      const arg_type<TimestampWithTimezone>& timestampWithTz2) {
    const auto unit = unit_.value_or(
        fromDateTimeUnitString(unitString, /*throwIfInvalid=*/true).value());

    // Presto's behavior is to use the time zone of the first parameter to
    // perform the calculation. Note that always normalizing to UTC is not
    // correct as calculations may cross daylight savings boundaries.
    auto timeZoneId = unpackZoneKeyId(*timestampWithTz1);

    result = diffTimestampWithTimeZone(
        unit,
        *timestampWithTz1,
        pack(unpackMillisUtc(*timestampWithTz2), timeZoneId));
  }
};

template <typename T>
struct DateFormatFunction : public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Timestamp>* /*timestamp*/,
      const arg_type<Varchar>* formatString) {
    sessionTimeZone_ = getTimeZoneFromConfig(config);
    if (formatString != nullptr) {
      setFormatter(*formatString);
      isConstFormat_ = true;
    }
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<TimestampWithTimezone>* /*timestamp*/,
      const arg_type<Varchar>* formatString) {
    if (formatString != nullptr) {
      setFormatter(*formatString);
      isConstFormat_ = true;
    }
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Timestamp>& timestamp,
      const arg_type<Varchar>& formatString) {
    if (!isConstFormat_) {
      setFormatter(formatString);
    }

    result.reserve(maxResultSize_);
    const auto resultSize = mysqlDateTime_->format(
        timestamp, sessionTimeZone_, maxResultSize_, result.data());
    result.resize(resultSize);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone,
      const arg_type<Varchar>& formatString) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    call(result, timestamp, formatString);
  }

 private:
  FOLLY_ALWAYS_INLINE void setFormatter(const arg_type<Varchar> formatString) {
    mysqlDateTime_ =
        buildMysqlDateTimeFormatter(
            std::string_view(formatString.data(), formatString.size()))
            .thenOrThrow(folly::identity, [&](const Status& status) {
              VELOX_USER_FAIL("{}", status.message());
            });
    maxResultSize_ = mysqlDateTime_->maxResultSize(sessionTimeZone_);
  }

  const tz::TimeZone* sessionTimeZone_ = nullptr;
  std::shared_ptr<DateTimeFormatter> mysqlDateTime_;
  uint32_t maxResultSize_;
  bool isConstFormat_ = false;
};

template <typename T>
struct FromIso8601Date {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE Status
  call(out_type<Date>& result, const arg_type<Varchar>& input) {
    const auto castResult = util::fromDateString(
        input.data(), input.size(), util::ParseMode::kIso8601);
    if (castResult.hasError()) {
      return castResult.error();
    }

    result = castResult.value();
    return Status::OK();
  }
};

template <typename T>
struct FromIso8601Timestamp {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* /*input*/) {
    auto sessionTzName = config.sessionTimezone();
    if (!sessionTzName.empty()) {
      sessionTimeZone_ = tz::locateZone(sessionTzName);
    }
  }

  FOLLY_ALWAYS_INLINE Status call(
      out_type<TimestampWithTimezone>& result,
      const arg_type<Varchar>& input) {
    const auto castResult = util::fromTimestampWithTimezoneString(
        input.data(), input.size(), util::TimestampParseMode::kIso8601);
    if (castResult.hasError()) {
      return castResult.error();
    }

    auto [ts, timeZone, offsetMillis] = castResult.value();
    VELOX_DCHECK(!offsetMillis.has_value());
    // Input string may not contain a timezone - if so, it is interpreted in
    // session timezone.
    if (!timeZone) {
      timeZone = sessionTimeZone_;
    }
    ts.toGMT(*timeZone);
    result = pack(ts, timeZone->id());
    return Status::OK();
  }

 private:
  const tz::TimeZone* sessionTimeZone_{tz::locateZone(0)}; // default to GMT.
};

template <typename T>
struct DateParseFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  std::shared_ptr<DateTimeFormatter> format_;

  // By default, assume 0 (GMT).
  const tz::TimeZone* sessionTimeZone_{tz::locateZone(0)};
  bool isConstFormat_ = false;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* /*input*/,
      const arg_type<Varchar>* formatString) {
    if (formatString != nullptr) {
      format_ =
          buildMysqlDateTimeFormatter(
              std::string_view(formatString->data(), formatString->size()))
              .thenOrThrow(folly::identity, [&](const Status& status) {
                VELOX_USER_FAIL("{}", status.message());
              });
      isConstFormat_ = true;
    }

    auto sessionTzName = config.sessionTimezone();
    if (!sessionTzName.empty()) {
      sessionTimeZone_ = tz::locateZone(sessionTzName);
    }
  }

  FOLLY_ALWAYS_INLINE Status call(
      out_type<Timestamp>& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& format) {
    if (!isConstFormat_) {
      format_ = buildMysqlDateTimeFormatter(
                    std::string_view(format.data(), format.size()))
                    .thenOrThrow(folly::identity, [&](const Status& status) {
                      VELOX_USER_FAIL("{}", status.message());
                    });
    }

    auto dateTimeResult = format_->parse((std::string_view)(input));
    if (dateTimeResult.hasError()) {
      return dateTimeResult.error();
    }

    dateTimeResult->timestamp.toGMT(*sessionTimeZone_);
    result = dateTimeResult->timestamp;
    return Status::OK();
  }
};

template <typename T>
struct FormatDateTimeFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Timestamp>* /*timestamp*/,
      const arg_type<Varchar>* formatString) {
    sessionTimeZone_ = getTimeZoneFromConfig(config);
    if (formatString != nullptr) {
      setFormatter(*formatString);
      isConstFormat_ = true;
    }
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Timestamp>& timestamp,
      const arg_type<Varchar>& formatString) {
    ensureFormatter(formatString);

    format(timestamp, sessionTimeZone_, maxResultSize_, result);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone,
      const arg_type<Varchar>& formatString) {
    ensureFormatter(formatString);

    const auto timestamp = unpackTimestampUtc(*timestampWithTimezone);
    const auto timeZoneId = unpackZoneKeyId(*timestampWithTimezone);
    auto* timezonePtr = tz::locateZone(tz::getTimeZoneName(timeZoneId));

    const auto maxResultSize = jodaDateTime_->maxResultSize(timezonePtr);
    format(timestamp, timezonePtr, maxResultSize, result);
  }

 private:
  FOLLY_ALWAYS_INLINE void ensureFormatter(
      const arg_type<Varchar>& formatString) {
    if (!isConstFormat_) {
      setFormatter(formatString);
    }
  }

  FOLLY_ALWAYS_INLINE void setFormatter(const arg_type<Varchar>& formatString) {
    buildJodaDateTimeFormatter(
        std::string_view(formatString.data(), formatString.size()))
        .thenOrThrow([this](auto formatter) {
          jodaDateTime_ = formatter;
          maxResultSize_ = jodaDateTime_->maxResultSize(sessionTimeZone_);
        });
  }

  void format(
      const Timestamp& timestamp,
      const tz::TimeZone* timeZone,
      uint32_t maxResultSize,
      out_type<Varchar>& result) const {
    result.reserve(maxResultSize);
    const auto resultSize = jodaDateTime_->format(
        timestamp, timeZone, maxResultSize, result.data());
    result.resize(resultSize);
  }

  const tz::TimeZone* sessionTimeZone_ = nullptr;
  std::shared_ptr<DateTimeFormatter> jodaDateTime_;
  uint32_t maxResultSize_;
  bool isConstFormat_ = false;
};

template <typename T>
struct ParseDateTimeFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  std::shared_ptr<DateTimeFormatter> format_;
  const tz::TimeZone* sessionTimeZone_{tz::locateZone(0)}; // GMT
  bool isConstFormat_ = false;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* /*input*/,
      const arg_type<Varchar>* format) {
    if (format != nullptr) {
      format_ = buildJodaDateTimeFormatter(
                    std::string_view(format->data(), format->size()))
                    .thenOrThrow(folly::identity, [&](const Status& status) {
                      VELOX_USER_FAIL("{}", status.message());
                    });
      isConstFormat_ = true;
    }

    auto sessionTzName = config.sessionTimezone();
    if (!sessionTzName.empty()) {
      sessionTimeZone_ = tz::locateZone(sessionTzName);
    }
  }

  FOLLY_ALWAYS_INLINE Status call(
      out_type<TimestampWithTimezone>& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& format) {
    if (!isConstFormat_) {
      format_ = buildJodaDateTimeFormatter(
                    std::string_view(format.data(), format.size()))
                    .thenOrThrow(folly::identity, [&](const Status& status) {
                      VELOX_USER_FAIL("{}", status.message());
                    });
    }
    auto dateTimeResult =
        format_->parse(std::string_view(input.data(), input.size()));
    if (dateTimeResult.hasError()) {
      return dateTimeResult.error();
    }

    // If timezone was not parsed, fallback to the session timezone. If there's
    // no session timezone, fallback to 0 (GMT).
    const auto* timeZone =
        dateTimeResult->timezone ? dateTimeResult->timezone : sessionTimeZone_;
    dateTimeResult->timestamp.toGMT(*timeZone);
    result = pack(dateTimeResult->timestamp, timeZone->id());
    return Status::OK();
  }
};

template <typename T>
struct CurrentDateFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  const tz::TimeZone* timeZone_ = nullptr;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config) {
    timeZone_ = getTimeZoneFromConfig(config);
  }

  FOLLY_ALWAYS_INLINE void call(out_type<Date>& result) {
    auto now = Timestamp::now();
    if (timeZone_ != nullptr) {
      now.toTimezone(*timeZone_);
    }
    const std::chrono::
        time_point<std::chrono::system_clock, std::chrono::milliseconds>
            localTimepoint(std::chrono::milliseconds(now.toMillis()));
    result = std::chrono::floor<date::days>((localTimepoint).time_since_epoch())
                 .count();
  }
};

template <typename T>
struct TimeZoneHourFunction : public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& input) {
    // Get offset in seconds with GMT and convert to hour
    auto offset = this->getGMTOffsetSec(input);
    result = offset / 3600;
  }
};

template <typename T>
struct TimeZoneMinuteFunction : public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& input) {
    // Get offset in seconds with GMT and convert to minute
    auto offset = this->getGMTOffsetSec(input);
    result = (offset / 60) % 60;
  }
};

template <typename T>
struct ToISO8601Function {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  ToISO8601Function() {
    auto formatter =
        functions::buildJodaDateTimeFormatter("yyyy-MM-dd'T'HH:mm:ss.SSSZZ");
    VELOX_CHECK(
        !formatter.hasError(),
        "Default format should always be valid, error: {}",
        formatter.error().message());
    formatter_ = formatter.value();
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& inputTypes,
      const core::QueryConfig& config,
      const arg_type<Timestamp>* /*input*/) {
    if (inputTypes[0]->isTimestamp()) {
      timeZone_ = getTimeZoneFromConfig(config);
    }
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Date>& date) {
    result = DateType::toIso8601(date);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Timestamp>& timestamp) {
    toIso8601(timestamp, timeZone_, result);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    const auto timestamp = unpackTimestampUtc(*timestampWithTimezone);
    const auto timeZoneId = unpackZoneKeyId(*timestampWithTimezone);
    const auto* timeZone = tz::locateZone(tz::getTimeZoneName(timeZoneId));

    toIso8601(timestamp, timeZone, result);
  }

 private:
  void toIso8601(
      const Timestamp& timestamp,
      const tz::TimeZone* timeZone,
      out_type<Varchar>& result) const {
    const auto maxResultSize = formatter_->maxResultSize(timeZone);
    result.reserve(maxResultSize);
    const auto resultSize = formatter_->format(
        timestamp, timeZone, maxResultSize, result.data(), false, "Z");
    result.resize(resultSize);
  }

  const tz::TimeZone* timeZone_{nullptr};
  std::shared_ptr<DateTimeFormatter> formatter_;
};

template <typename T>
struct AtTimezoneFunction : public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  std::optional<int64_t> targetTimezoneID_;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<TimestampWithTimezone>* /*tsWithTz*/,
      const arg_type<Varchar>* timezone) {
    if (timezone) {
      targetTimezoneID_ = tz::getTimeZoneID(
          std::string_view(timezone->data(), timezone->size()));
    }
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<TimestampWithTimezone>& result,
      const arg_type<TimestampWithTimezone>& tsWithTz,
      const arg_type<Varchar>& timezone) {
    const auto inputMs = unpackMillisUtc(*tsWithTz);
    const auto targetTimezoneID = targetTimezoneID_.has_value()
        ? targetTimezoneID_.value()
        : tz::getTimeZoneID(std::string_view(timezone.data(), timezone.size()));

    // Input and output TimestampWithTimezones should not contain
    // different timestamp values - solely timezone ID should differ between the
    // two, as timestamp is stored as a UTC offset. The timestamp is then
    // resolved to the respective timezone at the time of display.
    result = pack(inputMs, targetTimezoneID);
  }
};

template <typename TExec>
struct ToMillisecondFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  FOLLY_ALWAYS_INLINE void call(
      out_type<int64_t>& result,
      const arg_type<IntervalDayTime>& millis) {
    result = millis;
  }
};

/// xxhash64(Date) → bigint
/// Return a xxhash64 of input Date
template <typename T>
struct XxHash64DateFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE
  void call(out_type<int64_t>& result, const arg_type<Date>& input) {
    // Casted to int64_t to feed into XXH64
    auto date_input = static_cast<int64_t>(input);
    result = XXH64(&date_input, sizeof(date_input), 0);
  }
};

/// xxhash64(Timestamp) → bigint
/// Return a xxhash64 of input Timestamp
template <typename T>
struct XxHash64TimestampFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE
  void call(out_type<int64_t>& result, const arg_type<Timestamp>& input) {
    // Use the milliseconds representation of the timestamp
    auto timestamp_millis = input.toMillis();
    result = XXH64(&timestamp_millis, sizeof(timestamp_millis), 0);
  }
};

template <typename T>
struct ParseDurationFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  std::unique_ptr<boost::regex> durationRegex_;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<Varchar>* /*amountUnit*/) {
    durationRegex_ = std::make_unique<boost::regex>(
        "^\\s*(\\d+(?:\\.\\d+)?)\\s*([a-zA-Z]+)\\s*$");
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<IntervalDayTime>& result,
      const arg_type<Varchar>& amountUnit) {
    std::string strAmountUnit = (std::string)amountUnit;
    boost::smatch match;
    bool isMatch = boost::regex_search(strAmountUnit, match, *durationRegex_);
    if (!isMatch) {
      VELOX_USER_FAIL(
          "Input duration is not a valid data duration string: {}", amountUnit);
    }
    VELOX_USER_CHECK_EQ(
        match.size(),
        3,
        "Input duration does not have value and unit components only: {}",
        amountUnit);
    try {
      double value = std::stod(match[1].str());
      std::string unit = match[2].str();
      result = valueOfTimeUnitToMillis(value, unit);
    } catch (std::out_of_range&) {
      VELOX_USER_FAIL(
          "Input duration value is out of range for double: {}",
          match[1].str());
    } catch (std::invalid_argument&) {
      VELOX_USER_FAIL(
          "Input duration value is not a valid number: {}", match[1].str());
    }
  }
};

} // namespace facebook::velox::functions
