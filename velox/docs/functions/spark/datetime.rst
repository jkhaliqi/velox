=======================
Date and Time Functions
=======================

Convenience Extraction Functions
--------------------------------

These functions support TIMESTAMP and DATE input types.

.. spark:function:: add_months(startDate, numMonths) -> date

    Returns the date that is ``numMonths`` after ``startDate``.
    Adjusts result to a valid one, considering months have different total days, and especially
    February has 28 days in common year but 29 days in leap year.
    For example, add_months('2015-01-30', 1) returns '2015-02-28', because 28th is the last day
    in February of 2015.
    ``numMonths`` can be zero or negative. Throws an error when inputs lead to int overflow,
    e.g., add_months('2023-07-10', -2147483648). ::

        SELECT add_months('2015-01-01', 10); -- '2015-11-01'
        SELECT add_months('2015-01-30', 1); -- '2015-02-28'
        SELECT add_months('2015-01-30', 0); -- '2015-01-30'
        SELECT add_months('2015-01-30', -2); -- '2014-11-30'
        SELECT add_months('2015-03-31', -1); -- '2015-02-28'

.. spark:function:: date_add(start_date, num_days) -> date

    Returns the date that is ``num_days`` after ``start_date``. According to the inputs,
    the returned date will wrap around between the minimum negative date and
    maximum positive date. date_add('1969-12-31', 2147483647) get 5881580-07-10,
    and date_add('2024-01-22', 2147483647) get -5877587-07-12.

    If ``num_days`` is a negative value then these amount of days will be
    deducted from ``start_date``.
    Supported types for ``num_days`` are: TINYINT, SMALLINT, INTEGER.

.. spark:function:: date_format(timestamp, dateFormat) -> string

    Converts ``timestamp`` to a string in the format specified by ``dateFormat``.
    The format follows Spark's
    `Datetime patterns
    <https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html>`_.

        SELECT date_format('2020-01-29', 'yyyy'); -- '2020'
        SELECT date_format('2024-05-30 08:00:00', 'yyyy-MM-dd'); -- '2024-05-30'

.. spark:function:: date_from_unix_date(integer) -> date

    Creates date from the number of days since 1970-01-01 in either direction. Returns null when input is null.

        SELECT date_from_unix_date(1); -- '1970-01-02'
        SELECT date_from_unix_date(-1); -- '1969-12-31'

.. spark:function:: date_sub(start_date, num_days) -> date

    Returns the date that is ``num_days`` before ``start_date``. According to the inputs,
    the returned date will wrap around between the minimum negative date and
    maximum positive date. date_sub('1969-12-31', -2147483648) get 5881580-07-11,
    and date_sub('2023-07-10', -2147483648) get -5877588-12-29.

    ``num_days`` can be positive or negative.
    Supported types for ``num_days`` are: TINYINT, SMALLINT, INTEGER.

.. spark:function:: date_trunc(fmt, ts) -> timestamp

    Returns timestamp ``ts`` truncated to the unit specified by the format model ``fmt``.
    Returns NULL if ``fmt`` is invalid. ``fmt`` as "MICROSECOND" and abbreviated unit string are allowed.

    ``fmt`` is case insensitive and must be one of the following:
        * "YEAR", "YYYY", "YY" - truncate to the first date of the year that the ``ts`` falls in, the time part will be zero out
        * "QUARTER" - truncate to the first date of the quarter that the ``ts`` falls in, the time part will be zero out
        * "MONTH", "MM", "MON" - truncate to the first date of the month that the ``ts`` falls in, the time part will be zero out
        * "WEEK" - truncate to the Monday of the week that the ``ts`` falls in, the time part will be zero out
        * "DAY", "DD" - zero out the time part
        * "HOUR" - zero out the minute and second with fraction part
        * "MINUTE"- zero out the second with fraction part
        * "SECOND" - zero out the second fraction part
        * "MILLISECOND" - zero out the microseconds
        * "MICROSECOND" - everything remains.

    ::

        SELECT date_trunc('YEAR', '2015-03-05T09:32:05.359'); -- 2015-01-01 00:00:00
        SELECT date_trunc('YYYY', '2015-03-05T09:32:05.359'); -- 2015-01-01 00:00:00
        SELECT date_trunc('YY', '2015-03-05T09:32:05.359'); -- 2015-01-01 00:00:00
        SELECT date_trunc('QUARTER', '2015-03-05T09:32:05.359'); -- 2015-01-01 00:00:00
        SELECT date_trunc('MONTH', '2015-03-05T09:32:05.359'); -- 2015-03-01 00:00:00
        SELECT date_trunc('MM', '2015-03-05T09:32:05.359'); -- 2015-03-01 00:00:00
        SELECT date_trunc('MON', '2015-03-05T09:32:05.359'); -- 2015-03-01 00:00:00
        SELECT date_trunc('WEEK', '2015-03-05T09:32:05.359'); -- 2015-03-02 00:00:00
        SELECT date_trunc('DAY', '2015-03-05T09:32:05.359'); -- 2015-03-05 00:00:00
        SELECT date_trunc('DD', '2015-03-05T09:32:05.359'); -- 2015-03-05 00:00:00
        SELECT date_trunc('HOUR', '2015-03-05T09:32:05.359'); -- 2015-03-05 09:00:00
        SELECT date_trunc('MINUTE', '2015-03-05T09:32:05.359'); -- 2015-03-05 09:32:00
        SELECT date_trunc('SECOND', '2015-03-05T09:32:05.359'); -- 2015-03-05 09:32:05
        SELECT date_trunc('MILLISECOND', '2015-03-05T09:32:05.123456'); -- 2015-03-05 09:32:05.123
        SELECT date_trunc('MICROSECOND', '2015-03-05T09:32:05.123456'); -- 2015-03-05 09:32:05.123456
        SELECT date_trunc('', '2015-03-05T09:32:05.123456'); -- NULL
        SELECT date_trunc('Y', '2015-03-05T09:32:05.123456'); -- NULL

.. spark:function:: datediff(endDate, startDate) -> integer

    Returns the number of days from startDate to endDate. Only DATE type is allowed
    for input. ::

        SELECT datediff('2009-07-31', '2009-07-30'); -- 1
        SELECT datediff('2009-07-30', '2009-07-31'); -- -1

.. spark:function:: dayofmonth(date) -> integer

    Returns the day of month of the date. ::

        SELECT dayofmonth('2009-07-30'); -- 30

.. spark:function:: dayofyear(date) -> integer

    Returns the day of year of the date. ::

        SELECT dayofyear('2016-04-09'); -- 100

.. spark:function:: dayofweek(date) -> integer

    Returns the day of the week for date (1 = Sunday, 2 = Monday, ..., 7 = Saturday).

        SELECT dayofweek('2009-07-30'); -- 5
        SELECT dayofweek('2023-08-22'); -- 3

.. spark:function:: from_unixtime(unixTime, format) -> string

    Adjusts ``unixTime`` (elapsed seconds since UNIX epoch) to configured session timezone, then
    converts it to a formatted time string according to ``format``. Only supports BIGINT type for
    ``unixTime``.
    `Valid patterns for date format
    <https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html>`_. This function will convert input to
    milliseconds, and integer overflow is allowed in the conversion, which aligns with Spark. See the below third
    example where INT64_MAX is used, -1000 milliseconds are produced by INT64_MAX * 1000 due to integer overflow. ::

        SELECT from_unixtime(100, 'yyyy-MM-dd HH:mm:ss'); -- '1970-01-01 00:01:40'
        SELECT from_unixtime(3600, 'yyyy'); -- '1970'
        SELECT from_unixtime(9223372036854775807, "yyyy-MM-dd HH:mm:ss");  -- '1969-12-31 23:59:59'

    If we run the following query in the `Asia/Shanghai` time zone: ::

        SELECT from_unixtime(100, 'yyyy-MM-dd HH:mm:ss'); -- '1970-01-01 08:01:40'
        SELECT from_unixtime(3600, 'yyyy'); -- '1970'
        SELECT from_unixtime(9223372036854775807, "yyyy-MM-dd HH:mm:ss");  -- '1970-01-01 07:59:59'

.. spark:function:: from_utc_timestamp(timestamp, string) -> timestamp

    Returns the timestamp value from UTC timezone to the given timezone. ::

        SELECT from_utc_timestamp('2015-07-24 07:00:00', 'America/Los_Angeles'); -- '2015-07-24 00:00:00'

.. spark:function:: get_timestamp(string, dateFormat) -> timestamp

    Returns timestamp by parsing ``string`` according to the specified ``dateFormat``.
    The format follows Spark's
    `Datetime patterns
    <https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html>`_. ::

        SELECT get_timestamp('1970-01-01', 'yyyy-MM-dd);  -- timestamp `1970-01-01`
        SELECT get_timestamp('1970-01-01', 'yyyy-MM');  -- NULL (parsing error)
        SELECT get_timestamp('1970-01-01', null);  -- NULL

.. spark:function:: hour(timestamp) -> integer

    Returns the hour of ``timestamp``.::

        SELECT hour('2009-07-30 12:58:59'); -- 12

.. spark:function:: last_day(date) -> date

    Returns the last day of the month which the date belongs to.

.. spark:function:: make_date(year, month, day) -> date

    Returns the date from year, month and day fields.
    ``year``, ``month`` and ``day`` must be ``INTEGER``.
    Returns NULL if inputs are not valid.

    The valid inputs need to meet the following conditions,
    ``month`` need to be from 1 (January) to 12 (December).
    ``day`` need to be from 1 to 31, and matches the number of days in each month.
    days of ``year-month-day - 1970-01-01`` need to be in the range of INTEGER type.

.. spark:function:: make_timestamp(year, month, day, hour, minute, second[, timezone]) -> timestamp

    Create timestamp from ``year``, ``month``, ``day``, ``hour``, ``minute`` and ``second`` fields.
    If the ``timezone`` parameter is provided,
    the function interprets the input time components as being in the specified ``timezone``.
    Otherwise the function assumes the inputs are in the session's configured time zone.
    Requires ``session_timezone`` to be set, or an exceptions will be thrown.

    Arguments:
        * year - the year to represent, within the Joda datetime
        * month - the month-of-year to represent, from 1 (January) to 12 (December)
        * day - the day-of-month to represent, from 1 to 31
        * hour - the hour-of-day to represent, from 0 to 23
        * minute - the minute-of-hour to represent, from 0 to 59
        * second - the second-of-minute and its micro-fraction to represent, from 0 to 60.
          The value can be either an integer like 13, or a fraction like 13.123.
          The fractional part can have up to 6 digits to represent microseconds.
          If the sec argument equals to 60, the seconds field is set
          to 0 and 1 minute is added to the final timestamp.
        * timezone - the time zone identifier. For example, CET, UTC and etc.

    Returns the timestamp adjusted to the GMT time zone.
    Returns NULL for invalid or NULL input. ::

        SELECT make_timestamp(2014, 12, 28, 6, 30, 45.887); -- 2014-12-28 06:30:45.887
        SELECT make_timestamp(2014, 12, 28, 6, 30, 45.887, 'CET'); -- 2014-12-28 05:30:45.887
        SELECT make_timestamp(2019, 6, 30, 23, 59, 60); -- 2019-07-01 00:00:00
        SELECT make_timestamp(2019, 6, 30, 23, 59, 1); -- 2019-06-30 23:59:01
        SELECT make_timestamp(null, 7, 22, 15, 30, 0); -- NULL
        SELECT make_timestamp(2014, 12, 28, 6, 30, 60.000001); -- NULL
        SELECT make_timestamp(2014, 13, 28, 6, 30, 45.887); -- NULL

.. spark:function:: make_ym_interval([years[, months]]) -> interval year to month

    Make year-month interval from ``years`` and ``months`` fields.
    Returns the actual year-month with month in the range of [0, 11].
    Both ``years`` and ``months`` can be zero, positive or negative.
    Throws an error when inputs lead to int overflow,
    e.g., make_ym_interval(178956970, 8). ::

        SELECT make_ym_interval(1, 2); -- 1-2
        SELECT make_ym_interval(1, 0); -- 1-0
        SELECT make_ym_interval(-1, 1); -- -0-11
        SELECT make_ym_interval(1, 100); -- 9-4
        SELECT make_ym_interval(1, 12); -- 2-0
        SELECT make_ym_interval(1, -12); -- 0-0
        SELECT make_ym_interval(2); -- 2-0
        SELECT make_ym_interval(); -- 0-0

.. spark:function:: minute(timestamp) -> integer

    Returns the minutes of ``timestamp``.::

        SELECT minute('2009-07-30 12:58:59'); -- 58

.. spark:function:: month(date) -> integer

    Returns the month of ``date``. ::

        SELECT month('2009-07-30'); -- 7

.. spark:function:: next_day(startDate, dayOfWeek) -> date

    Returns the first date which is later than ``startDate`` and named as ``dayOfWeek``.
    Returns null if ``dayOfWeek`` is invalid.
    ``dayOfWeek`` is case insensitive and must be one of the following:
    ``SU``, ``SUN``, ``SUNDAY``, ``MO``, ``MON``, ``MONDAY``, ``TU``, ``TUE``, ``TUESDAY``,
    ``WE``, ``WED``, ``WEDNESDAY``, ``TH``, ``THU``, ``THURSDAY``, ``FR``, ``FRI``, ``FRIDAY``,
    ``SA``, ``SAT``, ``SATURDAY``. ::

        SELECT next_day('2015-07-23', "Mon"); -- '2015-07-27'
        SELECT next_day('2015-07-23', "mo"); -- '2015-07-27'
        SELECT next_day('2015-07-23', "Tue"); -- '2015-07-28'
        SELECT next_day('2015-07-23', "tu"); -- '2015-07-28'
        SELECT next_day('2015-07-23', "we"); -- '2015-07-29'

.. spark:function:: quarter(date) -> integer

    Returns the quarter of ``date``. The value ranges from ``1`` to ``4``. ::

        SELECT quarter('2009-07-30'); -- 3

.. spark:function:: second(timestamp) -> integer

    Returns the seconds of ``timestamp``. ::

        SELECT second('2009-07-30 12:58:59'); -- 59

.. spark:function:: timestampdiff(unit, timestamp1, timestamp2) -> bigint

    Returns ``timestamp2`` - ``timestamp1`` expressed in terms of ``unit``, the fraction
    part is truncated.
    Throws exception if ``unit`` is invalid.
    ``unit`` is case insensitive and must be one of the following:
    ``YEAR``, ``QUARTER``, ``MONTH``, ``WEEK``, ``DAY``, ``HOUR``, ``MINUTE``, ``SECOND``,
    ``MILLISECOND``, ``MICROSECOND``. ::

        SELECT timestampdiff(YEAR, '2020-02-29 10:00:00.500', '2030-02-28 10:00:00.500'); -- 9
        SELECT timestampdiff(DAY, '2019-01-30 10:00:00.500', '2020-02-29 10:00:00.500'); -- 395
        SELECT timestampdiff(SECOND, '2019-02-28 10:00:00.500', '2019-03-01 10:00:00.500'); -- 86400
        SELECT timestampdiff(MICROSECOND, '2019-02-28 10:00:00.000000', '2019-02-28 10:01:00.500999'); -- 60500999

.. spark:function:: timestamp_micros(x) -> timestamp

    Returns timestamp from the number of microseconds since UTC epoch.
    Supported types are: TINYINT, SMALLINT, INTEGER and BIGINT.::

        SELECT timestamp_micros(1230219000123123); -- '2008-12-25 15:30:00.123123'

.. spark:function:: timestamp_millis(x) -> timestamp

    Returns timestamp from the number of milliseconds since UTC epoch.
    Supported types are: TINYINT, SMALLINT, INTEGER and BIGINT.::

        SELECT timestamp_millis(1230219000123); -- '2008-12-25 15:30:00.123'

.. spark:function:: to_unix_timestamp(date) -> bigint
   :noindex:

    Alias for ``unix_timestamp(date) -> bigint``.

.. spark:function:: to_unix_timestamp(string) -> bigint

    Alias for ``unix_timestamp(string) -> bigint``.

.. spark:function:: to_unix_timestamp(string, format) -> bigint
   :noindex:

    Alias for ``unix_timestamp(string, format) -> bigint``.

.. spark:function:: to_unix_timestamp(timestamp) -> bigint
   :noindex:

    Alias for ``unix_timestamp(timestamp) -> bigint``.

.. spark:function:: to_utc_timestamp(timestamp, string) -> timestamp

    Returns the timestamp value from the given timezone to UTC timezone. ::

        SELECT to_utc_timestamp('2015-07-24 00:00:00', 'America/Los_Angeles'); -- '2015-07-24 07:00:00'

.. spark:function:: trunc(date, fmt) -> date

    Returns ``date`` truncated to the unit specified by the format model ``fmt``.
    Returns NULL if ``fmt`` is invalid.

    ``fmt`` is case insensitive and must be one of the following:
        * "YEAR", "YYYY", "YY" - truncate to the first date of the year that the ``date`` falls in
        * "QUARTER" - truncate to the first date of the quarter that the ``date`` falls in
        * "MONTH", "MM", "MON" - truncate to the first date of the month that the ``date`` falls in
        * "WEEK" - truncate to the Monday of the week that the ``date`` falls in

    ::

        SELECT trunc('2019-08-04', 'week'); -- 2019-07-29
        SELECT trunc('2019-08-04', 'quarter'); -- 2019-07-01
        SELECT trunc('2009-02-12', 'MM'); -- 2009-02-01
        SELECT trunc('2015-10-27', 'YEAR'); -- 2015-01-01
        SELECT trunc('2015-10-27', ''); -- NULL
        SELECT trunc('2015-10-27', 'day'); -- NULL

.. spark:function:: unix_date(date) -> integer

    Returns the number of days since 1970-01-01. ::

        SELECT unix_date('1970-01-01'); -- '0'
        SELECT unix_date('1970-01-02'); -- '1'
        SELECT unix_date('1969-12-31'); -- '-1'

.. spark:function:: unix_micros(timestamp) -> bigint

    Returns the number of microseconds since 1970-01-01 00:00:00 UTC.::

        SELECT unix_micros('1970-01-01 00:00:01'); -- 1000000

.. spark:function:: unix_millis(timestamp) -> bigint

    Returns the number of milliseconds since 1970-01-01 00:00:00 UTC. Truncates
    higher levels of precision.::

        SELECT unix_millis('1970-01-01 00:00:01'); -- 1000

.. spark:function:: unix_seconds(timestamp) -> bigint

    Returns the number of seconds since 1970-01-01 00:00:00 UTC. ::

        SELECT unix_seconds('1970-01-01 00:00:01'); -- 1

.. spark:function:: unix_timestamp() -> bigint

    Returns the current UNIX timestamp in seconds.

.. spark:function:: unix_timestamp(date) -> bigint

    Converts the time represented by ``date`` at the configured session timezone to the GMT time, and extracts the seconds. ::

        SELECT unix_timestamp('1970-01-01'); -- 0
        SELECT unix_timestamp('2024-10-01'); -- 1727740800
        SELECT unix_timestamp('-2025-02-18'); -- -126065894400

.. spark:function:: unix_timestamp(string) -> bigint
   :noindex:

    Returns the UNIX timestamp of time specified by ``string``. Assumes the
    format ``yyyy-MM-dd HH:mm:ss``. Returns null if ``string`` does not match
    ``format``.

.. spark:function:: unix_timestamp(string, format) -> bigint
   :noindex:

    Returns the UNIX timestamp of time specified by ``string`` using the
    format described in the ``format`` string. The format follows Spark's
    `Datetime patterns for formatting and parsing
    <https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html>`_.
    Returns null if ``string`` does not match ``format`` or if ``format``
    is invalid.

.. spark:function:: unix_timestamp(timestamp) -> bigint

    Returns the UNIX timestamp of the given ``timestamp`` in seconds. ::

        SELECT unix_timestamp(CAST(0 AS TIMESTAMP)); -- 0
        SELECT unix_timestamp(CAST(1739933174 AS TIMESTAMP)); -- 1739933174
        SELECT unix_timestamp(CAST(-1739933174 AS TIMESTAMP)); -- -1739933174

.. function:: week_of_year(x) -> integer

    Returns the `ISO-Week`_ of the year from x. The value ranges from ``1`` to ``53``.
    A week is considered to start on a Monday and week 1 is the first week with >3 days.

.. function:: weekday(date) -> integer

    Returns the day of the week for date (0 = Monday, 1 = Tuesday, …, 6 = Sunday). ::

        SELECT weekday('2015-04-08'); -- 2
        SELECT weekday('2024-02-10'); -- 5

.. _ISO-Week: https://en.wikipedia.org/wiki/ISO_week_date

.. spark:function:: year(x) -> integer

    Returns the year from ``x``.

.. spark:function:: year_of_week(x) -> integer

    Returns the ISO week-numbering year that ``x`` falls in. For example, 2005-01-02 is
    part of the 53rd week of year 2004, so the result is 2004. Only supports DATE type.

        SELECT year_of_week('2005-01-02'); -- 2004

Simple vs. Joda Date Formatter
------------------------------

To align with Spark, Velox supports both `Simple <https://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html>`_
and `Joda <https://www.joda.org/joda-time/>`_ date formmaters to parse/format timestamp/date strings
used in functions :spark:func:`from_unixtime`, :spark:func:`unix_timestamp`, :spark:func:`make_date`
and :spark:func:`to_unix_timestamp`.
If the configuration setting :doc:`spark.legacy_date_formatter <../../configs>` is true,
`Simple` date formmater in lenient mode is used; otherwise, `Joda` is used. It is important
to note that there are some different behaviors between these two formatters.

For :spark:func:`unix_timestamp` and :spark:func:`get_timestamp`, the `Simple` date formatter permits partial date parsing
which means that format can match only a part of input string. For example, if input string is
2015-07-22 10:00:00, it can be parsed using format is yyyy-MM-dd because the parser does not require entire
input to be consumed. In contrast, the `Joda` date formatter performs strict checks to ensure that the
format completely matches the string. If there is any mismatch, exception is thrown. ::

        SELECT get_timestamp('2015-07-22 10:00:00', 'yyyy-MM-dd'); -- timestamp `2015-07-22` (for Simple date formatter)
        SELECT get_timestamp('2015-07-22 10:00:00', 'yyyy-MM-dd'); -- (throws exception) (for Joda date formatter)
        SELECT unix_timestamp('2016-04-08 00:00:00', 'yyyy-MM-dd'); -- 1460041200 (for Simple date formatter)
        SELECT unix_timestamp('2016-04-08 00:00:00', 'yyyy-MM-dd'); -- (throws exception) (for Joda date formatter)

For :spark:func:`from_unixtime` and :spark:func:`get_timestamp`, when `Simple` date formatter is used, null is
returned for invalid format; otherwise, exception is thrown. ::

        SELECT from_unixtime(100, '!@#$%^&*'); -- NULL (parsing error) (for Simple date formatter)
        SELECT from_unixtime(100, '!@#$%^&*'); -- throws exception) (for Joda date formatter)
        SELECT get_timestamp('1970-01-01', '!@#$%^&*'); -- NULL (parsing error) (for Simple date formatter)
        SELECT get_timestamp('1970-01-01', '!@#$%^&*'); -- throws exception) (for Joda date formatter)
