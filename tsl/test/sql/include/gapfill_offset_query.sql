-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test integer gapfills with offsets

-- INT tests
\set TABLE tb_int
SELECT time_bucket_gapfill(
    5,
    ts,
    2, -- positive offset
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

SELECT time_bucket_gapfill(
    5,
    ts,
    -2, -- negative offset
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

-- SMALLINT tests
\set TABLE tb_smallint
SELECT time_bucket_gapfill(
    5,
    ts,
    2, -- positive offset
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

SELECT time_bucket_gapfill(
    5,
    ts,
    -2, -- negative offset
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

-- BIGINT tests
\set TABLE tb_bigint
SELECT time_bucket_gapfill(
    5,
    ts,
    2, -- positive offset
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

SELECT time_bucket_gapfill(
    5,
    ts,
    -2, -- negative offset
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

-- Date tests: only day intervals are allowed as offsets
\set TABLE tb_date
SELECT time_bucket_gapfill(
    '5 days'::interval,
    ts,
    '2 days'::interval, -- positive offset
    NULL,
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

SELECT time_bucket_gapfill(
    '5 days'::interval,
    ts,
    -'2 days'::interval, -- negative offset
    NULL,
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

-- Test time gapfills with variable-size offsets

-- Timestamp tests
\set TABLE tb_ts
SELECT time_bucket_gapfill(
    '5 days'::interval,
    ts,
    '2 days'::interval, -- positive offset
    NULL,
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

SELECT time_bucket_gapfill(
    '5 days'::interval,
    ts,
    -'2 days'::interval, -- negative offset
    NULL,
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

SELECT time_bucket_gapfill(
    '5 days'::interval,
    ts,
    '2 months'::interval, -- positive offset
    NULL,
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

SELECT time_bucket_gapfill(
    '5 days'::interval,
    ts,
    -'2 months'::interval, -- negative offset
    NULL,
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

-- TimestampTz tests
\set TABLE tb_tstz
SELECT time_bucket_gapfill(
    '5 days'::interval,
    ts,
    '2 days'::interval, -- positive offset
    NULL,
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

SELECT time_bucket_gapfill(
    '5 days'::interval,
    ts,
    -'2 days'::interval, -- negative offset
    NULL,
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

SELECT time_bucket_gapfill(
    '5 days'::interval,
    ts,
    '2 months'::interval, -- positive offset
    NULL,
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

SELECT time_bucket_gapfill(
    '5 days'::interval,
    ts,
    -'2 months'::interval, -- negative offset
    NULL,
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

-- TimestampTz with time zone tests
SELECT time_bucket_gapfill(
    '5 days'::interval,
    ts,
    'Europe/Berlin',
    '2 days'::interval, -- positive offset
    NULL,
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

SELECT time_bucket_gapfill(
    '5 days'::interval,
    ts,
    'Europe/Berlin',
    -'2 days'::interval, -- negative offset
    NULL,
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

SELECT time_bucket_gapfill(
    '5 days'::interval,
    ts,
    'Europe/Berlin',
    '2 months'::interval, -- positive offset
    NULL,
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

SELECT time_bucket_gapfill(
    '5 days'::interval,
    ts,
    'Europe/Berlin',
    -'2 months'::interval, -- negative offset
    NULL,
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

-- Test time gapfills with fixed-size offsets

-- Timestamp, time offsets
\set TABLE tb_time
SELECT time_bucket_gapfill(
    '1hour'::interval,
    ts,
    '15 min'::interval, -- positive offset
    NULL,
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

SELECT time_bucket_gapfill(
    '1hour'::interval,
    ts,
    '-15 min'::interval, -- negative offset
    NULL,
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

SELECT time_bucket_gapfill(
    '1hour'::interval,
    ts,
    '2 hour 5 min'::interval, -- positive offset
    NULL,
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

SELECT time_bucket_gapfill(
    '1hour'::interval,
    ts,
    '-2 hour 5 min'::interval, -- negative offset
    NULL,
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

-- TimestampTz, time offsets
\set TABLE tb_timetz
SELECT time_bucket_gapfill(
    '1hour'::interval,
    ts,
    '15 min'::interval, -- positive offset
    NULL,
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

SELECT time_bucket_gapfill(
    '1hour'::interval,
    ts,
    '-15 min'::interval, -- negative offset
    NULL,
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

SELECT time_bucket_gapfill(
    '1hour'::interval,
    ts,
    '2 hour 5 min'::interval, -- positive offset
    NULL,
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

SELECT time_bucket_gapfill(
    '1hour'::interval,
    ts,
    '-2 hour 5 min'::interval, -- negative offset
    NULL,
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

-- TimestampTz with time zone, time offsets
SELECT time_bucket_gapfill(
    '1hour'::interval,
    ts,
    'Europe/Berlin',
    '15 min'::interval, -- positive offset
    NULL,
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

SELECT time_bucket_gapfill(
    '1hour'::interval,
    ts,
    'Europe/Berlin',
    '-15 min'::interval, -- negative offset
    NULL,
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

SELECT time_bucket_gapfill(
    '1hour'::interval,
    ts,
    'Europe/Berlin',
    '2 hour 5 min'::interval, -- positive offset
    NULL,
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

SELECT time_bucket_gapfill(
    '1hour'::interval,
    ts,
    'Europe/Berlin',
    '-2 hour 5 min'::interval, -- negative offset
    NULL,
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

-- Origins

-- Date origin
\set TABLE tb_date
SELECT time_bucket_gapfill(
    '5 days'::interval,
    ts,
    NULL,
    '1970-01-01',  --unix epoch
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

SELECT time_bucket_gapfill(
    '5 days'::interval,
    ts,
    NULL,
    '2020-01-01',  -- origin within data set
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

-- Timestamp origin
\set TABLE tb_ts
SELECT time_bucket_gapfill(
    '5 days'::interval,
    ts,
    NULL,
    '1970-01-01',  --unix epoch
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

SELECT time_bucket_gapfill(
    '5 days'::interval,
    ts,
    NULL,
    '2020-01-07',  -- origin within data set, Tuesday
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

-- TimestampTz origin
\set TABLE tb_tstz
SELECT time_bucket_gapfill(
    '5 days'::interval,
    ts,
    NULL,
    '1970-01-01',  --unix epoch
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

SELECT time_bucket_gapfill(
    '5 days'::interval,
    ts,
    NULL,
    '2020-01-07',  -- origin within data set, Tuesday
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

-- TimestampTz with time zone
\set TABLE tb_timetz
SELECT time_bucket_gapfill(
    '1hour'::interval,
    ts,
    'Europe/Berlin',
    NULL,
    '1970-01-01',  --unix epoch
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

SELECT time_bucket_gapfill(
    '1hour'::interval,
    ts,
    'Europe/Berlin',
    NULL,
    '2025-10-25 23:05:00+00',  --origin within data set, before DST switch
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

SELECT time_bucket_gapfill(
    '1hour'::interval,
    ts,
    'Europe/Berlin',
    NULL,
    '2025-10-26 00:59:00+00',  --origin within data set, after DST switch
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

-- origin + offset
SELECT time_bucket_gapfill(
    '1hour'::interval,
    ts,
    'Europe/Berlin',
    '-15 min'::interval, -- negative offset
    '1970-01-01',  --unix epoch
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

SELECT time_bucket_gapfill(
    '1hour'::interval,
    ts,
    'Europe/Berlin',
    '-15 min'::interval, -- negative offset
    '2025-10-25 23:05:00+00',  --origin within data set, before DST switch
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

SELECT time_bucket_gapfill(
    '1hour'::interval,
    ts,
    'Europe/Berlin',
    '-15 min'::interval, -- negative offset
    '2025-10-26 00:59:00+00',  --origin within data set, after DST switch
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

SELECT time_bucket_gapfill(
    '1hour'::interval,
    ts,
    'Europe/Berlin',
    '15 min'::interval, -- positive offset
    '1970-01-01',  --unix epoch
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

SELECT time_bucket_gapfill(
    '1hour'::interval,
    ts,
    'Europe/Berlin',
    '15 min'::interval, -- positive offset
    '2025-10-25 23:05:00+00',  --origin within data set, before DST switch
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

SELECT time_bucket_gapfill(
    '1hour'::interval,
    ts,
    'Europe/Berlin',
    '15 min'::interval, -- positive offset
    '2025-10-26 00:59:00+00',  --origin within data set, after DST switch
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

-- NULL offset and origin: should produce results as if with no offset and origin
\set TABLE tb_int
SELECT time_bucket_gapfill(
    5,
    ts,
    NULL,
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

\set TABLE tb_ts
SELECT time_bucket_gapfill(
    '5 days'::interval,
    ts,
    NULL,
    NULL,
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;

\set TABLE tb_tstz
SELECT time_bucket_gapfill(
    '5 days'::interval,
    ts,
    'Europe/Berlin',
    NULL,
    NULL,
    (SELECT :START(ts) FROM :TABLE), (SELECT max(ts) FROM :TABLE)
), count(*)
FROM :TABLE
GROUP BY 1 ORDER BY 1;
