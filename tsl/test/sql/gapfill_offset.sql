-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test "time_bucket_gapfill" with "offset" and "origin" arguments
-------------------------------------------------------------------

\set TEST_BASE_NAME gapfill_offset
SELECT
    format('include/%s_query.sql', :'TEST_BASE_NAME') AS "TEST_QUERY_NAME",
    format('%s/results/%s_results_timebucket.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') AS "TEST_RESULTS_TIMEBUCKET",
    format('%s/results/%s_results_gapfill.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') AS "TEST_RESULTS_GAPFILL" \gset

SELECT format('\! diff -u --label "Time_bucket results" --label "Gapfill results" %s %s', :'TEST_RESULTS_TIMEBUCKET', :'TEST_RESULTS_GAPFILL') AS "DIFF_CMD" \gset

-- If we do "SELECT time_bucket_gapfill(.., start = MIN(ts), finish = max(ts)) FROM t GROUP BY ts" on table "t" with no gaps,
-- the result should be the same as for "SELECT time_bucket_gapfill(.., start = MAX(ts), finish = max(ts)) FROM t GROUP BY ts",
-- which will output regular "time_bucket" results before "start".
-- In this way we can make sure that "offset" and "origin" parameters are applied correctly to match regular "time_bucket" results.

-- We can also test bucket alignment when there are gaps in the data
-- by comparing buckets for "time_bucket" with no gaps between MIN(ts) and MAX(ts) (verified by the above test),
-- i.e. when "\set FILTER true ",
-- and buckets for "time_bucket_gapfill" with only MIN(ts) and MAX(ts) present in the data,
-- i.e. when "\set FILTER 'ts = (SELECT MIN(ts) FROM :TABLE) OR ts = (SELECT MAX(ts) FROM :TABLE)' "
-- forcing it to fill gaps from MIN(ts) to MAX(ts).
-- Buckets should align exactly.
-- We have to skip "count(*)" aggregate for this test as it'll differ between results with gaps and no gaps,
-- therefore "\set COUNT '' ".

-- Test integer gapfills with offsets
CREATE TABLE tb_int(ts INT);
INSERT INTO tb_int VALUES (-7),(-2),(1),(2),(3),(7),(11);

CREATE TABLE tb_smallint(ts SMALLINT);
INSERT INTO tb_smallint VALUES (-7),(-2),(1),(2),(3),(7),(11);

CREATE TABLE tb_bigint(ts BIGINT);
INSERT INTO tb_bigint VALUES (-7),(-2),(1),(2),(3),(7),(11);


SET time zone 'UTC';

-- Test time gapfills with day/month offsets
CREATE TABLE tb_date(ts DATE);
INSERT INTO tb_date VALUES ('2020-01-01'), ('2020-01-02'), ('2020-01-03'), ('2020-01-07'),
('2020-01-11'), ('2019-12-30'), ('2019-12-25');

CREATE TABLE tb_ts(ts TIMESTAMP);
INSERT INTO tb_ts VALUES ('2020-01-01'), ('2020-01-02'), ('2020-01-03'), ('2020-01-07'),
('2020-01-11'), ('2019-12-30'), ('2019-12-25');

CREATE TABLE tb_tstz(ts TIMESTAMPTZ);
INSERT INTO tb_tstz VALUES ('2020-01-01'), ('2020-01-02'), ('2020-01-03'), ('2020-01-07'),
('2020-01-11'), ('2019-12-30'), ('2019-12-25');

-- 'Europe/Berlin' time zone has DST going back on 2025-10-26
CREATE TABLE tb_time(ts TIMESTAMP);
INSERT INTO tb_time VALUES
    ('2025-10-25 23:05:00'),
    ('2025-10-25 23:59:45'),
    ('2025-10-26 00:00:00'),
    ('2025-10-26 00:05:00'),
    ('2025-10-26 00:59:00'),
    ('2025-10-26 01:30:00');

CREATE TABLE tb_timetz(ts TIMESTAMPTZ);
INSERT INTO tb_timetz VALUES
    ('2025-10-25 23:05:00+00'),
    ('2025-10-25 23:59:45+00'),
    ('2025-10-26 00:00:00+00'),
    ('2025-10-26 00:05:00+00'),
    ('2025-10-26 00:59:00+00'),
    ('2025-10-26 01:30:00+00');

-- Negative tests
\set ON_ERROR_STOP 0

-- non-simple expressions as offset and origin
SELECT time_bucket_gapfill(1, ts, ts+1, 11, 12) FROM tb_int GROUP BY 1;

SELECT time_bucket_gapfill(INTERVAL '1 hour', tb_ts.ts, (tb_int.ts * INTERVAL '1 second')::Interval, NULL,
(SELECT min(ts) FROM tb_ts), (SELECT max(ts) FROM tb_ts)) FROM tb_ts, tb_int GROUP BY 1;

SELECT time_bucket_gapfill(INTERVAL '1 hour', ts, NULL, ts,
(SELECT min(ts) FROM tb_ts), (SELECT max(ts) FROM tb_ts)) FROM tb_ts GROUP BY 1;

-- NULLs for required parameters combined with offset/origin
-- integer + offset
SELECT time_bucket_gapfill(NULL, ts, 1, 11, 12) FROM tb_int GROUP BY 1;
SELECT time_bucket_gapfill(1, NULL::int, 1, 11, 12) FROM tb_int GROUP BY 1;

-- time + origin
SELECT time_bucket_gapfill(NULL, ts, NULL, '1970-01-01'::timestamp,
(SELECT min(ts) FROM tb_ts), (SELECT max(ts) FROM tb_ts)) FROM tb_ts GROUP BY 1;

SELECT time_bucket_gapfill(INTERVAL '1 hour', NULL::timestamp, NULL, '1970-01-01'::timestamp,
(SELECT min(ts) FROM tb_ts), (SELECT max(ts) FROM tb_ts)) FROM tb_ts GROUP BY 1;

-- time + offset
SELECT time_bucket_gapfill(NULL, ts, INTERVAL '1 min', NULL,
(SELECT min(ts) FROM tb_ts), (SELECT max(ts) FROM tb_ts)) FROM tb_ts GROUP BY 1;

SELECT time_bucket_gapfill(INTERVAL '1 hour', NULL::timestamp, INTERVAL '1 min', NULL,
(SELECT min(ts) FROM tb_ts), (SELECT max(ts) FROM tb_ts)) FROM tb_ts GROUP BY 1;

-- time with timezone + origin
SELECT time_bucket_gapfill(NULL, ts, 'UTC', NULL, '1970-01-01'::timestamp,
(SELECT min(ts) FROM tb_ts), (SELECT max(ts) FROM tb_ts)) FROM tb_tstz GROUP BY 1;

SELECT time_bucket_gapfill(INTERVAL '1 hour', NULL::timestamp, 'UTC', NULL, '1970-01-01'::timestamp,
(SELECT min(ts) FROM tb_ts), (SELECT max(ts) FROM tb_ts)) FROM tb_tstz GROUP BY 1;

SELECT time_bucket_gapfill(INTERVAL '1 hour', ts, NULL::text, NULL, '1970-01-01'::timestamp,
(SELECT min(ts) FROM tb_ts), (SELECT max(ts) FROM tb_ts)) FROM tb_tstz GROUP BY 1;

-- time with timezone + offset
SELECT time_bucket_gapfill(NULL, ts, 'UTC', INTERVAL '1 min', NULL,
(SELECT min(ts) FROM tb_ts), (SELECT max(ts) FROM tb_ts)) FROM tb_tstz GROUP BY 1;

SELECT time_bucket_gapfill(INTERVAL '1 hour', NULL::timestamp, 'UTC', INTERVAL '1 min', NULL,
(SELECT min(ts) FROM tb_ts), (SELECT max(ts) FROM tb_ts)) FROM tb_tstz GROUP BY 1;

SELECT time_bucket_gapfill(INTERVAL '1 hour', ts, NULL::text, INTERVAL '1 min', NULL,
(SELECT min(ts) FROM tb_ts), (SELECT max(ts) FROM tb_ts)) FROM tb_tstz GROUP BY 1;

\set ON_ERROR_STOP 1

-- display aggregate, don't filter data between MIN and MAX
\set COUNT ', count(*)'
\set FILTER true

-- Obtain "time_bucket_gapfill" results over all rows
\set START min
\o :TEST_RESULTS_GAPFILL
\ir :TEST_QUERY_NAME
\o

-- Obtain "time_bucket" results over all rows
\set START max
\o :TEST_RESULTS_TIMEBUCKET
\ir :TEST_QUERY_NAME
\o

-- compare gapfill vs. time_bucket results over same rows
:DIFF_CMD

-- do not display aggregate
\set COUNT ''

-- Obtain "time_bucket" results over all rows
\set START max
\o :TEST_RESULTS_TIMEBUCKET
\ir :TEST_QUERY_NAME
\o

-- Obtain "time_bucket_gapfill" results with no data after MIN, forcing it to fill gaps
\set FILTER 'ts = (SELECT MIN(ts) FROM :TABLE) OR ts = (SELECT MAX(ts) FROM :TABLE)'
\set START min
\o :TEST_RESULTS_GAPFILL
\ir :TEST_QUERY_NAME
\o

-- compare gapfill gap filled rows vs. time_bucket rows
:DIFF_CMD

-- Make sure sort_transform works for new time_bucket_gapfill signatures
CREATE TABLE repro_ht(time TIMESTAMPTZ NOT NULL, value DOUBLE PRECISION);
SELECT create_hypertable('repro_ht', 'time');

INSERT INTO repro_ht VALUES
  ('2024-01-01 01:30:00+00', 10),
  ('2024-01-01 03:45:00+00', 20);

SELECT time_bucket_gapfill(
    '1 hour'::interval,
    time,
    '30 minutes'::interval,   -- offset
    NULL::timestamptz,         -- origin
    '2024-01-01'::timestamptz, -- start
    '2024-01-01 08:00:00'::timestamptz -- finish
  ),
  min(value)
FROM repro_ht
WHERE time >= '2024-01-01' AND time < '2024-01-01 08:00:00'
GROUP BY 1 ORDER BY 1;

drop table repro_ht cascade;

-- Make sure NULL offset which is not NULL constant is handled correctly
SELECT time_bucket_gapfill(
    '1 hour'::interval,
    ts,
    CASE WHEN random() < 0 THEN '0 hour'::interval END, -- offset evaluates to NULL
    '2000-01-03'::timestamptz, -- use default origin as origin has to be not NULL if offset is NULL
    '2024-01-01'::timestamptz,
    '2024-01-01 04:00'::timestamptz
)
FROM generate_series(
    '2024-01-01'::timestamptz,
    '2024-01-01 04:00'::timestamptz,
    '1 hour'::interval
) AS ts
GROUP BY 1 ORDER BY 1;

drop table tb_int, tb_smallint, tb_bigint cascade;
drop table tb_date, tb_ts, tb_tstz cascade;
drop table tb_time, tb_timetz cascade;

-- Corner cases for "time_bucket_gapfill" with "offset" and "origin" arguments,
-- exercised together with "locf" and "interpolate".
---------------------------------------------------------------------------------
--
-- Verification strategy
-- ---------------------
-- For every scenario we build a dataset that is aligned to the bucket grid and
-- carries the bucket ordinal as its value, then we remove some interior buckets
-- so that gaps appear. We then compare the gap-filled output against an
-- INDEPENDENT reference that is computed from the trusted, pre-existing
-- "time_bucket" (which already supports "offset"/"origin") over data with no
-- gaps:
--
--   * locf        -> max(bucket ordinal), same value for "time_bucket" and "time_bucket_gapfill"
--
--   * interpolate -> the bucket ordinal. Compute and compare only when bucket spacing is uniform.
--
-- If gap-filled output equals the reference it proves two things at once:
--   1. the "offset"/"origin" bucket boundaries computed by time_bucket_gapfill
--      line up with those of time_bucket across the corner (DST, month end, ...)
--   2. locf/interpolate fill the gaps with the correct values.

SET timezone TO 'UTC';

-- Build the subject (gap-filled) and the reference (oracle) views for a scenario.
--   tb   : a time_bucket(...) expression over column "ts" (trusted reference)
--   gf   : the matching time_bucket_gapfill(...) expression over column "ts"
--   grid : a generate_series(...) producing all raw timestamps in range
CREATE OR REPLACE FUNCTION cc_build(tb text, gf text, grid text) RETURNS void
LANGUAGE plpgsql AS $fn$
BEGIN
  DROP TABLE IF EXISTS cc_allb CASCADE;
  EXECUTE format($$
    CREATE TEMP TABLE cc_allb AS
      SELECT b AS bucket,
             row_number() OVER (ORDER BY b) AS rn,
             count(*)     OVER ()           AS cnt
      FROM (SELECT DISTINCT %s AS b FROM %s AS g(ts)) s
  $$, tb, grid);

-- The value stored per kept bucket is the bucket ordinal. We keep the first two,
-- the middle, and the last two buckets so there are multi-bucket gaps on both
-- sides and a lone interior point (which forces interpolate to use a prev and a
-- next neighbour).
  DROP TABLE IF EXISTS cc_data CASCADE;
  CREATE TEMP TABLE cc_data AS
    SELECT bucket AS ts, rn::int AS v
    FROM cc_allb
    WHERE rn IN (1, 2) OR rn = (cnt + 1) / 2 OR rn IN (cnt - 1, cnt);

  -- results produced with "time_bucket_gapfill"
  EXECUTE format($$
    CREATE OR REPLACE TEMP VIEW cc_subj AS
      SELECT %s AS bucket, locf(max(v)) AS locf, interpolate(max(v)) AS interp
      FROM cc_data GROUP BY 1
  $$, gf);

  -- results produced with "time_bucket", should match exactly
  EXECUTE format($$
    CREATE OR REPLACE TEMP VIEW cc_oracle AS
      WITH real AS (
        SELECT %s AS bucket, max(v) AS v FROM cc_data GROUP BY 1
      ),
      grp AS (
        SELECT a.bucket, a.rn, r.v,
               count(r.v) OVER (ORDER BY a.bucket) AS g
        FROM cc_allb a LEFT JOIN real r USING (bucket)
      )
      SELECT bucket,
             max(v) OVER (PARTITION BY g) AS locf,
             rn::int                      AS interp
      FROM grp
  $$, tb);
END;
$fn$;

-- Symmetric difference between subject and oracle on a single metric column.
-- Returns "match" (0 rows) on success.
CREATE OR REPLACE FUNCTION cc_check(label text, metric text) RETURNS text
LANGUAGE plpgsql AS $fn$
DECLARE n bigint;
BEGIN
  EXECUTE format($$
    SELECT count(*) FROM (
      (SELECT bucket, %1$I FROM cc_subj   EXCEPT SELECT bucket, %1$I FROM cc_oracle)
      UNION ALL
      (SELECT bucket, %1$I FROM cc_oracle EXCEPT SELECT bucket, %1$I FROM cc_subj)
    ) d
  $$, metric) INTO n;
  RETURN format('%s [%s]: %s', label, metric,
                CASE WHEN n = 0 THEN 'match' ELSE n::text END);
END;
$fn$;

---------------------------------------------------------------------------------
-- A. Integer buckets with a plain offset (no origin), positive and negative.
---------------------------------------------------------------------------------

-- A1: width 10, offset +3
SELECT cc_build(
  $$time_bucket(10, ts, 3)$$,
  $$time_bucket_gapfill(10, ts, 3, -50, 60)$$,
  $$generate_series(-50, 60)$$
);
SELECT * FROM cc_subj ORDER BY bucket;
SELECT cc_check('A1 int offset +3', 'locf');
SELECT cc_check('A1 int offset +3', 'interp');

-- A2: width 10, offset -3 (negative offset)
SELECT cc_build(
  $$time_bucket(10, ts, -3)$$,
  $$time_bucket_gapfill(10, ts, -3, -50, 60)$$,
  $$generate_series(-50, 60)$$
);
SELECT * FROM cc_subj ORDER BY bucket;
SELECT cc_check('A2 int offset -3', 'locf');
SELECT cc_check('A2 int offset -3', 'interp');

---------------------------------------------------------------------------------
-- B. DST fall-back (Europe/Berlin, 2025-10-26: 03:00 -> 02:00 local = 01:00 UTC).
--    1 hour buckets with a +30 min offset. The repeated local hour is folded;
--    UTC bucket spacing stays uniform so interpolate is exact.
---------------------------------------------------------------------------------
SELECT cc_build(
  $$time_bucket('1 hour'::interval, ts, 'Europe/Berlin', NULL::timestamptz, '30 min'::interval)$$,
  $$time_bucket_gapfill('1 hour'::interval, ts, 'Europe/Berlin', '30 min'::interval, NULL::timestamptz,
                        '2025-10-25 21:00+00'::timestamptz, '2025-10-26 05:00+00'::timestamptz)$$,
  $$generate_series('2025-10-25 21:00+00'::timestamptz, '2025-10-26 05:00+00', '5 min')$$
);
SELECT * FROM cc_subj ORDER BY bucket;
SELECT cc_check('B DST fall-back 1h +30min', 'locf');
SELECT cc_check('B DST fall-back 1h +30min', 'interp');

---------------------------------------------------------------------------------
-- C. DST spring-forward (Europe/Berlin, 2025-03-30: 02:00 -> 03:00 local, the
--    local hour 02:00-03:00 does not exist). 1 hour buckets with a +30 min
--    offset. UTC spacing stays uniform so interpolate is exact.
---------------------------------------------------------------------------------
SELECT cc_build(
  $$time_bucket('1 hour'::interval, ts, 'Europe/Berlin', NULL::timestamptz, '30 min'::interval)$$,
  $$time_bucket_gapfill('1 hour'::interval, ts, 'Europe/Berlin', '30 min'::interval, NULL::timestamptz,
                        '2025-03-29 23:00+00'::timestamptz, '2025-03-30 06:00+00'::timestamptz)$$,
  $$generate_series('2025-03-29 23:00+00'::timestamptz, '2025-03-30 06:00+00', '5 min')$$
);
SELECT * FROM cc_subj ORDER BY bucket;
SELECT cc_check('C DST spring-forward 1h +30min', 'locf');
SELECT cc_check('C DST spring-forward 1h +30min', 'interp');

---------------------------------------------------------------------------------
-- D. DST fall-back with 1 DAY buckets and a +6 hour offset. The day that
--    contains the fall-back is 25 hours long, so the bucket spacing is NOT
--    uniform across the fold. locf is still well defined; interpolate is shown
--    for completeness but is not asserted against the linear reference.
---------------------------------------------------------------------------------
SELECT cc_build(
  $$time_bucket('1 day'::interval, ts, 'Europe/Berlin', NULL::timestamptz, '6 hours'::interval)$$,
  $$time_bucket_gapfill('1 day'::interval, ts, 'Europe/Berlin', '6 hours'::interval, NULL::timestamptz,
                        '2025-10-22 00:00+00'::timestamptz, '2025-10-30 00:00+00'::timestamptz)$$,
  $$generate_series('2025-10-22 00:00+00'::timestamptz, '2025-10-30 00:00+00', '1 hour')$$
);
SELECT * FROM cc_subj ORDER BY bucket;
SELECT cc_check('D DST fall-back 1 day +6h', 'locf');

---------------------------------------------------------------------------------
-- E. DST fall-back, 1 hour buckets, with BOTH origin and offset supplied (and
--    an explicit time zone). Verifies the combined origin+offset signature.
---------------------------------------------------------------------------------
SELECT cc_build(
  $$time_bucket('1 hour'::interval, ts, 'Europe/Berlin', '2025-10-01 00:07+00'::timestamptz, '15 min'::interval)$$,
  $$time_bucket_gapfill('1 hour'::interval, ts, 'Europe/Berlin', '15 min'::interval, '2025-10-01 00:07+00'::timestamptz,
                        '2025-10-25 21:00+00'::timestamptz, '2025-10-26 05:00+00'::timestamptz)$$,
  $$generate_series('2025-10-25 21:00+00'::timestamptz, '2025-10-26 05:00+00', '5 min')$$
);
SELECT * FROM cc_subj ORDER BY bucket;
SELECT cc_check('E DST origin+offset 1h', 'locf');
SELECT cc_check('E DST origin+offset 1h', 'interp');

---------------------------------------------------------------------------------
-- F. Bucket boundaries crossing a month end with an offset that is NOT counted
--    in months. 10 day buckets with a +3 day offset over a leap year, so
--    boundaries cross Jan/Feb and Feb(29)/Mar. Uses plain timestamp (no tz).
---------------------------------------------------------------------------------
SELECT cc_build(
  $$time_bucket('10 days'::interval, ts, '3 days'::interval)$$,
  $$time_bucket_gapfill('10 days'::interval, ts, '3 days'::interval, NULL::timestamp,
                        '2024-01-05 00:00'::timestamp, '2024-03-20 00:00'::timestamp)$$,
  $$generate_series('2024-01-05 00:00'::timestamp, '2024-03-20 00:00', '6 hours')$$
);
SELECT * FROM cc_subj ORDER BY bucket;
SELECT cc_check('F month-end 10d offset +3d (leap)', 'locf');
SELECT cc_check('F month-end 10d offset +3d (leap)', 'interp');

---------------------------------------------------------------------------------
-- G. Month end crossing with a DATE column and an origin (no offset). 7 day
--    buckets anchored at a mid-February origin, crossing Feb(29)/Mar in the
--    leap year 2024.
---------------------------------------------------------------------------------
SELECT cc_build(
  $$time_bucket('7 days'::interval, ts, '2024-02-10'::date)$$,
  $$time_bucket_gapfill('7 days'::interval, ts, NULL::interval, '2024-02-10'::date,
                        '2024-01-20'::date, '2024-03-25'::date)$$,
  $$generate_series('2024-01-20'::date, '2024-03-25'::date, '1 day')$$
);
SELECT * FROM cc_subj ORDER BY bucket;
SELECT cc_check('G month-end 7d date origin', 'locf');
SELECT cc_check('G month-end 7d date origin', 'interp');

---------------------------------------------------------------------------------
-- H. Year end crossing with the time-zone signature (UTC, so no DST noise), 7
--    day buckets with a +2 day offset spanning Dec 2023 -> Jan 2024.
---------------------------------------------------------------------------------
SELECT cc_build(
  $$time_bucket('7 days'::interval, ts, 'UTC', NULL::timestamptz, '2 days'::interval)$$,
  $$time_bucket_gapfill('7 days'::interval, ts, 'UTC', '2 days'::interval, NULL::timestamptz,
                        '2023-12-10 00:00+00'::timestamptz, '2024-01-25 00:00+00'::timestamptz)$$,
  $$generate_series('2023-12-10 00:00+00'::timestamptz, '2024-01-25 00:00+00', '6 hours')$$
);
SELECT * FROM cc_subj ORDER BY bucket;
SELECT cc_check('H year-end 7d offset +2d', 'locf');
SELECT cc_check('H year-end 7d offset +2d', 'interp');

-- Cleanup
DROP VIEW IF EXISTS cc_subj;
DROP VIEW IF EXISTS cc_oracle;
DROP TABLE IF EXISTS cc_data;
DROP TABLE IF EXISTS cc_allb;
DROP FUNCTION cc_build(text, text, text);
DROP FUNCTION cc_check(text, text);
