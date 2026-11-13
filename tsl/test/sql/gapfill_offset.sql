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
