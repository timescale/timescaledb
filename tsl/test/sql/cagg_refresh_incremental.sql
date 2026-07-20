-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

-- Create a user with specific timezone for deterministic output
CREATE ROLE test_cagg_refresh_manual_user WITH LOGIN;
ALTER ROLE test_cagg_refresh_manual_user SET timezone TO 'UTC';
GRANT ALL ON SCHEMA public TO test_cagg_refresh_manual_user;

\c :TEST_DBNAME test_cagg_refresh_manual_user
SET timezone TO 'UTC';

CREATE TABLE conditions (
    time         TIMESTAMP WITH TIME ZONE NOT NULL,
    device_id    INTEGER,
    temperature  NUMERIC
);

SELECT FROM create_hypertable('conditions', by_range('time'));

INSERT INTO conditions
SELECT
    t, d, 10
FROM
    generate_series(
        '2025-02-05 00:00:00+00',
        '2025-03-05 00:00:00+00',
        '1 hour'::interval) AS t,
    generate_series(1,5) AS d;

CREATE MATERIALIZED VIEW conditions_by_day
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT
    time_bucket('1 day', time),
    device_id,
    count(*),
    min(temperature),
    max(temperature),
    avg(temperature),
    sum(temperature)
FROM
    conditions
GROUP BY
    1, 2
WITH NO DATA;

-- Issue an incremental manual refresh using JSONB options:
-- buckets_per_batch => 10. This is the manual equivalent of a
-- policy with the same setting.
SET client_min_messages TO LOG;
CALL refresh_continuous_aggregate(
    'conditions_by_day', NULL, NULL,
    options => '{"buckets_per_batch": 10}'::jsonb);
RESET client_min_messages;

SELECT count(*) FROM conditions_by_day;

CREATE MATERIALIZED VIEW conditions_by_day_atomic_refresh
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT
    time_bucket('1 day', time),
    device_id,
    count(*),
    min(temperature),
    max(temperature),
    avg(temperature),
    sum(temperature)
FROM
    conditions
GROUP BY
    1, 2
WITH NO DATA;

CALL refresh_continuous_aggregate('conditions_by_day_atomic_refresh', NULL, NULL);

SELECT count(*) FROM conditions_by_day;
SELECT count(*) FROM conditions_by_day_atomic_refresh;

-- Should have no differences
SELECT
    count(*) > 0 AS has_diff
FROM
    ((SELECT * FROM conditions_by_day_atomic_refresh ORDER BY 1, 2)
    EXCEPT
    (SELECT * FROM conditions_by_day ORDER BY 1, 2)) AS diff;

-- buckets_per_batch => 0 is NOT incremental: the whole window is materialized
-- in a single pass. Under LOG this shows exactly one delete+insert pair, unlike
-- the multi-batch run above. The TRUNCATE invalidates the whole range so there
-- is data to re-materialize.
TRUNCATE conditions_by_day;
SET client_min_messages TO LOG;
CALL refresh_continuous_aggregate(
    'conditions_by_day', NULL, NULL,
    options => '{"buckets_per_batch": 0}'::jsonb);
RESET client_min_messages;
SELECT count(*) FROM conditions_by_day;

-- The continuous aggregate is now fully materialized with no pending
-- invalidations, so a normal refresh is a no-op (reports up-to-date).
SET client_min_messages TO LOG;
CALL refresh_continuous_aggregate('conditions_by_day', NULL, NULL);
RESET client_min_messages;

-- Assert there really are no pending invalidations
SELECT
    (SELECT count(*)
       FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
      WHERE materialization_id = cagg.mat_hypertable_id
        AND greatest_modified_value >= lowest_modified_value
        AND lowest_modified_value != -9223372036854775808
        AND greatest_modified_value != 9223372036854775807) AS mat_invalidations,
    (SELECT count(*)
       FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
      WHERE hypertable_id = cagg.raw_hypertable_id
        AND greatest_modified_value >= lowest_modified_value) AS ht_invalidations
FROM _timescaledb_catalog.continuous_agg cagg
WHERE cagg.user_view_name = 'conditions_by_day';

-- A forced refresh is incremental but ignores the invalidation logs:
-- it re-materializes the entire window in batches (buckets_per_batch
-- defaults to 10) even though nothing is invalidated.
SET client_min_messages TO LOG;
CALL refresh_continuous_aggregate('conditions_by_day', NULL, NULL, force => true);
RESET client_min_messages;
SELECT count(*) FROM conditions_by_day;

TRUNCATE conditions_by_day;

-- Run with max_batches_per_execution => 2. Manual refresh stops mid-window
-- after processing 2 batches, leaving the rest for subsequent calls.
SET client_min_messages TO LOG;
CALL refresh_continuous_aggregate(
    'conditions_by_day', NULL, NULL,
    options => '{"buckets_per_batch": 10, "max_batches_per_execution": 2}'::jsonb);
RESET client_min_messages;

SELECT count(*) FROM conditions_by_day;
SELECT count(*) FROM conditions_by_day_atomic_refresh;

-- Should have differences (partial materialization)
SELECT
    count(*) > 0 AS has_diff
FROM
    ((SELECT * FROM conditions_by_day_atomic_refresh ORDER BY 1, 2)
    EXCEPT
    (SELECT * FROM conditions_by_day ORDER BY 1, 2)) AS diff;

-- Run a second call (same options) to process more batches.
SET client_min_messages TO LOG;
CALL refresh_continuous_aggregate(
    'conditions_by_day', NULL, NULL,
    options => '{"buckets_per_batch": 10, "max_batches_per_execution": 2}'::jsonb);
RESET client_min_messages;

SELECT count(*) FROM conditions_by_day;
SELECT count(*) FROM conditions_by_day_atomic_refresh;

-- Should have no differences (all batches processed)
SELECT
    count(*) > 0 AS has_diff
FROM
    ((SELECT * FROM conditions_by_day_atomic_refresh ORDER BY 1, 2)
    EXCEPT
    (SELECT * FROM conditions_by_day ORDER BY 1, 2)) AS diff;

-- Set max_batches_per_execution to 10 (effectively unlimited for our window)
-- and insert data into the past so a new set of batches must be processed.
INSERT INTO conditions
SELECT
    t, d, 10
FROM
    generate_series(
        '2020-02-05 00:00:00+00',
        '2020-03-05 00:00:00+00',
        '1 hour'::interval) AS t,
    generate_series(1,5) AS d;

SET client_min_messages TO LOG;
CALL refresh_continuous_aggregate(
    'conditions_by_day', NULL, NULL,
    options => '{"buckets_per_batch": 10, "max_batches_per_execution": 10}'::jsonb);
RESET client_min_messages;

SELECT count(*) FROM conditions_by_day;
SELECT count(*) FROM conditions_by_day_atomic_refresh;

CALL refresh_continuous_aggregate('conditions_by_day_atomic_refresh', NULL, NULL);

SELECT count(*) FROM conditions_by_day;
SELECT count(*) FROM conditions_by_day_atomic_refresh;

-- Should have no differences
SELECT
    count(*) > 0 AS has_diff
FROM
    ((SELECT * FROM conditions_by_day_atomic_refresh ORDER BY 1, 2)
    EXCEPT
    (SELECT * FROM conditions_by_day ORDER BY 1, 2)) AS diff;

-- Invalid configurations should be rejected
\set ON_ERROR_STOP 0
\set VERBOSITY default
CALL refresh_continuous_aggregate(
    'conditions_by_day', NULL, NULL,
    options => '{"max_batches_per_execution": -1}'::jsonb);
CALL refresh_continuous_aggregate(
    'conditions_by_day', NULL, NULL,
    options => '{"buckets_per_batch": -1}'::jsonb);
\set VERBOSITY terse
\set ON_ERROR_STOP 1

-- Truncate all data from the original hypertable.
TRUNCATE conditions;

-- Should fall back to single-batch processing because there's no data
-- to refresh on the source hypertable.
SET client_min_messages TO LOG;
CALL refresh_continuous_aggregate(
    'conditions_by_day', NULL, NULL,
    options => '{"buckets_per_batch": 10}'::jsonb);
RESET client_min_messages;

-- Should return zero rows
SELECT count(*) FROM conditions_by_day;

-- Insert 1 day of data
INSERT INTO conditions
SELECT
    t, d, 10
FROM
    generate_series(
        '2020-02-05 00:00:00+00',
        '2020-02-06 00:00:00+00',
        '1 hour'::interval) AS t,
    generate_series(1,5) AS d;

-- Should fall back to single-batch processing because the refresh size
-- (1 day) is smaller than 10 buckets x 1 day.
SET client_min_messages TO LOG;
CALL refresh_continuous_aggregate(
    'conditions_by_day', NULL, NULL,
    options => '{"buckets_per_batch": 10}'::jsonb);
RESET client_min_messages;

-- Should return 10 rows because the bucket width is `1 day` and we
-- inserted across two boundary timestamps for 5 devices.
SELECT count(*) FROM conditions_by_day;

TRUNCATE conditions_by_day, conditions;

-- Less than 1 day of data (smaller than the bucket width)
INSERT INTO conditions
VALUES ('2020-02-05 00:00:00+00', 1, 10);

-- Should fall back to single-batch processing because the refresh size
-- is smaller than the bucket width.
SET client_min_messages TO LOG;
CALL refresh_continuous_aggregate(
    'conditions_by_day', NULL, NULL,
    options => '{"buckets_per_batch": 10}'::jsonb);
RESET client_min_messages;

-- Should return 1 row
SELECT count(*) FROM conditions_by_day;

-- Re-test with an explicit refresh_newest_first => true (default behavior).
TRUNCATE conditions_by_day, conditions_by_day_atomic_refresh, conditions;

INSERT INTO conditions
SELECT
    t, d, 10
FROM
    generate_series(
        '2025-03-11 00:00:00+00'::timestamptz - INTERVAL '30 days',
        '2025-03-11 00:00:00+00'::timestamptz,
        '1 hour'::interval) AS t,
    generate_series(1,5) AS d;

SET client_min_messages TO LOG;
CALL refresh_continuous_aggregate(
    'conditions_by_day',
    '2025-03-11 00:00:00+00'::timestamptz - INTERVAL '15 days',
    NULL,
    options => '{"buckets_per_batch": 5, "refresh_newest_first": true}'::jsonb);
RESET client_min_messages;

CALL refresh_continuous_aggregate(
    'conditions_by_day_atomic_refresh',
    '2025-03-11 00:00:00+00'::timestamptz - INTERVAL '15 days',
    NULL);

-- Both continuous aggregates should have the same data
SELECT count(*) FROM conditions_by_day;
SELECT count(*) FROM conditions_by_day_atomic_refresh;

-- Should have no differences
SELECT
    count(*) > 0 AS has_diff
FROM
    ((SELECT * FROM conditions_by_day_atomic_refresh ORDER BY 1, 2)
    EXCEPT
    (SELECT * FROM conditions_by_day ORDER BY 1, 2)) AS diff;

-- refresh_newest_first => false (process from oldest to newest)
TRUNCATE conditions_by_day;

SET client_min_messages TO LOG;
CALL refresh_continuous_aggregate(
    'conditions_by_day',
    '2025-03-11 00:00:00+00'::timestamptz - INTERVAL '15 days',
    NULL,
    options => '{"buckets_per_batch": 5, "refresh_newest_first": false}'::jsonb);
RESET client_min_messages;

-- Both continuous aggregates should have the same data
SELECT count(*) FROM conditions_by_day;
SELECT count(*) FROM conditions_by_day_atomic_refresh;

-- Should have no differences
SELECT
    count(*) > 0 AS has_diff
FROM
    ((SELECT * FROM conditions_by_day_atomic_refresh ORDER BY 1, 2)
    EXCEPT
    (SELECT * FROM conditions_by_day ORDER BY 1, 2)) AS diff;

-- Tests with variable-sized bucket (monthly)
TRUNCATE conditions;

INSERT INTO conditions
SELECT
    t, d, 10
FROM
    generate_series(
        '2025-01-01 00:00:00+00',
        '2025-10-08 00:00:00+00',
        '1 hour'::interval) AS t,
    generate_series(1,5) AS d;

CREATE MATERIALIZED VIEW conditions_by_month
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT
    time_bucket('1 month', time),
    device_id,
    count(*),
    min(temperature),
    max(temperature),
    avg(temperature),
    sum(temperature)
FROM
    conditions
GROUP BY
    1, 2
WITH NO DATA;

SET client_min_messages TO LOG;
CALL refresh_continuous_aggregate(
    'conditions_by_month',
    '2025-03-11 00:00:00+00'::timestamptz - INTERVAL '600 days',
    '2025-03-11 00:00:00+00'::timestamptz - INTERVAL '7 days',
    options => '{"refresh_newest_first": false}'::jsonb);
RESET client_min_messages;

SELECT count(*) FROM conditions_by_month;

------------------------------------------------------------------------------------------
-- Test that batched refresh with variable-length buckets doesn't leave remainders
------------------------------------------------------------------------------------------
CREATE TABLE test_data (
    time TIMESTAMPTZ NOT NULL,
    value INT
);

SELECT public.create_hypertable(
        relation => 'test_data',
        time_column_name => 'time',
        chunk_time_interval => interval '1 months'
);
-- Insert initial data
INSERT INTO test_data
SELECT time, 1
FROM generate_series('2024-01-01'::timestamptz, '2024-12-31'::timestamptz, '1 day'::interval) time;

-- Create continuous aggregate with monthly buckets (variable-length)
CREATE MATERIALIZED VIEW batch_test_cagg
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 month'::interval, time) AS bucket,
    count(*) as count
FROM test_data
GROUP BY bucket
WITH NO DATA;

-- Run incremental manual refresh, 1 bucket per batch
SET client_min_messages TO LOG;
CALL refresh_continuous_aggregate(
    'batch_test_cagg', NULL, '2024-12-01'::timestamptz,
    options => '{"buckets_per_batch": 1}'::jsonb);
RESET client_min_messages;

-- Verify that the materialization invalidation log has no entries other than
-- the boundary -/+ infinity rows.
SELECT materialization_id,
       _timescaledb_functions.to_timestamp(lowest_modified_value) as low,
       _timescaledb_functions.to_timestamp(greatest_modified_value) as high
FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
WHERE materialization_id IN
      (SELECT mat_hypertable_id FROM _timescaledb_catalog.continuous_agg
       WHERE user_view_name = 'batch_test_cagg')
  AND lowest_modified_value != -9223372036854775808
  AND greatest_modified_value != 9223372036854775807
ORDER BY low;

-- Running again should be a no-op
SET client_min_messages TO LOG;
CALL refresh_continuous_aggregate(
    'batch_test_cagg', NULL, '2024-12-01'::timestamptz,
    options => '{"buckets_per_batch": 1}'::jsonb);
RESET client_min_messages;

DROP TABLE test_data CASCADE;

------------------------------------------------------------------------------------------
-- Test incremental manual refresh crashing between batches
------------------------------------------------------------------------------------------

-- Rows processed by a batch should be visible immediately after it finishes.
-- Inject an error after batch 1 completes and observe the cagg state.
\c :TEST_DBNAME :ROLE_SUPERUSER
TRUNCATE _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log, _timescaledb_catalog.continuous_aggs_materialization_invalidation_log;
\c :TEST_DBNAME test_cagg_refresh_manual_user
SET timezone TO 'UTC';

TRUNCATE conditions, conditions_by_day, conditions_by_day_atomic_refresh;

INSERT INTO conditions
SELECT
    t, d, 10
FROM
    generate_series(
        '2025-02-05 00:00:00+00',
        '2025-03-05 00:00:00+00',
        '1 hour'::interval) AS t,
    generate_series(1,5) AS d;

-- Crash after batch 1 finishes
SELECT debug_waitpoint_enable('cagg_policy_batch_1_after_refresh');

-- Cagg state before refresh starts
SELECT min(time_bucket), max(time_bucket) FROM conditions_by_day;

SET client_min_messages TO LOG;
\set ON_ERROR_STOP 0
CALL refresh_continuous_aggregate(
    'conditions_by_day', NULL, '1 hour'::interval,
    options => '{"buckets_per_batch": 10}'::jsonb);
\set ON_ERROR_STOP 1
RESET client_min_messages;

-- Rows processed by batch 1 should be materialized
SELECT count(*) AS rows_after_refresh FROM conditions_by_day;
SELECT min(time_bucket), max(time_bucket) FROM conditions_by_day;

-- Registered ranges should be cleaned up after the crash
SELECT count(*) AS registered_ranges FROM _timescaledb_catalog.continuous_aggs_jobs_refresh_ranges;

SELECT debug_waitpoint_release('cagg_policy_batch_1_after_refresh');

-- Process remaining invalidations and verify
CALL refresh_continuous_aggregate(
    'conditions_by_day', NULL, '1 hour'::interval,
    options => '{"buckets_per_batch": 10}'::jsonb);

-- Verify against an atomic refresh
CALL refresh_continuous_aggregate('conditions_by_day_atomic_refresh', NULL, '1 hour'::interval);

SELECT
    count(*) > 0 AS has_diff
FROM
    ((SELECT * FROM conditions_by_day_atomic_refresh ORDER BY 1, 2)
    EXCEPT
    (SELECT * FROM conditions_by_day ORDER BY 1, 2)) AS diff;

------------------------------------------------------------------------------------------
-- #10221: an invalidation whose greatest_modified_value lands EXACTLY on a bucket
-- start must not lose its last bucket when the refresh window is split into batches.
------------------------------------------------------------------------------------------
SET timezone TO 'UTC';

CREATE TABLE boundary_data (
    time    TIMESTAMPTZ NOT NULL,
    device  INTEGER,
    value   FLOAT
);
SELECT FROM create_hypertable('boundary_data', 'time');

-- device 1: mid-bucket rows only, 2025-03-01 .. 2025-03-10
INSERT INTO boundary_data
SELECT time, 1, 1.0
FROM generate_series('2025-03-01 12:00:00+00'::timestamptz,
                     '2025-03-10 12:00:00+00'::timestamptz,
                     '1 day'::interval) AS time;

CREATE MATERIALIZED VIEW boundary_cagg
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 day', time) AS bucket, device, count(*) AS cnt
FROM boundary_data
GROUP BY 1, 2
WITH NO DATA;

-- Full refresh advances the watermark to 2025-03-11
CALL refresh_continuous_aggregate('boundary_cagg', NULL, NULL);

-- device 2: single row EXACTLY at a bucket start below the watermark. The
-- hypertable invalidation entry for it is [2025-03-05, 2025-03-05] -- both
-- bounds precisely on the bucket boundary.
INSERT INTO boundary_data VALUES ('2025-03-05 00:00:00+00', 2, 1.0);

-- Batched refresh over a finite window: bucket [2025-03-05, 2025-03-06) must
-- get its own batch.
CALL refresh_continuous_aggregate(
    'boundary_cagg', '2025-03-01'::timestamptz, '2025-04-01'::timestamptz,
    options => '{"buckets_per_batch": 1}'::jsonb);

-- The 2025-03-05 bucket must be materialized for device 2
SELECT * FROM boundary_cagg WHERE device = 2 ORDER BY bucket;

-- No stranded invalidation may remain
SELECT _timescaledb_functions.to_timestamp(lowest_modified_value)   AS stranded_low,
       _timescaledb_functions.to_timestamp(greatest_modified_value) AS stranded_high
FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
WHERE materialization_id = (SELECT mat_hypertable_id FROM _timescaledb_catalog.continuous_agg
                            WHERE user_view_name = 'boundary_cagg')
  AND lowest_modified_value  > -210866803200000000
  AND greatest_modified_value < 9223372036854775807;

DROP TABLE boundary_data CASCADE;
RESET timezone;

\c :TEST_DBNAME :ROLE_SUPERUSER
REASSIGN OWNED BY test_cagg_refresh_manual_user TO :ROLE_SUPERUSER;
REVOKE ALL ON SCHEMA public FROM test_cagg_refresh_manual_user;

DROP ROLE test_cagg_refresh_manual_user;
