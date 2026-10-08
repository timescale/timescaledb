-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE OR REPLACE FUNCTION ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(timeout INT = -1, mock_start_time INT = 0) RETURNS VOID
AS :MODULE_PATHNAME LANGUAGE C VOLATILE;
CREATE OR REPLACE FUNCTION ts_bgw_params_create() RETURNS VOID
AS :MODULE_PATHNAME LANGUAGE C VOLATILE;
CREATE OR REPLACE FUNCTION ts_bgw_params_destroy() RETURNS VOID
AS :MODULE_PATHNAME LANGUAGE C VOLATILE;
CREATE OR REPLACE FUNCTION ts_bgw_params_reset_time(set_time BIGINT = 0, wait BOOLEAN = false) RETURNS VOID
AS :MODULE_PATHNAME LANGUAGE C VOLATILE;

-- Create a user with specific timezone and mock time
CREATE ROLE test_cagg_refresh_policy_user WITH LOGIN;
ALTER ROLE test_cagg_refresh_policy_user SET timezone TO 'UTC';
ALTER ROLE test_cagg_refresh_policy_user SET timescaledb.current_timestamp_mock TO '2025-03-11 00:00:00+00';
GRANT ALL ON SCHEMA public TO test_cagg_refresh_policy_user;

\c :TEST_DBNAME test_cagg_refresh_policy_user

CREATE TABLE public.bgw_log(
    msg_no INT,
    mock_time BIGINT,
    application_name TEXT,
    msg TEXT
);

CREATE VIEW sorted_bgw_log AS
SELECT
    msg_no,
    mock_time,
    application_name,
    regexp_replace(regexp_replace(msg, '(Wait until|started at|execution time) [0-9]+(\.[0-9]+)?', '\1 (RANDOM)', 'g'), 'background worker "[^"]+"','connection') AS msg
FROM
    bgw_log
ORDER BY
    mock_time,
    application_name COLLATE "C",
    msg_no;

CREATE TABLE public.bgw_dsm_handle_store(
    handle BIGINT
);
INSERT INTO public.bgw_dsm_handle_store VALUES (0);
SELECT ts_bgw_params_create();

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

SELECT
    add_continuous_aggregate_policy(
        'conditions_by_day',
        start_offset => NULL,
        end_offset => NULL,
        schedule_interval => INTERVAL '1 h',
        buckets_per_batch => 10
    ) AS job_id \gset

SELECT
    config
FROM
    timescaledb_information.jobs
WHERE
    job_id = :'job_id' \gset

SELECT ts_bgw_params_reset_time(0, true);
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25);
SELECT * FROM sorted_bgw_log;
SELECT * FROM _timescaledb_catalog.continuous_aggs_materialization_ranges;

CREATE MATERIALIZED VIEW conditions_by_day_manual_refresh
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

CALL refresh_continuous_aggregate('conditions_by_day_manual_refresh', NULL, NULL);

SELECT count(*) FROM conditions_by_day;
SELECT count(*) FROM conditions_by_day_manual_refresh;

-- Should have no differences
SELECT
    count(*) > 0 AS has_diff
FROM
    ((SELECT * FROM conditions_by_day_manual_refresh ORDER BY 1, 2)
    EXCEPT
    (SELECT * FROM conditions_by_day ORDER BY 1, 2)) AS diff;

TRUNCATE bgw_log, conditions_by_day;

SELECT
    config
FROM
    alter_job(
        :'job_id',
        config => jsonb_set(:'config', '{max_batches_per_execution}', '2')
    );

-- advance time by 1h so that job runs one more time
SELECT ts_bgw_params_reset_time(extract(epoch from interval '1 hour')::bigint * 1000000, true);

SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25);
SELECT * FROM sorted_bgw_log;
SELECT * FROM _timescaledb_catalog.continuous_aggs_materialization_ranges;

SELECT count(*) FROM conditions_by_day;
SELECT count(*) FROM conditions_by_day_manual_refresh;

-- Should have differences
SELECT
    count(*) > 0 AS has_diff
FROM
    ((SELECT * FROM conditions_by_day_manual_refresh ORDER BY 1, 2)
    EXCEPT
    (SELECT * FROM conditions_by_day ORDER BY 1, 2)) AS diff;

-- advance time by 2h so that job runs one more time
SELECT ts_bgw_params_reset_time(extract(epoch from interval '2 hour')::bigint * 1000000, true);

SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25);
SELECT * FROM sorted_bgw_log;
SELECT * FROM _timescaledb_catalog.continuous_aggs_materialization_ranges;

-- Should have no differences
SELECT
    count(*) > 0 AS has_diff
FROM
    ((SELECT * FROM conditions_by_day_manual_refresh ORDER BY 1, 2)
    EXCEPT
    (SELECT * FROM conditions_by_day ORDER BY 1, 2)) AS diff;

-- Set max_batches_per_execution to 10
SELECT
    config
FROM
    alter_job(
        :'job_id',
        config => jsonb_set(:'config', '{max_batches_per_execution}', '10')
    );

TRUNCATE bgw_log;

-- Insert data into the past
INSERT INTO conditions
SELECT
    t, d, 10
FROM
    generate_series(
        '2020-02-05 00:00:00+00',
        '2020-03-05 00:00:00+00',
        '1 hour'::interval) AS t,
    generate_series(1,5) AS d;

-- advance time by 3h so that job runs one more time
SELECT ts_bgw_params_reset_time(extract(epoch from interval '3 hour')::bigint * 1000000, true);

-- Should process all three batches in the past
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25);
SELECT * FROM sorted_bgw_log;
SELECT * FROM _timescaledb_catalog.continuous_aggs_materialization_ranges;

SELECT count(*) FROM conditions_by_day;
SELECT count(*) FROM conditions_by_day_manual_refresh;

CALL refresh_continuous_aggregate('conditions_by_day_manual_refresh', NULL, NULL);

SELECT count(*) FROM conditions_by_day;
SELECT count(*) FROM conditions_by_day_manual_refresh;

-- Should have no differences
SELECT
    count(*) > 0 AS has_diff
FROM
    ((SELECT * FROM conditions_by_day_manual_refresh ORDER BY 1, 2)
    EXCEPT
    (SELECT * FROM conditions_by_day ORDER BY 1, 2)) AS diff;

-- Check invalid configurations
\set ON_ERROR_STOP 0
\set VERBOSITY default
SELECT
    config
FROM
    alter_job(
        :'job_id',
        config => jsonb_set(:'config', '{max_batches_per_execution}', '-1')
    );
SELECT
    config
FROM
    alter_job(
        :'job_id',
        config => jsonb_set(:'config', '{buckets_per_batch}', '-1')
    );
\set VERBOSITY terse
\set ON_ERROR_STOP 1

-- Truncate all data from the original hypertable
TRUNCATE bgw_log, conditions;

-- advance time by 4h so that job runs one more time
SELECT ts_bgw_params_reset_time(extract(epoch from interval '4 hour')::bigint * 1000000, true);

-- Should fallback to single batch processing because there's no data to be refreshed on the original hypertable
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25);
SELECT * FROM sorted_bgw_log;
SELECT * FROM _timescaledb_catalog.continuous_aggs_materialization_ranges;

-- Should return zero rows
SELECT count(*) FROM conditions_by_day;

-- 1 day of data
INSERT INTO conditions
SELECT
    t, d, 10
FROM
    generate_series(
        '2020-02-05 00:00:00+00',
        '2020-02-06 00:00:00+00',
        '1 hour'::interval) AS t,
    generate_series(1,5) AS d;

TRUNCATE bgw_log;

-- advance time by 5h so that job runs one more time
SELECT ts_bgw_params_reset_time(extract(epoch from interval '5 hour')::bigint * 1000000, true);

-- Should fallback to single batch processing because the refresh size is too small
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25);
SELECT * FROM sorted_bgw_log;
SELECT * FROM _timescaledb_catalog.continuous_aggs_materialization_ranges;

-- Should return 10 rows because the bucket width is `1 day` and buckets per batch is `10`
SELECT count(*) FROM conditions_by_day;

TRUNCATE conditions_by_day, conditions, bgw_log;

-- Less than 1 day of data (smaller than the bucket width)
INSERT INTO conditions
VALUES ('2020-02-05 00:00:00+00', 1, 10);

-- advance time by 6h so that job runs one more time
SELECT ts_bgw_params_reset_time(extract(epoch from interval '6 hour')::bigint * 1000000, true);

-- Should fallback to single batch processing because the refresh size is too small
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25);
SELECT * FROM sorted_bgw_log;
SELECT * FROM _timescaledb_catalog.continuous_aggs_materialization_ranges;

-- Should return 1 row
SELECT count(*) FROM conditions_by_day;

SELECT delete_job(:job_id);

SELECT
    add_continuous_aggregate_policy(
        'conditions_by_day',
        start_offset => INTERVAL '15 days',
        end_offset => NULL,
        schedule_interval => INTERVAL '1 h',
        buckets_per_batch => 5,
        refresh_newest_first => true -- explicitly set to true to test the default behavior
    ) AS job_id \gset

SELECT
    add_continuous_aggregate_policy(
        'conditions_by_day_manual_refresh',
        start_offset => INTERVAL '15 days',
        end_offset => NULL,
        schedule_interval => INTERVAL '1 h',
        buckets_per_batch => 0 -- 0 means no batching, so it will refresh all buckets in one go
    ) AS job_id_manual \gset

TRUNCATE bgw_log, conditions_by_day, conditions_by_day_manual_refresh, conditions;

INSERT INTO conditions
SELECT
    t, d, 10
FROM
    generate_series(
        '2025-03-11 00:00:00+00'::timestamptz - INTERVAL '30 days',
        '2025-03-11 00:00:00+00'::timestamptz,
        '1 hour'::interval) AS t,
    generate_series(1,5) AS d;

SELECT ts_bgw_params_reset_time(0, true);
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25);
SELECT * FROM sorted_bgw_log;
SELECT * FROM _timescaledb_catalog.continuous_aggs_materialization_ranges;

-- Both continuous aggregates should have the same data
SELECT count(*) FROM conditions_by_day;
SELECT count(*) FROM conditions_by_day_manual_refresh;

-- Should have no differences
SELECT
    count(*) > 0 AS has_diff
FROM
    ((SELECT * FROM conditions_by_day_manual_refresh ORDER BY 1, 2)
    EXCEPT
    (SELECT * FROM conditions_by_day ORDER BY 1, 2)) AS diff;

-- Testing with explicit refresh_newest_first = false (from oldest to newest)
SELECT delete_job(:job_id);
SELECT delete_job(:job_id_manual);

SELECT
    add_continuous_aggregate_policy(
        'conditions_by_day',
        start_offset => INTERVAL '15 days',
        end_offset => NULL,
        schedule_interval => INTERVAL '1 h',
        buckets_per_batch => 5,
        refresh_newest_first => false
    ) AS job_id \gset

SELECT
    config
FROM
    timescaledb_information.jobs
WHERE
    job_id = :'job_id';

TRUNCATE bgw_log, conditions_by_day;

SELECT ts_bgw_params_reset_time(0, true);
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25);
SELECT * FROM sorted_bgw_log;
SELECT * FROM _timescaledb_catalog.continuous_aggs_materialization_ranges;

-- Both continuous aggregates should have the same data
SELECT count(*) FROM conditions_by_day;
SELECT count(*) FROM conditions_by_day_manual_refresh;

-- Should have no differences
SELECT
    count(*) > 0 AS has_diff
FROM
    ((SELECT * FROM conditions_by_day_manual_refresh ORDER BY 1, 2)
    EXCEPT
    (SELECT * FROM conditions_by_day ORDER BY 1, 2)) AS diff;


-- Tests with Variable sized bucket
SELECT delete_job(:job_id);
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

SELECT
    add_continuous_aggregate_policy(
        'conditions_by_month',
        start_offset => INTERVAL '600 days',
        end_offset => INTERVAL '7 days',
        schedule_interval => INTERVAL '1 day',
        refresh_newest_first => false
    ) AS job_id \gset

SELECT
    config
FROM
    timescaledb_information.jobs
WHERE
    job_id = :'job_id';

TRUNCATE bgw_log, conditions_by_day;

SELECT ts_bgw_params_reset_time(0, true);
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25);
SELECT * FROM sorted_bgw_log;
SELECT * FROM _timescaledb_catalog.continuous_aggs_materialization_ranges;

SELECT delete_job(:job_id);
------------------------------------------------------------------------------------------
--Test that batched refresh with variable-length buckets doesn't leave remainders
-------------------------------------------------------------------------------------------
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

-- Create continuous aggregate with monthly buckets and timezone (variable-length buckets)
CREATE MATERIALIZED VIEW batch_test_cagg
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 month'::interval, time) AS bucket,
    count(*) as count
FROM test_data
GROUP BY bucket
WITH NO DATA;


-- Add a policy to enable batched refresh (batch size is 30 days by default for monthly buckets)
SELECT add_continuous_aggregate_policy('batch_test_cagg',
    start_offset =>null,
    end_offset => INTERVAL '1 month',
    schedule_interval => INTERVAL '1 hour',
    buckets_per_batch => 1

) AS job_id \gset

-- Run the policy job - this uses batched processing, 1 bucket per batch
TRUNCATE bgw_log;
SELECT ts_bgw_params_reset_time(0, true);
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25);
-- Verify that invalidation log has no entries other than the left and right ends with -/+ infinity
SELECT materialization_id,
       _timescaledb_functions.to_timestamp(lowest_modified_value) as low,
       _timescaledb_functions.to_timestamp(greatest_modified_value) as high
FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
WHERE materialization_id IN
      (SELECT mat_hypertable_id FROM _timescaledb_catalog.continuous_agg
       WHERE user_view_name = 'batch_test_cagg')
  AND lowest_modified_value != -9223372036854775808 --  -infinity
  AND greatest_modified_value != 9223372036854775807 -- +infinity
ORDER BY low;

--verify that there is no duplicate/overlapping refreshes.
--Each batch should cover exactly 1 month (buckets_per_batch=1).

SELECT * FROM sorted_bgw_log;

--now run the refresh again, should not do anything
TRUNCATE bgw_log;
SELECT ts_bgw_params_reset_time(extract(epoch from interval '1 hour')::bigint * 1000000, true);
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25);
SELECT * FROM sorted_bgw_log;

--clean up
DROP TABLE test_data CASCADE;

------------------------------------------------------------------------------------------
-- Test incremental refresh policy crashing between batches
------------------------------------------------------------------------------------------

-- Rows processed by a batch should be visible immediately after it finishes
-- We test this by injecting an error after the first batch completes and observing the CAgg state
\c :TEST_DBNAME :ROLE_SUPERUSER
TRUNCATE _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log, _timescaledb_catalog.continuous_aggs_materialization_invalidation_log;
\c :TEST_DBNAME test_cagg_refresh_policy_user

TRUNCATE bgw_log, conditions, conditions_by_day, conditions_by_day_manual_refresh;

SELECT delete_job(job_id) FROM timescaledb_information.jobs WHERE hypertable_name = 'conditions_by_day';

INSERT INTO conditions
SELECT
    t, d, 10
FROM
    generate_series(
        '2025-02-05 00:00:00+00',
        '2025-03-05 00:00:00+00',
        '1 hour'::interval) AS t,
    generate_series(1,5) AS d;

SELECT
    add_continuous_aggregate_policy(
        'conditions_by_day',
        start_offset => NULL,
        end_offset => '1 hour',
        schedule_interval => INTERVAL '1 h',
        buckets_per_batch => 10
    ) AS crash_job_id2 \gset

-- Crash after batch 1 finishes refreshing
SELECT debug_waitpoint_enable('cagg_policy_batch_1_after_refresh');

-- CAgg state before refresh starts
SELECT min(time_bucket), max(time_bucket) FROM conditions_by_day;

SET client_min_messages TO LOG;
\set ON_ERROR_STOP 0
CALL run_job(:crash_job_id2);
\set ON_ERROR_STOP 1
RESET client_min_messages;

-- Rows processed by batch 1 should be materialized
SELECT count(*) AS rows_after_refresh FROM conditions_by_day;
SELECT min(time_bucket), max(time_bucket) FROM conditions_by_day;

-- Verify registered ranges are cleaned up after the crash
SELECT count(*) AS registered_ranges FROM _timescaledb_catalog.continuous_aggs_jobs_refresh_ranges;

SELECT debug_waitpoint_release('cagg_policy_batch_1_after_refresh');

-- Process reminaing invalidations and verify
CALL run_job(:crash_job_id2);

-- Verify against manual refresh
CALL refresh_continuous_aggregate('conditions_by_day_manual_refresh', NULL, '1 hour'::interval);

SELECT
    count(*) > 0 AS has_diff
FROM
    ((SELECT * FROM conditions_by_day_manual_refresh ORDER BY 1, 2)
    EXCEPT
    (SELECT * FROM conditions_by_day ORDER BY 1, 2)) AS diff;

------------------------------------------------------------------------------------------
-- Test variable-width bucket batching: demonstrates chunk gap skip and invalidation
-- gap skip with non-null start/end offsets (no null-restoration).
-- chunk_time_interval=25 days is intentionally not aligned with monthly buckets.
--
-- Chunk boundaries are fixed 25-day windows anchored to the Unix epoch
-- (Jan 1 1970), so they fall at arbitrary calendar dates. The four data points land in:
--   Mar 15 -> chunk [Feb 22, Mar 18): spans Feb and Mar bucket boundaries
--   May 15 -> chunk [May  7, Jun  1): entirely within May
--   Jun 10 -> chunk [Jun  1, Jun 26): entirely within June
--   Oct 15 -> chunk [Oct  4, Oct 29): entirely within October
--
-- June is explicitly refreshed before the policy runs, consuming its invalidation.
-- The sentinel [-inf, +inf] is cut into two residuals in mat_inval_log:
--   left  [-inf,     May 31 23:59:59.999999]  (covers Mar and May)
--   right [Jul 1,   +inf]                      (covers Oct and later)
-- -> gap from Jun 1 to Jun 30 means June has no pending invalidation.
--
-- Expected batch behaviour (refresh_newest_first=false, buckets_per_batch=1,
-- start_offset='10 years', end_offset='1 month'):
--   Feb: batch [Feb 1, Mar 1) -- EMPTY: the Feb 22 chunk touches the Feb bucket but
--        no data falls in February; illustrates a chunk intersecting a bucket boundary
--   Mar: batch generated  (Mar chunk overlaps Mar bucket + left-residual inval)
--   Apr: SKIPPED          (chunk gap: no chunks in April)
--   May: batch generated  (May chunk + left-residual inval)
--   Jun: SKIPPED          (invalidation gap: inval was consumed by explicit refresh)
--   Jul-Sep: SKIPPED      (chunk gap)
--   Oct: batch generated  (Oct chunk + right-residual inval)
-- No null-restoration: start_offset and end_offset are both non-null.
-- Final batches: [Feb 1, Mar 1),  [Mar 1, Apr 1),  [May 1, Jun 1),  [Oct 1, Nov 1).
------------------------------------------------------------------------------------------
-- Monthly bucket boundaries are at midnight UTC; set timezone to avoid PST-shift issues.
SET timezone = 'UTC';
CREATE TABLE sparse_data (
    time TIMESTAMPTZ NOT NULL,
    value INT
);
SELECT FROM create_hypertable('sparse_data', by_range('time', INTERVAL '25 days'));

INSERT INTO sparse_data VALUES
    ('2024-03-15 00:00:00+00', 3),   -- lands in chunk [Feb 22, Mar 18)
    ('2024-05-15 00:00:00+00', 5),   -- lands in chunk [May  7, Jun  1)
    ('2024-06-10 00:00:00+00', 6),   -- lands in chunk [Jun  1, Jun 26)
    ('2024-10-15 00:00:00+00', 10);  -- lands in chunk [Oct  4, Oct 29)

CREATE MATERIALIZED VIEW sparse_cagg
WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
SELECT time_bucket('1 month'::interval, time) AS bucket, count(*) AS cnt
FROM sparse_data
GROUP BY bucket
WITH NO DATA;

-- Consume June's invalidation so the June bucket has no pending invalidation.
CALL refresh_continuous_aggregate('sparse_cagg',
    '2024-06-01'::timestamptz, '2024-07-01'::timestamptz);

-- Validate dimension slices: four chunks showing the epoch-aligned boundaries.
SELECT
    _timescaledb_functions.to_timestamp(ds.range_start) AS range_start,
    _timescaledb_functions.to_timestamp(ds.range_end)   AS range_end
FROM _timescaledb_catalog.dimension_slice ds
JOIN _timescaledb_catalog.dimension d ON d.id = ds.dimension_id
JOIN _timescaledb_catalog.hypertable h ON h.id = d.hypertable_id
WHERE h.table_name = 'sparse_data'
ORDER BY ds.range_start;

-- Validate invalidation log: two residuals with a gap over June.
SELECT
    _timescaledb_functions.to_timestamp(lowest_modified_value)   AS lowest,
    _timescaledb_functions.to_timestamp(greatest_modified_value) AS greatest
FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
WHERE materialization_id = (
    SELECT mat_hypertable_id FROM _timescaledb_catalog.continuous_agg
    WHERE user_view_name = 'sparse_cagg'
)
ORDER BY lowest_modified_value;

SELECT add_continuous_aggregate_policy('sparse_cagg',
    start_offset => INTERVAL '10 years',
    end_offset => INTERVAL '1 month',
    schedule_interval => INTERVAL '1 hour',
    buckets_per_batch => 1,
    refresh_newest_first => false
) AS sparse_job_id \gset

-- Four batches expected: [Feb 1, Mar 1) empty, [Mar 1, Apr 1), [May 1, Jun 1), [Oct 1, Nov 1).
-- Apr/Jul/Aug/Sep skipped (chunk gap), Jun skipped (invalidation gap).
SET client_min_messages TO DEBUG1;
CALL run_job(:sparse_job_id);
RESET client_min_messages;

------------------------------------------------------------------------------------------
-- Similar test using a fixed 10-day bucket CAgg on the same sparse_data table.
-- time_bucket('10 days', ...) uses Jan 3 2000 as origin. Relevant 2024 boundaries:
--   Feb 16, Feb 26, Mar 7, Mar 17, Mar 27, ..., May 6, May 16, May 26,
--   Jun 5, Jun 15, Jun 25, Jul 5, ..., Oct 3, Oct 13, Oct 23, Nov 2, ...
--
-- Refresh [Jun 5, Jun 25) consumes two 10-day June buckets, leaving:
--   left  [-inf,     Jun  4 23:59:59.999999]
--   right [Jun 25,  +inf]
--
-- start_offset='10 years': window start is far in the past; jump logic advances
-- directly to cagg_current_bucket_start(Feb 22) = Feb 16, the bucket containing
-- the earliest chunk. No null-restoration (start_offset non-null).
-- Gaps are skipped by jumping to cagg_current_bucket_start(jump_target), the same
-- as for variable-width buckets. The chunk starts May 7 and Oct 4 fall mid-bucket:
--   cagg_current_bucket_start(May 7) = May 6  (bucket [May  6, May 16))
--   cagg_current_bucket_start(Oct 4) = Oct 3  (bucket [Oct  3, Oct 13))
-- so those batches start one day before the corresponding chunk.
--
-- Expected batches (buckets_per_batch=1, refresh_newest_first=false):
--   Feb: [Feb 16, Feb 26)  EMPTY (no Feb data; illustrates chunk spanning bucket boundary)
--   Mar (3 buckets): [Feb 26, Mar 7), [Mar 7, Mar 17), [Mar 17, Mar 27)
--   Apr: SKIPPED  (chunk gap: no chunks in April)
--   May (3 buckets): [May 6, May 16), [May 16, May 26), [May 26, Jun 5)
--                    May 6 = bucket start of May 7 (first chunk boundary)
--   Jun 5-Jun 25: SKIPPED  (invalidation gap: consumed by explicit refresh)
--   Jun 25: batch [Jun 25, Jul 5)  (last Jun chunk + right residual)
--   Jul-Sep: SKIPPED  (chunk gap)
--   Oct (3 buckets): [Oct 3, Oct 13), [Oct 13, Oct 23), [Oct 23, Nov 2)
--                    Oct 3 = bucket start of Oct 4 (first chunk boundary)
------------------------------------------------------------------------------------------
CREATE MATERIALIZED VIEW sparse_cagg_fixed
WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
SELECT time_bucket('10 days'::interval, time) AS bucket, count(*) AS cnt
FROM sparse_data
GROUP BY bucket
WITH NO DATA;

-- Consume [Jun 5, Jun 25) to create an invalidation gap for those two June buckets.
CALL refresh_continuous_aggregate('sparse_cagg_fixed',
    '2024-06-05'::timestamptz, '2024-06-25'::timestamptz);

-- Validate invalidation log: two residuals with a gap over [Jun 5, Jun 25).
SELECT
    _timescaledb_functions.to_timestamp(lowest_modified_value)   AS lowest,
    _timescaledb_functions.to_timestamp(greatest_modified_value) AS greatest
FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
WHERE materialization_id = (
    SELECT mat_hypertable_id FROM _timescaledb_catalog.continuous_agg
    WHERE user_view_name = 'sparse_cagg_fixed'
)
ORDER BY lowest_modified_value;

SELECT add_continuous_aggregate_policy('sparse_cagg_fixed',
    start_offset => INTERVAL '10 years',
    end_offset => INTERVAL '1 month',
    schedule_interval => INTERVAL '1 hour',
    buckets_per_batch => 1,
    refresh_newest_first => false
) AS fixed_job_id \gset

-- Eleven batches expected: 1 empty in Feb, 3 in Mar, 3 in May, 1 at Jun 25, 3 in Oct.
SET client_min_messages TO DEBUG1;
CALL run_job(:fixed_job_id);
RESET client_min_messages;

------------------------------------------------------------------------------------------
-- Same sparse_data as above but with buckets_per_batch=3 to show how the algorithm
-- groups consecutive buckets and absorbs inval gaps into batch windows.
--
-- Pre-refresh [Jun 1, Jul 1) creates the same residuals as for sparse_cagg:
--   left  [-inf,     May 31 23:59:59.999999]
--   right [Jul  1,  +inf]
--
-- With buckets_per_batch=3 the algorithm advances exactly 3 bucket widths per batch
-- from the starting position; gaps within a batch window are not spliced out.
--
-- Batch analysis (oldest-first):
--   1. jump to Feb 1 (cagg_current_bucket_start of first chunk Feb 22),
--      emit 3 months: [Feb 1, May 1)
--        Feb: empty (no data in Feb bucket -- chunk [Feb 22, Mar 18) has only Mar data)
--        Mar: 1 row (Mar 15)
--        Apr: chunk gap (absorbed into batch, nothing materialized)
--   2. cur = May 1; cagg_current_bucket_start(May 7) = May 1 = cur, no jump;
--      emit 3 months: [May 1, Aug 1)
--      The batch window intersects two invalidation ranges, so two sub-refreshes run:
--        [May 1, Jun 1): 1 row (May 15), from left residual [-inf, May 31]
--        [Jun 1, Jul 1): SKIPPED -- no invalidation covers Jun (consumed by pre-refresh);
--                                   the Jun row from pre-refresh is preserved unchanged
--        [Jul 1, Aug 1): 0 rows, from right residual [Jul 1, +inf); no chunk data in Jul
--   3. cur = Aug 1; jump: Max(Oct 4, Jul 1) -> cagg_current_bucket_start(Oct 4) = Oct 1;
--      emit 3 months: [Oct 1, Jan 1 2025)
--        Oct: 1 row (Oct 15); Nov-Dec: chunk gaps (absorbed into batch)
--
-- Three batches produced (vs four with buckets_per_batch=1).
------------------------------------------------------------------------------------------
CREATE MATERIALIZED VIEW sparse_cagg_bp3
WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
SELECT time_bucket('1 month'::interval, time) AS bucket, count(*) AS cnt
FROM sparse_data
GROUP BY bucket
WITH NO DATA;

-- Pre-refresh June to consume June's invalidation, creating the same inval gap.
CALL refresh_continuous_aggregate('sparse_cagg_bp3',
    '2024-06-01'::timestamptz, '2024-07-01'::timestamptz);

SELECT add_continuous_aggregate_policy('sparse_cagg_bp3',
    start_offset => INTERVAL '10 years',
    end_offset => INTERVAL '1 month',
    schedule_interval => INTERVAL '1 hour',
    buckets_per_batch => 3,
    refresh_newest_first => false
) AS sparse_bp3_job_id \gset

-- Three batches: [Feb 1, May 1), [May 1, Aug 1), [Oct 1, Jan 1 2025).
-- Apr/Jul gaps and the Jun inval gap are absorbed inside batch windows.
SET client_min_messages TO DEBUG1;
CALL run_job(:sparse_bp3_job_id);
RESET client_min_messages;

------------------------------------------------------------------------------------------
-- Test correct null-offset handling for BOTH start_offset=NULL and end_offset=NULL.
--
-- start_offset=NULL: window start capped to cagg_current_bucket_start(Feb 22) = Feb 1.
--   Feb 1 is already a bucket boundary -> inscribing is a no-op.
--   -> Feb bucket [Feb 1, Mar 1) gets its own dedicated batch.
--
-- end_offset=NULL: window end capped to cagg_next_bucket_start(Oct 29 - 1) = Nov 1.
--   Nov 1 is already a bucket boundary -> inscribing is a no-op.
--   -> Oct chunk [Oct 4, Oct 29) has range_start=Oct 4 < Nov 1, so it IS
--      included in the dimension-slice scan; Oct gets a dedicated batch.
--
-- Inval log after refreshing June:
--   left  [-inf,     May 31 23:59:59.999999]
--   right [Jul 1,   +inf]
--
-- Merge scan over window [Feb 1, Nov 1):
--   Feb: batch generated  (chunk [Feb 22, Mar 18) overlaps Feb bucket, left-residual inval)
--   Mar: batch generated  (same chunk, left-residual inval covers Mar 15)
--   Apr: SKIPPED          (chunk gap)
--   May: batch generated  (chunk [May 7, Jun 1), left-residual inval)
--   Jun: SKIPPED          (invalidation gap -- June already refreshed)
--   Jul-Sep: SKIPPED      (chunk gap)
--   Oct: batch generated  (chunk [Oct 4, Oct 29), right-residual inval [Jul 1, +inf))
--
-- Null-restoration (list_length=4 >= 2):
--   first batch (Feb) start_isnull -> [-inf, Mar 1)   inserts 1 row: Feb 25
--   last  batch (Oct) end_isnull   -> [Oct 1, +inf)   inserts 1 row: Oct 15
-- Final batches: [-inf, Mar 1),  [Mar 1, Apr 1),  [May 1, Jun 1),  [Oct 1, +inf).
------------------------------------------------------------------------------------------
CREATE TABLE end_null_data (
    time TIMESTAMPTZ NOT NULL,
    value INT
);
SELECT FROM create_hypertable('end_null_data', by_range('time', INTERVAL '25 days'));

-- Same chunk layout as sparse_data, with an extra Feb 25 row (bucket Feb 1) to show
-- that the Feb bucket gets its own dedicated first batch: null-restored to [-inf, Mar 1),
-- inserting Feb 25 separately from the Mar 15 row in the following [Mar 1, Apr 1) batch.
INSERT INTO end_null_data VALUES
    ('2024-02-25 00:00:00+00', 2),   -- lands in chunk [Feb 22, Mar 18), bucket Feb 1
    ('2024-03-15 00:00:00+00', 3),   -- lands in chunk [Feb 22, Mar 18), bucket Mar 1
    ('2024-05-15 00:00:00+00', 5),   -- lands in chunk [May  7, Jun  1)
    ('2024-06-10 00:00:00+00', 6),   -- lands in chunk [Jun  1, Jun 26)
    ('2024-10-15 00:00:00+00', 10);  -- lands in chunk [Oct  4, Oct 29)

CREATE MATERIALIZED VIEW end_null_cagg
WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
SELECT time_bucket('1 month'::interval, time) AS bucket, count(*) AS cnt
FROM end_null_data
GROUP BY bucket
WITH NO DATA;

-- Consume June's invalidation to create an inval gap over June.
CALL refresh_continuous_aggregate('end_null_cagg',
    '2024-06-01'::timestamptz, '2024-07-01'::timestamptz);

-- Validate invalidation log: two residuals with a gap over June.
SELECT
    _timescaledb_functions.to_timestamp(lowest_modified_value)   AS lowest,
    _timescaledb_functions.to_timestamp(greatest_modified_value) AS greatest
FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
WHERE materialization_id = (
    SELECT mat_hypertable_id FROM _timescaledb_catalog.continuous_agg
    WHERE user_view_name = 'end_null_cagg'
)
ORDER BY lowest_modified_value;

SELECT add_continuous_aggregate_policy('end_null_cagg',
    start_offset => NULL,
    end_offset => NULL,
    schedule_interval => INTERVAL '1 hour',
    buckets_per_batch => 1,
    refresh_newest_first => false
) AS end_null_job_id \gset

-- Four batches expected: [-inf, Mar 1), [Mar 1, Apr 1), [May 1, Jun 1), [Oct 1, +inf).
-- Feb batch is the null-restored first batch (1 row: Feb 25).
-- Oct batch is the null-restored last batch (1 row: Oct 15); Oct chunk now included
-- because effective window end is Nov 1 (not Oct 1 as under the old inscribing bug).
SET client_min_messages TO DEBUG1;
CALL run_job(:end_null_job_id);
RESET client_min_messages;

DROP TABLE end_null_data CASCADE;

-- Test: null end_offset — incremental and single-pass refresh set the same threshold.
--
-- A 3-day bucket is used so bucket boundaries are clearly distinct from both the
-- chunk boundaries and the raw data timestamp, making any mismatch obvious.
--
-- Setup (1-month chunk interval ≈ 30 days from Unix epoch):
--   data point    : 2024-01-05 12:00 UTC  (noon, not on any bucket boundary)
--   chunk range   : [2023-12-19 00:00, 2024-01-18 00:00) UTC
--   data bucket   : time_bucket('3 days', data) = 2024-01-03 → [Jan 03, Jan 06)
--   threshold set by 1-pass refresh: cagg_next_bucket_start(data) = 2024-01-06 00:00 UTC
--
-- Before the fix the split function capped the window at chunk_end (Jan 18 UTC)
-- instead of max-data's next bucket (Jan 06).  With buckets_per_batch=1 (batch_size
-- = 3 days), that produces ten 3-day batches over the 30-day chunk:
--   [Dec 19, Dec 22) ... [Jan 03, Jan 06) ... [Jan 15, Jan 18) → [Jan 15, INF)
-- Newest-first processing:
--   batch [Jan 15, INF): max_refresh → threshold = Jan 06 (from max data)
--   batch [Jan 12, Jan 15): Jan 15 > Jan 06 → threshold advances to Jan 15
-- The remaining older batches don't advance it further, so the final threshold is
-- Jan 15 — 9 days past the value set by the single-pass refresh.
--
-- After the fix the window is capped at Jan 06, producing six 3-day batches:
--   [Dec 19, Dec 22)  [Dec 22, Dec 25)  [Dec 25, Dec 28)
--   [Dec 28, Dec 31)  [Dec 31, Jan 03)  [Jan 03, Jan 06) → [Jan 03, INF)
-- Newest-first processing:
--   batch [Jan 03, INF): max_refresh → threshold = Jan 06 (from max data)
--   batch [Dec 31, Jan 03): Jan 03 < Jan 06 → no change  ← FIXED
-- Final threshold = Jan 06, matching single-pass exactly.
--
-- Two separate hypertables (identical data) keep the thresholds independent so the
-- run order cannot mask a discrepancy.
CREATE TABLE threshold_sp_data (time TIMESTAMPTZ NOT NULL, val INT);
SELECT FROM create_hypertable('threshold_sp_data', by_range('time', INTERVAL '1 month'));

CREATE TABLE threshold_inc_data (time TIMESTAMPTZ NOT NULL, val INT);
SELECT FROM create_hypertable('threshold_inc_data', by_range('time', INTERVAL '1 month'));

INSERT INTO threshold_sp_data  VALUES ('2024-01-05 12:00:00+00', 1);
INSERT INTO threshold_inc_data VALUES ('2024-01-05 12:00:00+00', 1);

CREATE MATERIALIZED VIEW threshold_sp_cagg
WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
SELECT time_bucket('3 days', time) AS bucket, count(*) AS cnt
FROM threshold_sp_data GROUP BY 1 WITH NO DATA;

CREATE MATERIALIZED VIEW threshold_inc_cagg
WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
SELECT time_bucket('3 days', time) AS bucket, count(*) AS cnt
FROM threshold_inc_data GROUP BY 1 WITH NO DATA;

SELECT add_continuous_aggregate_policy('threshold_sp_cagg',
    start_offset => NULL, end_offset => NULL,
    schedule_interval => INTERVAL '1 hour',
    buckets_per_batch => 0   -- single-pass
) AS sp_job \gset

SELECT add_continuous_aggregate_policy('threshold_inc_cagg',
    start_offset => NULL, end_offset => NULL,
    schedule_interval => INTERVAL '1 hour',
    buckets_per_batch => 1   -- incremental: one 3-day batch at a time
) AS inc_job \gset

CALL run_job(:sp_job);
CALL run_job(:inc_job);

-- Both must equal 2024-01-06 00:00:00+00 = cagg_next_bucket_start(MAX(data) = Jan 05 12:00).
-- Before the fix incremental_threshold was 2024-01-15 00:00:00+00 (9 days too far).
SELECT
    _timescaledb_functions.to_timestamp(t1.watermark) AS singlepass_threshold,
    _timescaledb_functions.to_timestamp(t2.watermark) AS incremental_threshold
FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold t1
JOIN _timescaledb_catalog.continuous_aggs_invalidation_threshold t2
  ON true
WHERE t1.hypertable_id = (
    SELECT raw_hypertable_id FROM _timescaledb_catalog.continuous_agg
    WHERE user_view_name = 'threshold_sp_cagg')
  AND t2.hypertable_id = (
    SELECT raw_hypertable_id FROM _timescaledb_catalog.continuous_agg
    WHERE user_view_name = 'threshold_inc_cagg');

DROP TABLE threshold_sp_data CASCADE;

-- Test: null end_offset on an empty hypertable hits the maxval_isnull branch and
-- falls back to single-pass without error.  The DEBUG1 message confirms the path.
TRUNCATE threshold_inc_data;
SET client_min_messages TO DEBUG1;
CALL run_job(:inc_job);
RESET client_min_messages;

DROP TABLE threshold_inc_data CASCADE;

DROP TABLE sparse_data CASCADE;

------------------------------------------------------------------------------------------
-- #10221: policy-path variant. An invalidation whose greatest_modified_value sits
-- EXACTLY on a bucket start must not lose its last bucket when the policy refresh
-- window is split into batches. The policy has finite start/end offsets, so no
-- null-restoration can mask a missing batch.
------------------------------------------------------------------------------------------
CREATE TABLE pol_boundary (
    time    TIMESTAMPTZ NOT NULL,
    device  INTEGER,
    value   FLOAT
);
SELECT FROM create_hypertable('pol_boundary', 'time');

-- device 1: mid-bucket rows, 2025-03-01 .. 2025-03-08
INSERT INTO pol_boundary
SELECT time, 1, 1.0
FROM generate_series('2025-03-01 12:00:00+00'::timestamptz,
                     '2025-03-08 12:00:00+00'::timestamptz,
                     '1 day'::interval) AS time;

CREATE MATERIALIZED VIEW pol_boundary_cagg
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 day', time) AS bucket, device, count(*) AS cnt
FROM pol_boundary
GROUP BY 1, 2
WITH NO DATA;

SELECT add_continuous_aggregate_policy('pol_boundary_cagg',
    start_offset => INTERVAL '30 days',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 hour',
    buckets_per_batch => 1
) AS pol_boundary_job_id \gset

-- First run materializes everything and advances the watermark to 2025-03-09
CALL run_job(:pol_boundary_job_id);

-- device 2: three rows below the watermark whose greatest value (2025-03-04
-- 00:00) lands exactly on a bucket start
INSERT INTO pol_boundary VALUES
    ('2025-03-02 12:00:00+00', 2, 1.0),
    ('2025-03-03 12:00:00+00', 2, 1.0),
    ('2025-03-04 00:00:00+00', 2, 1.0);

-- Second run must refresh all three device-2 buckets, including
-- [2025-03-04, 2025-03-05)
CALL run_job(:pol_boundary_job_id);

SELECT * FROM pol_boundary_cagg WHERE device = 2 ORDER BY bucket;

-- No stranded invalidation may remain
SELECT _timescaledb_functions.to_timestamp(lowest_modified_value)   AS stranded_low,
       _timescaledb_functions.to_timestamp(greatest_modified_value) AS stranded_high
FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
WHERE materialization_id = (SELECT mat_hypertable_id FROM _timescaledb_catalog.continuous_agg
                            WHERE user_view_name = 'pol_boundary_cagg')
  AND lowest_modified_value  > -210866803200000000
  AND greatest_modified_value < 9223372036854775807;

DROP TABLE pol_boundary CASCADE;
RESET timezone;

\c :TEST_DBNAME :ROLE_SUPERUSER
REASSIGN OWNED BY test_cagg_refresh_policy_user TO :ROLE_SUPERUSER;
REVOKE ALL ON SCHEMA public FROM test_cagg_refresh_policy_user;

DROP ROLE test_cagg_refresh_policy_user;
