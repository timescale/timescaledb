-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

ALTER DATABASE :TEST_DBNAME SET timezone TO 'UTC';

-- Log every job execution and not only the failed ones, so that the refresh
-- information of a successful policy execution shows up in the job history.
ALTER SYSTEM SET timescaledb.enable_job_execution_logging TO ON;
SELECT pg_reload_conf();

-- Reconnect to make sure the GUC is set
\c :TEST_DBNAME :ROLE_SUPERUSER

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
        '2025-02-01 00:00:00+00',
        '2025-02-20 00:00:00+00',
        '1 hour'::interval) AS t,
    generate_series(1, 2) AS d;

CREATE MATERIALIZED VIEW conditions_by_day
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT
    time_bucket('1 day', time) AS bucket,
    device_id,
    count(*)
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
        schedule_interval => INTERVAL '1 hour',
        buckets_per_batch => 5,
        max_batches_per_execution => 2
    ) AS job_id \gset

SELECT config FROM timescaledb_information.jobs WHERE job_id = :'job_id' \gset

-- The reported range is the union of the windows of the batches the execution
-- actually processed, so it is a concrete range whenever the refresh window
-- was split into batches.
CREATE VIEW refresh_info AS
SELECT
    succeeded,
    data->'info'->>'total_batches' AS total_batches,
    data->'info'->>'batches_processed' AS batches_processed,
    data->'info'->>'range_start' AS range_start,
    jsonb_typeof(data->'info'->'range_start') AS range_start_type,
    data->'info'->>'range_end' AS range_end
FROM
    _timescaledb_internal.bgw_job_stat_history
WHERE
    job_id = :job_id
ORDER BY
    id;

SELECT _timescaledb_functions.start_background_workers();

-- Test 1: the history entry of a batched refresh reports how many batches the
-- refresh window was split into, how many of them this execution got to
-- process before `max_batches_per_execution` stopped it, and the time range
-- those processed batches cover.
SELECT test.wait_for_job_to_run(:job_id, 1);
SELECT * FROM refresh_info;

-- Test 2: only two of the batches were processed before, so the next execution
-- processes the batches that are still left to refresh and reports a range
-- that covers only those.
SELECT scheduled FROM alter_job(:job_id, config => jsonb_set(:'config', '{max_batches_per_execution}', '10'));
SELECT scheduled FROM alter_job(:job_id, next_start => '2000-01-01 00:00:00+00'::timestamptz);
SELECT pg_reload_conf();
SELECT test.wait_for_job_to_run(:job_id, 2);
SELECT * FROM refresh_info;

-- Test 3: an up-to-date continuous aggregate is not split into batches, so its
-- execution reports a single batch covering the whole requested window, which
-- for a policy without offsets is open ended.
SELECT scheduled FROM alter_job(:job_id, next_start => '2000-01-01 00:00:00+00'::timestamptz);
SELECT pg_reload_conf();
SELECT test.wait_for_job_to_run(:job_id, 3);
SELECT * FROM refresh_info;

-- Test 4: on a hypertable partitioned by an integer column the range
-- boundaries are reported as numbers instead of timestamps.
CREATE TABLE measurements (
    time         BIGINT NOT NULL,
    device_id    INTEGER,
    temperature  NUMERIC
);

SELECT FROM create_hypertable('measurements', by_range('time', 100));

CREATE OR REPLACE FUNCTION measurements_now() RETURNS BIGINT
LANGUAGE SQL STABLE AS $$ SELECT 1000::BIGINT $$;

SELECT set_integer_now_func('measurements', 'measurements_now');

INSERT INTO measurements
SELECT t, d, 10
FROM generate_series(0, 999) AS t, generate_series(1, 2) AS d;

CREATE MATERIALIZED VIEW measurements_by_bucket
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT
    time_bucket(100, time) AS bucket,
    device_id,
    count(*)
FROM
    measurements
GROUP BY
    1, 2
WITH NO DATA;

SELECT
    add_continuous_aggregate_policy(
        'measurements_by_bucket',
        start_offset => 1000,
        end_offset => 0,
        schedule_interval => INTERVAL '1 hour',
        buckets_per_batch => 5
    ) AS int_job_id \gset

SELECT pg_reload_conf();
SELECT test.wait_for_job_to_run(:int_job_id, 1);
SELECT
    succeeded,
    data->'info'->>'total_batches' AS total_batches,
    data->'info'->>'batches_processed' AS batches_processed,
    data->'info'->>'range_start' AS range_start,
    jsonb_typeof(data->'info'->'range_start') AS range_start_type,
    data->'info'->>'range_end' AS range_end
FROM
    _timescaledb_internal.bgw_job_stat_history
WHERE
    job_id = :int_job_id
ORDER BY
    id;

-- Test 5: an execution that fails part way through never reports anything, so
-- its history entry carries the error information but no `info` key. The
-- refresh is made to fail after the first batch with an error injection.
CREATE TABLE failing (
    time         TIMESTAMP WITH TIME ZONE NOT NULL,
    device_id    INTEGER,
    temperature  NUMERIC
);

SELECT FROM create_hypertable('failing', by_range('time'));

INSERT INTO failing
SELECT
    t, d, 10
FROM
    generate_series(
        '2025-02-01 00:00:00+00',
        '2025-02-20 00:00:00+00',
        '1 hour'::interval) AS t,
    generate_series(1, 2) AS d;

CREATE MATERIALIZED VIEW failing_by_day
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT
    time_bucket('1 day', time) AS bucket,
    device_id,
    count(*)
FROM
    failing
GROUP BY
    1, 2
WITH NO DATA;

SELECT debug_waitpoint_enable('cagg_policy_batch_1_after_refresh');

SELECT
    add_continuous_aggregate_policy(
        'failing_by_day',
        start_offset => NULL,
        end_offset => NULL,
        schedule_interval => INTERVAL '1 hour',
        buckets_per_batch => 5
    ) AS fail_job_id \gset

-- `test.wait_for_job_to_run_or_fail` returns as soon as the job started, which
-- is an earlier transaction than the one writing the finished history entry.
-- The failure counter on the other hand is bumped in the very transaction that
-- marks the history entry as finished, so waiting on it makes the assertion
-- below see a completed entry.
CREATE FUNCTION wait_for_job_to_fail(job_param_id INTEGER, spins INTEGER=:TEST_SPINWAIT_ITERS)
RETURNS BOOLEAN LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    failures BIGINT;
BEGIN
    FOR i IN 1..spins
    LOOP
        SELECT total_failures FROM _timescaledb_internal.bgw_job_stat WHERE job_id = job_param_id INTO failures;
        IF (failures > 0) THEN
            RETURN true;
        END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    RAISE INFO 'wait_for_job_to_fail: timeout after % tries', spins;
    RETURN false;
END
$BODY$;

SELECT pg_reload_conf();
SELECT wait_for_job_to_fail(:fail_job_id);

-- The job keeps being retried while the error injection is enabled, so only
-- look at the entries of executions that already finished. They are all
-- identical failures.
SELECT DISTINCT
    succeeded,
    data ? 'info' AS has_info,
    data->'error_data'->>'sqlerrcode' AS sqlerrcode
FROM
    _timescaledb_internal.bgw_job_stat_history
WHERE
    job_id = :fail_job_id
    AND succeeded IS NOT NULL;

SELECT debug_waitpoint_release('cagg_policy_batch_1_after_refresh');


SELECT _timescaledb_functions.stop_background_workers();

ALTER SYSTEM RESET timescaledb.enable_job_execution_logging;
SELECT pg_reload_conf();
