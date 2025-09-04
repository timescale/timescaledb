-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER

CREATE OR REPLACE FUNCTION ts_bgw_log_register_emit_log_hook(application_name TEXT) RETURNS VOID
AS :MODULE_PATHNAME LANGUAGE C VOLATILE;
CREATE OR REPLACE FUNCTION ts_bgw_log_unregister_emit_log_hook() RETURNS VOID
AS :MODULE_PATHNAME LANGUAGE C VOLATILE;
CREATE OR REPLACE FUNCTION ts_bgw_params_create() RETURNS VOID
AS :MODULE_PATHNAME LANGUAGE C VOLATILE;

-- Create a user with specific timezone and mock time
CREATE ROLE test_cagg_refresh_policy_user WITH LOGIN;
GRANT ALL ON SCHEMA public TO test_cagg_refresh_policy_user;

\c :TEST_DBNAME test_cagg_refresh_policy_user

CREATE PROCEDURE run_job_with_log(job_id INTEGER)
AS
$$
BEGIN
    SET LOCAL application_name = 'cagg_policy_incremental';
    CALL run_job(job_id);
END;
$$
LANGUAGE plpgsql;

SET timezone TO 'UTC';

CREATE TABLE public.bgw_log(
    msg_no INT,
    mock_time BIGINT,
    application_name TEXT,
    msg TEXT
);

CREATE VIEW sorted_bgw_log AS
SELECT
    row_number() OVER () AS msg_no,
    regexp_replace(regexp_replace(msg, '(Wait until|started at|execution time) [0-9]+(\.[0-9]+)?', '\1 (RANDOM)', 'g'), 'background worker "[^"]+"','connection') AS msg
FROM
    bgw_log
WHERE
    msg !~ '^(statement:|duration:|LOG:  background worker "[^"]+" )'
    AND application_name = 'cagg_policy_incremental'
ORDER BY
    mock_time,
    application_name COLLATE "C",
    msg_no;

CREATE TABLE public.bgw_dsm_handle_store(
    handle BIGINT
);
INSERT INTO public.bgw_dsm_handle_store VALUES (0);
SELECT ts_bgw_params_create();
SELECT ts_bgw_log_register_emit_log_hook('cagg_policy_incremental');

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
        initial_start => NOW() + INTERVAL '1 h',
        buckets_per_batch => 10
    ) AS job_id \gset

SELECT
    config
FROM
    timescaledb_information.jobs
WHERE
    job_id = :'job_id' \gset

CALL run_job_with_log(:'job_id');
SELECT * FROM sorted_bgw_log;

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

CALL run_job_with_log(:'job_id');
SELECT * FROM sorted_bgw_log;

SELECT count(*) FROM conditions_by_day;
SELECT count(*) FROM conditions_by_day_manual_refresh;

-- Should have differences
SELECT
    count(*) > 0 AS has_diff
FROM
    ((SELECT * FROM conditions_by_day_manual_refresh ORDER BY 1, 2)
    EXCEPT
    (SELECT * FROM conditions_by_day ORDER BY 1, 2)) AS diff;

CALL run_job_with_log(:'job_id');
SELECT * FROM sorted_bgw_log;

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

CALL run_job_with_log(:'job_id');
SELECT * FROM sorted_bgw_log;

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

CALL run_job_with_log(:'job_id');
SELECT * FROM sorted_bgw_log;

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

CALL run_job_with_log(:'job_id');
SELECT * FROM sorted_bgw_log;

-- Should return 10 rows because the bucket width is `1 day` and buckets per batch is `10`
SELECT count(*) FROM conditions_by_day;

TRUNCATE conditions_by_day, conditions, bgw_log;

-- Less than 1 day of data (smaller than the bucket width)
INSERT INTO conditions
VALUES ('2020-02-05 00:00:00+00', 1, 10);

CALL run_job_with_log(:'job_id');
SELECT * FROM sorted_bgw_log;

-- Should return 1 row
SELECT count(*) FROM conditions_by_day;

SELECT delete_job(:job_id);

SELECT
    add_continuous_aggregate_policy(
        'conditions_by_day',
        start_offset => NULL,
        end_offset => NULL,
        schedule_interval => INTERVAL '1 h',
        initial_start => NOW() + INTERVAL '1 h',
        buckets_per_batch => 5,
        refresh_newest_first => true -- explicitly set to true to test the default behavior
    ) AS job_id \gset

SELECT
    add_continuous_aggregate_policy(
        'conditions_by_day_manual_refresh',
        start_offset => NULL,
        end_offset => NULL,
        schedule_interval => INTERVAL '1 h',
        initial_start => NOW() + INTERVAL '1 h',
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

CALL run_job_with_log(:'job_id');
CALL run_job_with_log(:'job_id_manual');
SELECT * FROM sorted_bgw_log;

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
        start_offset => NULL,
        end_offset => NULL,
        schedule_interval => INTERVAL '1 h',
        initial_start => NOW() + INTERVAL '1 h',
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

CALL run_job_with_log(:'job_id');
SELECT * FROM sorted_bgw_log;

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

SELECT ts_bgw_log_unregister_emit_log_hook();

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER
REASSIGN OWNED BY test_cagg_refresh_policy_user TO :ROLE_CLUSTER_SUPERUSER;
REVOKE ALL ON SCHEMA public FROM test_cagg_refresh_policy_user;
DROP ROLE test_cagg_refresh_policy_user;
