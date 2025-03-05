-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER

CREATE OR REPLACE FUNCTION ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(timeout INT = -1, mock_start_time INT = 0) RETURNS VOID
AS :MODULE_PATHNAME LANGUAGE C VOLATILE;
CREATE OR REPLACE FUNCTION ts_bgw_params_create() RETURNS VOID
AS :MODULE_PATHNAME LANGUAGE C VOLATILE;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

SET timezone = 'America/Sao_Paulo';

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
        '2025-02-05 00:00:00-03', 
        '2025-03-05 00:00:00-03', 
        '1 hour'::interval) AS t,
    generate_series(1,5) AS d;

CREATE MATERIALIZED VIEW conditions_by_day
WITH (timescaledb.continuous, timescaledb.materialized_only=false) AS
SELECT
    time_bucket('1 day', time),
    device_id,
    max(temperature)
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
        schedule_interval => INTERVAL '12 h',
        nbuckets_per_batch => 10,
        max_batches_per_job_execution => 10
    ) AS job_id \gset

SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25);
SELECT * FROM sorted_bgw_log;

TRUNCATE bgw_log, conditions_by_day;

SELECT
    delete_job(:'job_id');

SELECT
    add_continuous_aggregate_policy(
        'conditions_by_day',
        start_offset => NULL,
        end_offset => NULL,
        schedule_interval => INTERVAL '12 h',
        nbuckets_per_batch => 10,
        max_batches_per_job_execution => 2
    ) AS job_id \gset

SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25);
SELECT * FROM sorted_bgw_log;
