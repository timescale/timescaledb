-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

ALTER DATABASE :TEST_DBNAME SET timezone TO 'UTC';
\c

CREATE PROCEDURE custom_job_ok(job_id int, config jsonb) LANGUAGE PLPGSQL AS
$$
BEGIN
  RAISE INFO 'custom_job';
END
$$;

CREATE PROCEDURE custom_job_error(job_id int, config jsonb) LANGUAGE PLPGSQL AS
$$
BEGIN
  PERFORM 1/0;
END
$$;

CREATE VIEW job_history_summary AS
SELECT job_id, succeeded, count(*) AS record_count
  FROM _timescaledb_internal.bgw_job_stat_history
GROUP BY job_id, succeeded;

CREATE VIEW recent_job_history_summary AS
SELECT job_id, succeeded, count(*) AS record_count
  FROM _timescaledb_internal.bgw_job_stat_history
 WHERE execution_finish > now() - interval '30 days'
GROUP BY job_id, succeeded;

-- Do not log all jobs, only FAILED executions
SHOW timescaledb.enable_job_execution_logging;


SELECT add_job('custom_job_ok', schedule_interval => interval '1 hour', initial_start := now()) AS job_id_1 \gset
SELECT add_job('custom_job_error', schedule_interval => interval '1 hour', initial_start := now()) AS job_id_2 \gset

-- Start Background Workers
SELECT _timescaledb_functions.start_background_workers();

SELECT test.wait_for_job_to_run(:job_id_1, 1);
SELECT test.wait_for_job_to_run(:job_id_2, 1);

-- only 1 failure
SELECT count(*), succeeded FROM timescaledb_information.job_history WHERE job_id >= 1000 GROUP BY 2 ORDER BY 2;
SELECT proc_schema, proc_name, sqlerrcode, err_message FROM timescaledb_information.job_history WHERE job_id >= 1000 AND succeeded IS FALSE;

-- Check current jobs status
SELECT job_id, job_status, total_runs, total_successes, total_failures
FROM timescaledb_information.job_stats
WHERE job_id >= 1000
ORDER BY job_id;

-- Log all executions
ALTER SYSTEM SET timescaledb.enable_job_execution_logging TO ON;
SELECT pg_reload_conf();

-- Reconnect to make sure the GUC is set
\c :TEST_DBNAME :ROLE_SUPERUSER

SELECT scheduled FROM alter_job(:job_id_1, next_start => now());
SELECT scheduled FROM alter_job(:job_id_2, next_start => now());

SELECT _timescaledb_functions.restart_background_workers();
SELECT test.wait_for_job_to_run(:job_id_1, 2);
SELECT test.wait_for_job_to_run(:job_id_2, 2);

-- 1 succeeded 2 failures
SELECT count(*), succeeded FROM timescaledb_information.job_history WHERE job_id >= 1000 GROUP BY 2 ORDER BY 2;

-- Check current jobs status
SELECT job_id, job_status, total_runs, total_successes, total_failures
FROM timescaledb_information.job_stats
WHERE job_id >= 1000
ORDER BY job_id;

-- Check config changes over time
SELECT scheduled FROM alter_job(:job_id_1, config => '{"foo": 1}'::jsonb);
SELECT scheduled FROM alter_job(:job_id_2, config => '{"bar": 1}'::jsonb);

SELECT scheduled FROM alter_job(:job_id_1, next_start => now());
SELECT scheduled FROM alter_job(:job_id_2, next_start => now());

SELECT _timescaledb_functions.restart_background_workers();
SELECT test.wait_for_job_to_run(:job_id_1, 3);
SELECT test.wait_for_job_to_run(:job_id_2, 3);

-- Check job execution history
SELECT job_id, pid IS NOT NULL AS pid, proc_schema, proc_name, succeeded, config, sqlerrcode, err_message
FROM timescaledb_information.job_history
WHERE job_id >= 1000
ORDER BY id, job_id;

-- Changing the config of one job
SELECT scheduled FROM alter_job(:job_id_1, config => '{"foo": 2, "bar": 1}'::jsonb);
SELECT scheduled FROM alter_job(:job_id_1, next_start => now());
SELECT _timescaledb_functions.restart_background_workers();
SELECT test.wait_for_job_to_run(:job_id_1, 4);

-- Check job execution history
SELECT job_id, pid IS NOT NULL AS pid, proc_schema, proc_name, succeeded, config, sqlerrcode, err_message
FROM timescaledb_information.job_history
WHERE job_id = :job_id_1
ORDER BY id;

-- Change the job procedure to alter the job configuration during the execution
CREATE OR REPLACE PROCEDURE custom_job_ok(job_id int, config jsonb) LANGUAGE PLPGSQL AS
$$
BEGIN
  RAISE INFO 'custom_job';
  PERFORM alter_job(job_id, config => '{"config_changed_by_job_execution": 1}'::jsonb);
END
$$;

-- Run the job
SELECT scheduled FROM alter_job(:job_id_1, next_start => now());
SELECT _timescaledb_functions.restart_background_workers();
SELECT test.wait_for_job_to_run(:job_id_1, 5);

-- Check job execution history
SELECT job_id, pid IS NOT NULL AS pid, proc_schema, proc_name, succeeded, config, sqlerrcode, err_message
FROM timescaledb_information.job_history
WHERE job_id = :job_id_1
ORDER BY id;

-- Change the job procedure to alter the job configuration during the execution
CREATE OR REPLACE PROCEDURE custom_job_ok(job_id int, config jsonb) LANGUAGE PLPGSQL AS
$$
BEGIN
  RAISE INFO 'custom_job';
  PERFORM alter_job(job_id, config => '{"change_not_logged": 1}'::jsonb);
  COMMIT;
  PERFORM alter_job(job_id, config => '{"only_last_change_is_logged": 1}'::jsonb);
  COMMIT;
END
$$;

-- Run the job
SELECT scheduled FROM alter_job(:job_id_1, next_start => now());
SELECT _timescaledb_functions.restart_background_workers();
SELECT test.wait_for_job_to_run(:job_id_1, 6);

-- Check job execution history
SELECT job_id, pid IS NOT NULL AS pid, proc_schema, proc_name, succeeded, config, sqlerrcode, err_message
FROM timescaledb_information.job_history
WHERE job_id = :job_id_1
ORDER BY id;

-- Alter other information about the job
CREATE PROCEDURE custom_job_alter(job_id int, config jsonb) LANGUAGE PLPGSQL AS
$$
BEGIN
  RAISE LOG 'custom_job_alter';
END
$$;

SELECT add_job('custom_job_alter', schedule_interval => interval '1 hour', initial_start := now()) AS job_id_3 \gset
SELECT _timescaledb_functions.restart_background_workers();
SELECT test.wait_for_job_to_run(:job_id_3, 1);

SELECT timezone, fixed_schedule, config, schedule_interval
FROM alter_job(:job_id_3, timezone => 'America/Sao_Paulo', fixed_schedule => false, config => '{"key": "value"}'::jsonb, schedule_interval => interval '10 min', next_start => now());
SELECT _timescaledb_functions.restart_background_workers();
SELECT test.wait_for_job_to_run(:job_id_3, 2);

-- Should return two executions, the second will show the changed values
SELECT job_id, succeeded, data->'job'->>'timezone' AS timezone, data->'job'->>'fixed_schedule' AS fixed_schedule, data->'job'->>'schedule_interval' AS schedule_interval, data->'job'->'config' AS config
FROM _timescaledb_internal.bgw_job_stat_history
WHERE job_id = :job_id_3
ORDER BY id;

SELECT delete_job(:job_id_1);
SELECT delete_job(:job_id_2);
SELECT delete_job(:job_id_3);

ALTER SYSTEM RESET timescaledb.enable_job_execution_logging;
SELECT pg_reload_conf();

\c :TEST_DBNAME :ROLE_SUPERUSER

-- The GUC is PGC_SIGHUP context so only ALTER SYSTEM is allowed
\set ON_ERROR_STOP 0
SHOW timescaledb.enable_job_execution_logging;
SET timescaledb.enable_job_execution_logging TO OFF;
SHOW timescaledb.enable_job_execution_logging;
ALTER DATABASE :TEST_DBNAME SET timescaledb.enable_job_execution_logging TO ON;
SHOW timescaledb.enable_job_execution_logging;
\set ON_ERROR_STOP 1

SELECT _timescaledb_functions.stop_background_workers();

-- Test bgw_job_stat_history retention job

-- Alter the drop_after interval to be fixed (30 days) to ensure tests are deterministic
SELECT config AS config FROM _timescaledb_config.bgw_job WHERE id = 3 \gset
SELECT config FROM alter_job(3, config => jsonb_set(:'config', '{drop_after}', '"30 days"'));

-- These configuration should fail since they are not valid.
\set ON_ERROR_STOP 0
SELECT config FROM alter_job(3, config => :'config'::jsonb - 'drop_after');
SELECT config FROM alter_job(3, config => :'config'::jsonb - 'max_successes_per_job');
SELECT config FROM alter_job(3, config => :'config'::jsonb - 'max_failures_per_job');
SELECT config FROM alter_job(3, config => jsonb_set(:'config', '{max_successes_per_job}', '0'));
SELECT config FROM alter_job(3, config => jsonb_set(:'config', '{max_failures_per_job}', '0'));
SELECT config FROM alter_job(3, config => jsonb_set(:'config', '{max_successes_per_job}', '"none"'));
SELECT config FROM alter_job(3, config => jsonb_set(:'config', '{max_failures_per_job}', '"none"'));
\set ON_ERROR_STOP 1

-- Test 1
TRUNCATE _timescaledb_internal.bgw_job_stat_history;

-- Insert test data: jobs every 15 minutes from 3 months ago to today
-- Each job runs for 5 minutes (job_id=100, pid=12345)
-- Fix NOW to ensure the tests are deterministic

INSERT INTO _timescaledb_internal.bgw_job_stat_history
(job_id, pid, succeeded, execution_start, execution_finish, data)
SELECT
    100 as job_id,
    12345 as pid,
    true as succeeded,
    ts as execution_start,
    ts + interval '5 minutes' as execution_finish,
    '{}'::jsonb as data
FROM generate_series(now() - interval '90 days', now(), interval '15 minutes') as ts;

-- Check data after insertion
select * from job_history_summary;

-- Test the retention job (job id 3)
CALL run_job(3);

-- Check data after retention
SELECT * FROM job_history_summary;

-- Verify only recent records remain
SELECT * FROM recent_job_history_summary;

-- Cleanup
TRUNCATE _timescaledb_internal.bgw_job_stat_history;

-- Test 2: Empty table (no job history)
CALL run_job(3);
SELECT * FROM job_history_summary;

-- Verify only recent records remain
SELECT * FROM recent_job_history_summary;

-- Test 3: Odd number of entries (5 entries)
INSERT INTO _timescaledb_internal.bgw_job_stat_history
(job_id, pid, succeeded, execution_start, execution_finish, data)
VALUES
(301, 3001, true, now() - interval '60 days', now() - interval '60 days' + interval '5 minutes', '{}'),
(302, 3002, true, now() - interval '6 weeks', now() - interval '6 weeks' + interval '5 minutes', '{}'),
(303, 3003, true, now() - interval '30 days', now() - interval '30 days' + interval '5 minutes', '{}'),
(301, 3001, true, now() - interval '2 weeks', now() - interval '2 weeks' + interval '5 minutes', '{}'),
(304, 3004, true, now() - interval '1 week', now() - interval '1 week' + interval '5 minutes', '{}');

SELECT * FROM job_history_summary;
CALL run_job(3);
SELECT * FROM job_history_summary;

-- Verify only recent records remain
SELECT * FROM recent_job_history_summary;

TRUNCATE _timescaledb_internal.bgw_job_stat_history;

-- Test 4: Even number of entries (6 entries)
INSERT INTO _timescaledb_internal.bgw_job_stat_history
(job_id, pid, succeeded, execution_start, execution_finish, data)
VALUES
(401, 4001, true, now() - interval '90 days', now() - interval '90 days' + interval '5 minutes', '{}'),
(402, 4002, true, now() - interval '60 days', now() - interval '60 days' + interval '5 minutes', '{}'),
(403, 4003, true, now() - interval '6 weeks', now() - interval '6 weeks' + interval '5 minutes', '{}'),
(401, 4001, true, now() - interval '30 days', now() - interval '30 days' + interval '5 minutes', '{}'),
(404, 4004, true, now() - interval '2 weeks', now() - interval '2 weeks' + interval '5 minutes', '{}'),
(402, 4002, true, now() - interval '1 week', now() - interval '1 week' + interval '5 minutes', '{}');

SELECT * FROM job_history_summary;
CALL run_job(3);
SELECT * FROM job_history_summary;

-- Verify only recent records remain
SELECT * FROM recent_job_history_summary;

TRUNCATE _timescaledb_internal.bgw_job_stat_history;

-- Test 5: Missing middle job id (gaps in sequence)
INSERT INTO _timescaledb_internal.bgw_job_stat_history
(job_id, pid, succeeded, execution_start, execution_finish, data)
SELECT
    501 + (row_number() over () % 3) as job_id,
    5001 + (row_number() over () % 3) as pid,
    true as succeeded,
    ts as execution_start,
    ts + interval '5 minutes' as execution_finish,
    '{}'::jsonb as data
FROM generate_series(now() - interval '60 days', now() - interval '1 week', interval '1 week') as ts;

-- Delete some records to create gaps
DELETE FROM _timescaledb_internal.bgw_job_stat_history
WHERE id IN (SELECT id FROM _timescaledb_internal.bgw_job_stat_history ORDER BY id LIMIT 2 OFFSET 2);

SELECT * FROM job_history_summary;
CALL run_job(3);
SELECT * FROM job_history_summary;

-- Verify only recent records remain
SELECT * FROM recent_job_history_summary;

TRUNCATE _timescaledb_internal.bgw_job_stat_history;

-- Test 6: All records older than retention period
INSERT INTO _timescaledb_internal.bgw_job_stat_history
(job_id, pid, succeeded, execution_start, execution_finish, data)
VALUES
(601, 6001, true, now() - interval '90 days', now() - interval '90 days' + interval '5 minutes', '{}'),
(602, 6002, true, now() - interval '60 days', now() - interval '60 days' + interval '5 minutes', '{}'),
(601, 6001, true, now() - interval '6 weeks', now() - interval '6 weeks' + interval '5 minutes', '{}');

SELECT * FROM job_history_summary;
CALL run_job(3);
SELECT * FROM job_history_summary;

-- Verify only recent records remain
SELECT * FROM recent_job_history_summary;

TRUNCATE _timescaledb_internal.bgw_job_stat_history;

-- Test 7: No records older than retention period
INSERT INTO _timescaledb_internal.bgw_job_stat_history
(job_id, pid, succeeded, execution_start, execution_finish, data)
VALUES
(701, 7001, true, now() - interval '1 week', now() - interval '1 week' + interval '7 minutes', '{}'),
(702, 7002, true, now() - interval '6 days', now() - interval '6 days' + interval '7 minutes', '{}'),
(703, 7003, true, now() - interval '7 days', now() - interval '7 days' + interval '7 minutes', '{}'),
(701, 7001, true, now() - interval '4 days', now() - interval '4 days' + interval '7 minutes', '{}');

SELECT * FROM job_history_summary;
CALL run_job(3);
SELECT * FROM job_history_summary;

-- Verify only recent records remain
SELECT * FROM recent_job_history_summary;

TRUNCATE _timescaledb_internal.bgw_job_stat_history;

-- Test 8: No records older than retention period and not more than
-- the expected number of successes and failures per job.
SELECT config AS config FROM _timescaledb_config.bgw_job WHERE id = 3 \gset
SELECT config FROM alter_job(3,
    config => jsonb_set(jsonb_set(:'config', '{max_failures_per_job}', '15'), '{max_successes_per_job}', '10'));
INSERT INTO _timescaledb_internal.bgw_job_stat_history
(job_id, pid, succeeded, execution_start, execution_finish, data)
VALUES
(803, 7003, true, now() - interval '7 days', now() - interval '7 days' + interval '7 minutes', '{}'),
(801, 7001, true, now() - interval '4 days', now() - interval '4 days' + interval '7 minutes', '{}');

INSERT INTO
   _timescaledb_internal.bgw_job_stat_history(job_id, pid, succeeded, execution_start, execution_finish, data)
SELECT 801, 7001, true, now() - format('%s hour', hours)::interval, now() - interval '1 week' + interval '7 minutes', '{}'
FROM generate_series(1,20) hours;

INSERT INTO
   _timescaledb_internal.bgw_job_stat_history(job_id, pid, succeeded, execution_start, execution_finish, data)
SELECT 802, 7001, false, now() - format('%s minutes', hours)::interval, now() - interval '6 days' + interval '7 minutes', '{}'
FROM generate_series(1,20) hours;

SELECT * FROM job_history_summary;
CALL run_job(3);
SELECT * FROM job_history_summary;

-- Verify only recent records remain
SELECT * FROM recent_job_history_summary;

-- Cleanup
TRUNCATE _timescaledb_internal.bgw_job_stat_history;

-- Test that lock_timeout can be configured
SELECT config FROM alter_job(3, config => jsonb_set(:'config', '{lock_timeout}', '"1s"'));
CALL run_job(3);

-- Test the job_history_bsearch function directly as well
-- It returns the first element where execution_finish >= search_point or NULL if no such element exists

\set NOW '2025-08-15 12:34:00'

-- No elements in table
SELECT _timescaledb_functions.job_history_bsearch(:'NOW'::timestamptz - interval '1 month');

-- Single element
INSERT INTO _timescaledb_internal.bgw_job_stat_history
(id, job_id, pid, succeeded, execution_start, execution_finish, data)
VALUES
(5, 601, 6001, true, :'NOW'::timestamptz - interval '2 weeks', :'NOW'::timestamptz - interval '2 weeks' + interval '5 minutes', '{}');

-- Return the single element
SELECT _timescaledb_functions.job_history_bsearch(:'NOW'::timestamptz - interval '1 month');

-- Return NULL
SELECT _timescaledb_functions.job_history_bsearch(:'NOW'::timestamptz - interval '1 week');

TRUNCATE _timescaledb_internal.bgw_job_stat_history;

-- Two elements
INSERT INTO _timescaledb_internal.bgw_job_stat_history
(id, job_id, pid, succeeded, execution_start, execution_finish, data)
VALUES
(5, 701, 7001, true, :'NOW'::timestamptz - interval '3 weeks', :'NOW'::timestamptz - interval '3 weeks' + interval '5 minutes', '{}'),
(6, 702, 7002, true, :'NOW'::timestamptz - interval '1 week', :'NOW'::timestamptz - interval '1 week' + interval '5 minutes', '{}');

-- Returns the first element
SELECT _timescaledb_functions.job_history_bsearch(:'NOW'::timestamptz - interval '1 month');
-- Returns the second element
SELECT _timescaledb_functions.job_history_bsearch(:'NOW'::timestamptz - interval '2 weeks');
-- Returns NULL
SELECT _timescaledb_functions.job_history_bsearch(:'NOW'::timestamptz - interval '3 days');

TRUNCATE _timescaledb_internal.bgw_job_stat_history;

-- Odd number of elements
INSERT INTO _timescaledb_internal.bgw_job_stat_history
(id, job_id, pid, succeeded, execution_start, execution_finish, data)
VALUES
(5, 801, 8001, true, :'NOW'::timestamptz - interval '5 weeks', :'NOW'::timestamptz - interval '5 weeks' + interval '5 minutes', '{}'),
(6, 802, 8002, true, :'NOW'::timestamptz - interval '4 weeks', :'NOW'::timestamptz - interval '4 weeks' + interval '5 minutes', '{}'),
(7, 803, 8003, true, :'NOW'::timestamptz - interval '3 weeks', :'NOW'::timestamptz - interval '3 weeks' + interval '5 minutes', '{}'),
(8, 804, 8004, true, :'NOW'::timestamptz - interval '2 weeks', :'NOW'::timestamptz - interval '2 weeks' + interval '5 minutes', '{}'),
(9, 805, 8005, true, :'NOW'::timestamptz - interval '1 week', :'NOW'::timestamptz - interval '1 week' + interval '5 minutes', '{}');

-- Returns the first element
SELECT _timescaledb_functions.job_history_bsearch(:'NOW'::timestamptz - interval '6 weeks');
-- Returns the middle element
SELECT _timescaledb_functions.job_history_bsearch(:'NOW'::timestamptz - interval '3 weeks');
-- Returns one after the middle element
SELECT _timescaledb_functions.job_history_bsearch(:'NOW'::timestamptz - interval '2 weeks 3 days');
-- Returns NULL
SELECT _timescaledb_functions.job_history_bsearch(:'NOW'::timestamptz - interval '2 days');

TRUNCATE _timescaledb_internal.bgw_job_stat_history;

-- Even number of elements
INSERT INTO _timescaledb_internal.bgw_job_stat_history
(id, job_id, pid, succeeded, execution_start, execution_finish, data)
VALUES
(5, 902, 9002, true, :'NOW'::timestamptz - interval '5 weeks', :'NOW'::timestamptz - interval '5 weeks' + interval '5 minutes', '{}'),
(6, 903, 9003, true, :'NOW'::timestamptz - interval '4 weeks', :'NOW'::timestamptz - interval '4 weeks' + interval '5 minutes', '{}'),
(7, 904, 9004, true, :'NOW'::timestamptz - interval '3 weeks', :'NOW'::timestamptz - interval '3 weeks' + interval '5 minutes', '{}'),
(8, 905, 9005, true, :'NOW'::timestamptz - interval '2 weeks', :'NOW'::timestamptz - interval '2 weeks' + interval '5 minutes', '{}'),
(9, 906, 9006, true, :'NOW'::timestamptz - interval '1 week', :'NOW'::timestamptz - interval '1 week' + interval '5 minutes', '{}'),
(10, 907, 9007, true, :'NOW'::timestamptz - interval '3 days', :'NOW'::timestamptz - interval '3 days' + interval '5 minutes', '{}');

-- Returns the first element
SELECT _timescaledb_functions.job_history_bsearch(:'NOW'::timestamptz - interval '6 weeks');
-- Returns the middle element
SELECT _timescaledb_functions.job_history_bsearch(:'NOW'::timestamptz - interval '3 weeks');
-- Returns one after the middle element
SELECT _timescaledb_functions.job_history_bsearch(:'NOW'::timestamptz - interval '2 weeks 3 days');
-- Returns NULL
SELECT _timescaledb_functions.job_history_bsearch(:'NOW'::timestamptz - interval '2 days');

TRUNCATE _timescaledb_internal.bgw_job_stat_history;

-- With gaps in id
INSERT INTO _timescaledb_internal.bgw_job_stat_history
(id, job_id, pid, succeeded, execution_start, execution_finish, data)
VALUES
(10, 1001, 10001, true, :'NOW'::timestamptz - interval '5 weeks', :'NOW'::timestamptz - interval '5 weeks' + interval '5 minutes', '{}'),
(11, 1002, 10002, true, :'NOW'::timestamptz - interval '4 weeks', :'NOW'::timestamptz - interval '4 weeks' + interval '5 minutes', '{}'),
(13, 1003, 10003, true, :'NOW'::timestamptz - interval '3 weeks', :'NOW'::timestamptz - interval '3 weeks' + interval '5 minutes', '{}'),
(15, 1004, 10004, true, :'NOW'::timestamptz - interval '2 weeks', :'NOW'::timestamptz - interval '2 weeks' + interval '5 minutes', '{}'),
(16, 1005, 10005, true, :'NOW'::timestamptz - interval '1 week', :'NOW'::timestamptz - interval '1 week' + interval '5 minutes', '{}');

-- Returns id before the gap
SELECT _timescaledb_functions.job_history_bsearch(:'NOW'::timestamptz - interval '3 weeks 3 days') AS result_gap_trigger1;
-- Returns id after the gap
SELECT _timescaledb_functions.job_history_bsearch(:'NOW'::timestamptz - interval '2 weeks 3 days') AS result_gap_trigger2;
-- Returns second element
SELECT _timescaledb_functions.job_history_bsearch(:'NOW'::timestamptz - interval '4 weeks 3 days') AS result_gap_trigger3;

-- Final cleanup
TRUNCATE _timescaledb_internal.bgw_job_stat_history;
