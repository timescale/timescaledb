-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

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

-- Do not log all jobs, only FAILED executions
SHOW timescaledb.enable_job_execution_logging;

-- Start Background Workers
SELECT _timescaledb_functions.start_background_workers();

SELECT add_job('custom_job_ok', schedule_interval => interval '1 hour', initial_start := now()) AS job_id_1 \gset
SELECT add_job('custom_job_error', schedule_interval => interval '1 hour', initial_start := now()) AS job_id_2 \gset

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
SELECT test.wait_for_job_to_run(:job_id_3, 1);

SELECT timezone, fixed_schedule, config, schedule_interval
FROM alter_job(:job_id_3, timezone => 'America/Sao_Paulo', fixed_schedule => false, config => '{"key": "value"}'::jsonb, schedule_interval => interval '10 min', next_start => now());
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
