-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE TABLE custom_log(job_id int, args jsonb, extra text, runner NAME DEFAULT CURRENT_ROLE);

CREATE OR REPLACE FUNCTION custom_func(jobid int, args jsonb) RETURNS VOID LANGUAGE SQL AS
$$
  INSERT INTO custom_log VALUES($1, $2, 'custom_func');
$$;

CREATE OR REPLACE FUNCTION custom_func_definer(jobid int, args jsonb) RETURNS VOID LANGUAGE SQL AS
$$
  INSERT INTO custom_log VALUES($1, $2, 'security definer');
$$ SECURITY DEFINER;

CREATE OR REPLACE PROCEDURE custom_proc(job_id int, args jsonb) LANGUAGE SQL AS
$$
  INSERT INTO custom_log VALUES($1, $2, 'custom_proc');
$$;

-- procedure with transaction handling
CREATE OR REPLACE PROCEDURE custom_proc2(job_id int, args jsonb) LANGUAGE PLPGSQL AS
$$
BEGIN
  INSERT INTO custom_log VALUES($1, $2, 'custom_proc 1 COMMIT');
  COMMIT;
  INSERT INTO custom_log VALUES($1, $2, 'custom_proc 2 ROLLBACK');
  ROLLBACK;
  INSERT INTO custom_log VALUES($1, $2, 'custom_proc 3 COMMIT');
  COMMIT;
END
$$;

\set ON_ERROR_STOP 0
-- test bad input
SELECT add_job(NULL, '1h');
SELECT add_job(0, '1h');
-- this will return an error about Oid 4294967295
-- while regproc is unsigned int postgres has an implicit cast from int to regproc
SELECT add_job(-1, '1h');
SELECT add_job('invalid_func', '1h');
SELECT add_job('custom_func', NULL);
SELECT add_job('custom_func', 'invalid interval');
\set ON_ERROR_STOP 1

SELECT add_job('custom_func','1h', config:='{"type":"function"}'::jsonb);
SELECT add_job('custom_proc','1h', config:='{"type":"procedure"}'::jsonb);
SELECT add_job('custom_proc2','1h', config:= '{"type":"procedure"}'::jsonb);

SELECT add_job('custom_func', '1h', config:='{"type":"function"}'::jsonb);
SELECT add_job('custom_func_definer', '1h', config:='{"type":"function"}'::jsonb);

SELECT * FROM timescaledb_information.jobs WHERE job_id != 1 ORDER BY 1;

SELECT count(*) FROM _timescaledb_config.bgw_job WHERE config->>'type' IN ('procedure', 'function');

\set ON_ERROR_STOP 0
-- test bad input
CALL run_job(NULL);
CALL run_job(-1);
\set ON_ERROR_STOP 1

CALL run_job(1000);
CALL run_job(1001);
CALL run_job(1002);
CALL run_job(1003);
CALL run_job(1004);

SELECT * FROM custom_log ORDER BY job_id, extra;

\set ON_ERROR_STOP 0
-- test bad input
SELECT delete_job(NULL);
SELECT delete_job(-1);
\set ON_ERROR_STOP 1

-- We keep job 1000 for some additional checks.
SELECT delete_job(1001);
SELECT delete_job(1002);
SELECT delete_job(1003);
SELECT delete_job(1004);

-- check jobs got removed
SELECT count(*) FROM timescaledb_information.jobs WHERE job_id >= 1001;

\c :TEST_DBNAME :ROLE_SUPERUSER

\set ON_ERROR_STOP 0
-- test bad input
SELECT alter_job(NULL, if_exists => false);
SELECT alter_job(-1, if_exists => false);
\set ON_ERROR_STOP 1
-- test bad input but don't fail
SELECT alter_job(NULL, if_exists => true);
SELECT alter_job(-1, if_exists => true);

-- test altering job with NULL config
SELECT job_id FROM alter_job(1000,scheduled:=false);
SELECT scheduled, config FROM timescaledb_information.jobs WHERE job_id = 1000;

-- test updating job settings
SELECT job_id FROM alter_job(1000,config:='{"test":"test"}');
SELECT scheduled, config FROM timescaledb_information.jobs WHERE job_id = 1000;
SELECT job_id FROM alter_job(1000,scheduled:=true);
SELECT scheduled, config FROM timescaledb_information.jobs WHERE job_id = 1000;
SELECT job_id FROM alter_job(1000,scheduled:=false);
SELECT scheduled, config FROM timescaledb_information.jobs WHERE job_id = 1000;

-- Done with job 1000 now, so remove it.
SELECT delete_job(1000);

--test for #2793
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
-- background workers are disabled, so the job will not run --
SELECT add_job( proc=>'custom_func',
     schedule_interval=>'1h', initial_start =>'2018-01-01 10:00:00-05');

SELECT job_id, next_start, scheduled, schedule_interval
FROM timescaledb_information.jobs WHERE job_id > 1000;
\x
SELECT * FROM timescaledb_information.job_stats WHERE job_id > 1000;
\x

-- tests for #3545
CREATE FUNCTION wait_for_job_to_run(job_param_id INTEGER, expected_runs INTEGER, spins INTEGER=:TEST_SPINWAIT_ITERS) RETURNS BOOLEAN LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    r RECORD;
BEGIN
    FOR i in 1..spins
    LOOP
    SELECT total_successes, total_failures FROM _timescaledb_internal.bgw_job_stat WHERE job_id=job_param_id INTO r;
    IF (r.total_failures > 0) THEN
        RAISE INFO 'wait_for_job_to_run: job execution failed';
        RETURN false;
    ELSEIF (r.total_successes = expected_runs) THEN
        RETURN true;
    ELSEIF (r.total_successes > expected_runs) THEN
        RAISE 'num_runs > expected';
    ELSE
        PERFORM pg_sleep(0.1);
    END IF;
    END LOOP;
    RAISE INFO 'wait_for_job_to_run: timeout after % tries', spins;
    RETURN false;
END
$BODY$;

TRUNCATE custom_log;

-- Nested procedure call
CREATE OR REPLACE PROCEDURE custom_proc_nested(job_id int, args jsonb) LANGUAGE PLPGSQL AS
$$
BEGIN
  INSERT INTO custom_log VALUES($1, $2, 'custom_proc_nested 1 COMMIT');
  COMMIT;
  INSERT INTO custom_log VALUES($1, $2, 'custom_proc_nested 2 ROLLBACK');
  ROLLBACK;
  INSERT INTO custom_log VALUES($1, $2, 'custom_proc_nested 3 COMMIT');
  COMMIT;
END
$$;

CREATE OR REPLACE PROCEDURE custom_proc3(job_id int, args jsonb) LANGUAGE PLPGSQL AS
$$
BEGIN
    CALL custom_proc_nested(job_id, args);
END
$$;

CREATE OR REPLACE PROCEDURE custom_proc4(job_id int, args jsonb) LANGUAGE PLPGSQL AS
$$
BEGIN
    INSERT INTO custom_log VALUES($1, $2, 'custom_proc4 1 COMMIT');
    COMMIT;
    INSERT INTO custom_log VALUES($1, $2, 'custom_proc4 2 ROLLBACK');
    ROLLBACK;
    RAISE EXCEPTION 'forced exception';
    INSERT INTO custom_log VALUES($1, $2, 'custom_proc4 3 ABORT');
    COMMIT;
END
$$;

CREATE OR REPLACE PROCEDURE custom_proc5(job_id int, args jsonb) LANGUAGE PLPGSQL AS
$$
BEGIN
    CALL refresh_continuous_aggregate('conditions_summary_daily', '2021-08-01 00:00', '2021-08-31 00:00');
END
$$;

-- Remove any default jobs, e.g., telemetry
\c :TEST_DBNAME :ROLE_SUPERUSER
TRUNCATE _timescaledb_config.bgw_job RESTART IDENTITY CASCADE;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

SELECT add_job('custom_proc2', '1h', config := '{"type":"procedure"}'::jsonb, initial_start := now()) AS job_id_1 \gset
SELECT add_job('custom_proc3', '1h', config := '{"type":"procedure"}'::jsonb, initial_start := now()) AS job_id_2 \gset

\c :TEST_DBNAME :ROLE_SUPERUSER
-- Start Background Workers
SELECT _timescaledb_internal.start_background_workers();

-- Wait for jobs
SELECT wait_for_job_to_run(:job_id_1, 1);
SELECT wait_for_job_to_run(:job_id_2, 1);

-- Check results
SELECT * FROM custom_log ORDER BY job_id, extra;

-- Delete previous jobs
SELECT delete_job(:job_id_1);
SELECT delete_job(:job_id_2);
TRUNCATE custom_log;

-- Forced Exception
SELECT add_job('custom_proc4', '1h', config := '{"type":"procedure"}'::jsonb, initial_start := now()) AS job_id_3 \gset
SELECT wait_for_job_to_run(:job_id_3, 1);

-- Check results
SELECT * FROM custom_log ORDER BY job_id, extra;

-- Delete previous jobs
SELECT delete_job(:job_id_3);

CREATE TABLE conditions (
  time TIMESTAMP NOT NULL,
  location TEXT NOT NULL,
  location2 char(10) NOT NULL,
  temperature DOUBLE PRECISION NULL,
  humidity DOUBLE PRECISION NULL
) WITH (autovacuum_enabled = FALSE);

SELECT create_hypertable('conditions', 'time', chunk_time_interval := '15 days'::interval);

ALTER TABLE conditions
  SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'location',
    timescaledb.compress_orderby = 'time'
);
INSERT INTO conditions
SELECT generate_series('2021-08-01 00:00'::timestamp, '2021-08-31 00:00'::timestamp, '1 day'), 'POR', 'klick', 55, 75;

-- Chunk compress stats
SELECT * FROM _timescaledb_internal.compressed_chunk_stats ORDER BY chunk_name;

-- Compression policy
SELECT add_compression_policy('conditions', interval '1 day') AS job_id_4 \gset
SELECT wait_for_job_to_run(:job_id_4, 1);

-- Chunk compress stats
SELECT * FROM _timescaledb_internal.compressed_chunk_stats ORDER BY chunk_name;

--TEST compression job after inserting data into previously compressed chunk
INSERT INTO conditions
SELECT generate_series('2021-08-01 00:00'::timestamp, '2021-08-31 00:00'::timestamp, '1 day'), 'NYC', 'nycity', 40, 40;

SELECT id, table_name, status from _timescaledb_catalog.chunk 
where hypertable_id = (select id from _timescaledb_catalog.hypertable 
                       where table_name = 'conditions')
order by id; 

--running job second time, wait for it to complete 
select t.schedule_interval FROM alter_job(:job_id_4, next_start=> now() ) t;
SELECT wait_for_job_to_run(:job_id_4, 2);

SELECT id, table_name, status from _timescaledb_catalog.chunk 
where hypertable_id = (select id from _timescaledb_catalog.hypertable 
                       where table_name = 'conditions')
order by id; 


-- Decompress chunks before create the cagg
SELECT decompress_chunk(c) FROM show_chunks('conditions') c;

-- TEST Continuous Aggregate job
CREATE MATERIALIZED VIEW conditions_summary_daily
WITH (timescaledb.continuous) AS
SELECT location,
   time_bucket(INTERVAL '1 day', time) AS bucket,
   AVG(temperature),
   MAX(temperature),
   MIN(temperature)
FROM conditions
GROUP BY location, bucket
WITH NO DATA;

-- Refresh Continous Aggregate by Job
SELECT add_job('custom_proc5', '1h', config := '{"type":"procedure"}'::jsonb, initial_start := now()) AS job_id_5 \gset
SELECT wait_for_job_to_run(:job_id_5, 1);
SELECT count(*) FROM conditions_summary_daily;

-- Stop Background Workers
SELECT _timescaledb_internal.stop_background_workers();
