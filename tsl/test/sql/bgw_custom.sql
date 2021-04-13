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

SELECT * FROM timescaledb_information.jobs ORDER BY 1;

-- check for corrects counts in telemetry
SELECT json_object_field(get_telemetry_report(always_display_report := true)::json,'num_user_defined_actions');

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

SELECT delete_job(1000);
SELECT delete_job(1001);
SELECT delete_job(1002);
SELECT delete_job(1003);
SELECT delete_job(1004);

-- check jobs got removed
SELECT count(*) FROM timescaledb_information.jobs WHERE job_id >= 1000;

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
SELECT job_id FROM alter_job(1,scheduled:=false);
SELECT * FROM timescaledb_information.jobs WHERE job_id = 1;

-- test updating job settings
SELECT job_id FROM alter_job(1,config:='{"test":"test"}');
SELECT * FROM timescaledb_information.jobs WHERE job_id = 1;
SELECT job_id FROM alter_job(1,scheduled:=true);
SELECT * FROM timescaledb_information.jobs WHERE job_id = 1;
SELECT job_id FROM alter_job(1,scheduled:=false);
SELECT * FROM timescaledb_information.jobs WHERE job_id = 1;

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
