-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

-- Table to update concurrently to generate error message
CREATE TABLE my_table (a int, b int);
INSERT INTO my_table VALUES (0, 0);
GRANT ALL ON my_table TO PUBLIC;
ALTER SYSTEM SET DEFAULT_TRANSACTION_ISOLATION TO 'serializable';
SELECT pg_reload_conf();

SET ROLE :ROLE_DEFAULT_PERM_USER;

CREATE OR REPLACE PROCEDURE job_fail(jobid int, config jsonb)
AS $$
BEGIN
    RAISE EXCEPTION 'raising an exception';
END
$$ LANGUAGE plpgsql;

SELECT add_job('job_fail', '4 minutes', initial_start => now()) as job_fail_id \gset

CREATE OR REPLACE PROCEDURE custom_proc1(jobid int, config jsonb) LANGUAGE PLPGSQL AS
$$
BEGIN
  UPDATE my_table SET b = 1 WHERE a = 0;
  PERFORM pg_sleep(10);
  COMMIT;
END
$$;

SELECT add_job('custom_proc1', '2 min', initial_start => now()) as custom_proc1_id \gset

SET ROLE :ROLE_DEFAULT_PERM_USER_2;

CREATE OR REPLACE PROCEDURE custom_proc2(jobid int, config jsonb) LANGUAGE PLPGSQL AS
$$
BEGIN
  UPDATE my_table SET b = 2 WHERE a = 0;
  PERFORM pg_sleep(10);
  COMMIT;
END
$$;

-- to make sure custom_log is first updated by custom_proc_1
select add_job('custom_proc2', '2 min', initial_start => now() + interval '5 seconds') as custom_proc2_id \gset

SET ROLE :ROLE_SUPERUSER;
SELECT _timescaledb_internal.start_background_workers();
SELECT pg_sleep(20);

\d timescaledb_information.job_errors

-- We add a few entries without a matching job id, so that we get a
-- null owner. Note that the second entry does not have a message
-- defined, so it will print a standardized message assuming that the
-- job crashed.
\set start '2000-01-01 00:00:00+00'
\set finish '2000-01-01 00:00:10+00'
INSERT INTO _timescaledb_internal.job_errors(job_id, pid, start_time, finish_time, error_data) VALUES
       (11111, 12345, :'start'::timestamptz, :'finish'::timestamptz, '{"message": "not an error"}'),
       (22222, 45678, :'start'::timestamptz, :'finish'::timestamptz, '{}');

-- We check the log as different users and should only see what we
-- have permissions to see. We only bother about jobs at 1000 or
-- larger since the standard jobs are flaky.
SET ROLE :ROLE_DEFAULT_PERM_USER;
SELECT job_id, proc_schema, proc_name, sqlerrcode, err_message
FROM timescaledb_information.job_errors WHERE job_id >= 1000;

SET ROLE :ROLE_DEFAULT_PERM_USER_2;
SELECT job_id, proc_schema, proc_name, sqlerrcode, err_message
FROM timescaledb_information.job_errors WHERE job_id >= 1000;

SET ROLE :ROLE_SUPERUSER;
SELECT job_id, proc_schema, proc_name, sqlerrcode, err_message
FROM timescaledb_information.job_errors WHERE job_id >= 1000;

SELECT _timescaledb_internal.stop_background_workers();

SELECT delete_job(:custom_proc2_id);
SELECT delete_job(:custom_proc1_id);
SELECT delete_job(:job_fail_id);
