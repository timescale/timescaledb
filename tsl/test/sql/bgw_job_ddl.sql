-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test for DDL-like functionality

CREATE VIEW my_jobs AS
SELECT proc_schema, proc_name, owner
  FROM _timescaledb_config.bgw_job
 WHERE id >= 1000
ORDER BY proc_schema, proc_name, owner;

GRANT SELECT ON my_jobs TO PUBLIC;

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE OR REPLACE FUNCTION insert_job(
       application_name NAME,
       job_type NAME,
       schedule_interval INTERVAL,
       max_runtime INTERVAL,
       retry_period INTERVAL,
       owner regrole DEFAULT CURRENT_ROLE::regrole,
       scheduled BOOL DEFAULT true,
       fixed_schedule BOOL DEFAULT false
) RETURNS INT LANGUAGE SQL SECURITY DEFINER AS
$$
  INSERT INTO _timescaledb_config.bgw_job(application_name,schedule_interval,max_runtime,max_retries,
  retry_period,proc_name,proc_schema,owner,scheduled,fixed_schedule)
  VALUES($1,$3,$4,5,$5,$2,'public',$6,$7,$8) RETURNING id;
$$;

CREATE PROCEDURE more_magic(job_id INT, config jsonb) LANGUAGE plpgsql AS $$
BEGIN
  RAISE NOTICE 'done';
END;
$$;

CREATE PROCEDURE some_magic(job_id INT, config jsonb) LANGUAGE plpgsql AS $$
BEGIN
  RAISE NOTICE 'done';
END;
$$;

CREATE USER another_user;

SET ROLE another_user;
SELECT insert_job('one_job', 'some_magic', INTERVAL '100ms', INTERVAL '100s', INTERVAL '1s') AS job_id_2 \gset
SELECT insert_job('another_one', 'more_magic', INTERVAL '100ms', INTERVAL '100s', INTERVAL '1s') AS job_id \gset

SELECT * FROM my_jobs;

-- Test that reassigning to another user privileges does not work for
-- a normal user. We test both users with superuser privileges and
-- default permissions.
\set ON_ERROR_STOP 0
REASSIGN OWNED BY another_user TO :ROLE_SUPERUSER;
REASSIGN OWNED BY another_user TO :ROLE_DEFAULT_PERM_USER;
\set ON_ERROR_STOP 1

RESET ROLE;

-- Test that renaming a user changes keeps the job assigned to that user.
ALTER USER another_user RENAME TO renamed_user;
SELECT * FROM my_jobs;

-- Test that renaming the procedure also modifies the entry in the
-- jobs table.
ALTER PROCEDURE more_magic RENAME TO magic;
SELECT * FROM my_jobs;

-- Test that modifying the schema also modifies the entry in the jobs
-- table.
CREATE SCHEMA frugal;
ALTER PROCEDURE magic SET SCHEMA frugal;
ALTER PROCEDURE some_magic SET SCHEMA frugal;
SELECT * FROM my_jobs;

-- Test that renaming the schema will rename the procedure schema
START TRANSACTION;
ALTER SCHEMA frugal RENAME TO wicked;
SELECT * FROM my_jobs;
ROLLBACK;

\set VERBOSITY default
\set ON_ERROR_STOP 0

SELECT * FROM my_jobs;

-- Test that dropping a user owning a job fails.
DROP USER renamed_user;

-- Test that dropping the procedure fails since there is a background
-- job using it.
DROP PROCEDURE frugal.magic;

-- Test that re-assigning objects owned by an unknown user still fails
REASSIGN OWNED BY renamed_user, unknown_user TO :ROLE_DEFAULT_PERM_USER;

-- Test that dropping the schema without CASCADE will error out
DROP SCHEMA frugal;

\set ON_ERROR_STOP 1

-- Test that reassigning the owned job actually changes the owner of
-- the job.
START TRANSACTION;
REASSIGN OWNED BY renamed_user TO :ROLE_DEFAULT_PERM_USER;
SELECT * FROM my_jobs;
ROLLBACK;

-- Test that reassigning to postgres works
REASSIGN OWNED BY renamed_user TO :ROLE_SUPERUSER;
SELECT * FROM my_jobs;

-- Dropping the user now should work.
DROP USER renamed_user;

-- Dropping using Cascade should work and remove the background worker
-- entry as well.
START TRANSACTION;
SELECT * FROM my_jobs;
DROP PROCEDURE frugal.magic CASCADE;
SELECT * FROM my_jobs;
ROLLBACK;

DELETE FROM _timescaledb_config.bgw_job WHERE id = :job_id;

-- We should be able to drop the procedure without CASCADE now since
-- it is not used by any job.
DROP PROCEDURE frugal.magic;

-- We should be able to drop the schema with CASCADE despite
-- containing a procedure used by a background worker, but this should
-- remove the job from the background worker table.
SELECT * FROM my_jobs;
DROP SCHEMA frugal CASCADE;
SELECT * FROM my_jobs;
