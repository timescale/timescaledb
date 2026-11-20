-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Jobs in timescaledb_information.jobs, including their config, must
-- only be visible to members of the job owner role or the database
-- owner role.

\c :TEST_DBNAME :ROLE_SUPERUSER

SET ROLE :ROLE_DEFAULT_PERM_USER;
CREATE PROCEDURE owner_action(job_id int, config jsonb)
LANGUAGE plpgsql AS $$ BEGIN END $$;
SELECT add_job('owner_action', '1h', scheduled => false,
               config => '{"api_key": "SECRET-xyz"}') AS job_id \gset
RESET ROLE;

-- Test 1: the job owner sees the job and its config.
SET ROLE :ROLE_DEFAULT_PERM_USER;
SELECT job_id = :job_id AS is_job, owner, config
FROM timescaledb_information.jobs WHERE proc_name = 'owner_action';
RESET ROLE;

-- Test 2: an unrelated user does not see the job.
SET ROLE :ROLE_DEFAULT_PERM_USER_2;
SELECT job_id, owner, config
FROM timescaledb_information.jobs WHERE proc_name = 'owner_action';
RESET ROLE;

-- Test 3: a function in the caller's WHERE clause must not see the
-- config of hidden jobs, so no NOTICE must be raised.
SET ROLE :ROLE_DEFAULT_PERM_USER_2;
CREATE FUNCTION leak(config jsonb) RETURNS boolean
LANGUAGE plpgsql COST 0.0000001 AS $$
BEGIN
  RAISE NOTICE 'leaked config: %', config;
  RETURN true;
END
$$;
SELECT count(*) FROM timescaledb_information.jobs WHERE leak(config);
DROP FUNCTION leak(jsonb);
RESET ROLE;

-- Test 4: a member of the job owner role sees the job and its config.
GRANT :ROLE_DEFAULT_PERM_USER TO :ROLE_DEFAULT_PERM_USER_2;
SET ROLE :ROLE_DEFAULT_PERM_USER_2;
SELECT config FROM timescaledb_information.jobs WHERE proc_name = 'owner_action';
RESET ROLE;
REVOKE :ROLE_DEFAULT_PERM_USER FROM :ROLE_DEFAULT_PERM_USER_2;

-- Test 5: the database owner sees the job and its config.
ALTER DATABASE :TEST_DBNAME OWNER TO :ROLE_DEFAULT_PERM_USER_2;
SET ROLE :ROLE_DEFAULT_PERM_USER_2;
SELECT config FROM timescaledb_information.jobs WHERE proc_name = 'owner_action';
RESET ROLE;
ALTER DATABASE :TEST_DBNAME OWNER TO :ROLE_SUPERUSER;

-- Test 6: a superuser sees the job and its config.
SELECT config FROM timescaledb_information.jobs WHERE proc_name = 'owner_action';

SELECT delete_job(:job_id);
DROP PROCEDURE owner_action;
