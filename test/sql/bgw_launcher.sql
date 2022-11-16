-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\set TEST_DBNAME_2 :TEST_DBNAME _2
\c :TEST_DBNAME :ROLE_SUPERUSER

-- start bgw since they are stopped for tests by default
SELECT _timescaledb_internal.start_background_workers();

CREATE DATABASE :TEST_DBNAME_2;

\c :TEST_DBNAME_2 :ROLE_SUPERUSER

\ir include/bgw_launcher_utils.sql

-- When we've connected to test db 2, we should be able to see the cluster launcher
-- and the scheduler for test db in pg_stat_activity
-- but test db 2 shouldn't have a scheduler because ext not created yet
SELECT wait_worker_counts(1,1,0,0);

-- Now create the extension in test db 2
SET client_min_messages = ERROR;
CREATE EXTENSION timescaledb CASCADE;
RESET client_min_messages;
SELECT wait_worker_counts(1,1,1,0);

DROP DATABASE :TEST_DBNAME;

-- Now the db_scheduler for test db should have disappeared
SELECT wait_worker_counts(1,0,1,0);

-- Now let's restart the scheduler in test db 2 and make sure our backend_start changed
SELECT backend_start as orig_backend_start
FROM pg_stat_activity
WHERE application_name = 'TimescaleDB Background Worker Scheduler'
AND datname = :'TEST_DBNAME_2' \gset
-- We'll do this in a txn so that we can see that the worker locks on our txn before continuing
BEGIN;
SELECT _timescaledb_internal.restart_background_workers();
SELECT wait_worker_counts(1,0,1,0);

SELECT (backend_start > :'orig_backend_start'::timestamptz) backend_start_changed,
(wait_event = 'virtualxid') wait_event_changed
FROM pg_stat_activity
WHERE application_name = 'TimescaleDB Background Worker Scheduler'
AND datname = :'TEST_DBNAME_2';
COMMIT;

SELECT wait_worker_counts(1,0,1,0);
SELECT (wait_event IS DISTINCT FROM 'virtualxid') wait_event_changed
FROM pg_stat_activity
WHERE application_name = 'TimescaleDB Background Worker Scheduler'
AND datname = :'TEST_DBNAME_2';

-- Test stop
SELECT _timescaledb_internal.stop_background_workers();
SELECT wait_worker_counts(1,0,0,0);

-- Make sure it doesn't break if we stop twice in a row
SELECT _timescaledb_internal.stop_background_workers();
SELECT wait_worker_counts(1,0,0,0);

-- test start
SELECT _timescaledb_internal.start_background_workers();
SELECT wait_worker_counts(1,0,1,0);

-- make sure start is idempotent
SELECT backend_start as orig_backend_start
FROM pg_stat_activity
WHERE application_name = 'TimescaleDB Background Worker Scheduler'
AND datname = :'TEST_DBNAME_2' \gset

-- Since we're doing idempotency tests, we're also going to exercise our queue and start 20 times
SELECT _timescaledb_internal.start_background_workers() as start_background_workers, * FROM generate_series(1,20);
-- Here we're waiting to see if something shows up in pg_stat_activity,
--  so we have to condition our loop in the opposite way. We'll only wait
--  half a second in total as well so that tests don't take too long.
CREATE FUNCTION wait_equals(TIMESTAMPTZ, TEXT) RETURNS BOOLEAN LANGUAGE PLPGSQL AS
$BODY$
DECLARE
r BOOLEAN;
BEGIN
FOR i in 1..5
LOOP
SELECT (backend_start = $1::timestamptz) backend_start_unchanged
FROM pg_stat_activity
WHERE application_name = 'TimescaleDB Background Worker Scheduler'
AND datname = $2 into r;
if(r) THEN
  PERFORM pg_sleep(0.1);
  PERFORM pg_stat_clear_snapshot();
ELSE
  RETURN FALSE;
END IF;
END LOOP;
RETURN TRUE;
END
$BODY$;
select wait_equals(:'orig_backend_start', :'TEST_DBNAME_2');

-- Make sure restart starts a worker even if it is stopped
SELECT _timescaledb_internal.stop_background_workers();
SELECT wait_worker_counts(1,0,0,0);
SELECT _timescaledb_internal.restart_background_workers();
SELECT wait_worker_counts(1,0,1,0);

-- Make sure drop extension statement restarts the worker and on rollback it keeps running
-- Now let's restart the scheduler and make sure our backend_start changed
SELECT backend_start as orig_backend_start
FROM pg_stat_activity
WHERE application_name = 'TimescaleDB Background Worker Scheduler'
AND datname = :'TEST_DBNAME_2' \gset

BEGIN;
DROP EXTENSION timescaledb;
SELECT wait_worker_counts(1,0,1,0);
ROLLBACK;

CREATE FUNCTION wait_greater(TIMESTAMPTZ,TEXT) RETURNS BOOLEAN LANGUAGE PLPGSQL AS
$BODY$
DECLARE
r BOOLEAN;
BEGIN
FOR i in 1..10
LOOP
SELECT (backend_start > $1::timestamptz) backend_start_changed
FROM pg_stat_activity
WHERE application_name = 'TimescaleDB Background Worker Scheduler'
AND datname = $2 into r;
if(NOT r) THEN
  PERFORM pg_sleep(0.1);
  PERFORM pg_stat_clear_snapshot();
ELSE
  RETURN TRUE;
END IF;
END LOOP;
RETURN FALSE;
END
$BODY$;
SELECT wait_greater(:'orig_backend_start',:'TEST_DBNAME_2');

-- Make sure canceling the launcher backend causes a restart of schedulers
SELECT backend_start as orig_backend_start
FROM pg_stat_activity
WHERE application_name = 'TimescaleDB Background Worker Scheduler'
AND datname = :'TEST_DBNAME_2' \gset

SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE application_name = 'TimescaleDB Background Worker Launcher';

SELECT wait_worker_counts(1,0,1,0);

SELECT wait_greater(:'orig_backend_start', :'TEST_DBNAME_2');

-- Make sure running pre_restore function stops background workers
SELECT timescaledb_pre_restore();
SELECT wait_worker_counts(1,0,0,0);
-- Make sure a restart with restoring on first starts the background worker
BEGIN;
SELECT _timescaledb_internal.restart_background_workers();
SELECT wait_worker_counts(1,0,1,0);
COMMIT;
-- Then the worker dies when it sees that restoring is on after the txn commits
SELECT wait_worker_counts(1,0,0,0);

--And post_restore starts them
BEGIN;
SELECT timescaledb_post_restore();
SELECT wait_worker_counts(1,0,1,0);
COMMIT;
-- And they stay started
SELECT wait_worker_counts(1,0,1,0);

-- Make sure dropping the extension means that the scheduler is stopped
BEGIN;
DROP EXTENSION timescaledb;
COMMIT;
SELECT wait_worker_counts(1,0,0,0);

-- Test that background workers are stopped with DROP OWNED
ALTER ROLE :ROLE_DEFAULT_PERM_USER WITH SUPERUSER;
\c :TEST_DBNAME_2 :ROLE_DEFAULT_PERM_USER
SET client_min_messages = ERROR;
CREATE EXTENSION timescaledb CASCADE;
RESET client_min_messages;
-- Make sure there is 1 launcher and 1 bgw in test db 2
SELECT wait_worker_counts(launcher_ct=>1, scheduler1_ct=> 0, scheduler2_ct=>1, template1_ct=>0);
-- drop a non-owner of the extension results in no change to worker counts
DROP OWNED BY :ROLE_DEFAULT_PERM_USER_2;
SELECT wait_worker_counts(launcher_ct=>1, scheduler1_ct=> 0, scheduler2_ct=>1, template1_ct=>0);
-- drop of owner of extension results in extension drop and a stop to the bgw
DROP OWNED BY :ROLE_DEFAULT_PERM_USER;
-- The worker in test db 2 is dead. Note that 0s are respected
SELECT wait_worker_counts(launcher_ct=>1, scheduler1_ct=>0, scheduler2_ct=>0, template1_ct=>0);
\c :TEST_DBNAME_2 :ROLE_SUPERUSER
ALTER ROLE :ROLE_DEFAULT_PERM_USER WITH NOSUPERUSER;

-- Connect to the template1 database
\c template1
\ir include/bgw_launcher_utils.sql
BEGIN;
-- Then create extension there in a txn and make sure we see a scheduler start

SET client_min_messages = ERROR;
CREATE EXTENSION timescaledb CASCADE;
RESET client_min_messages;
SELECT wait_worker_counts(1,0,0,1);
COMMIT;
-- End our transaction and it should immediately exit because it's a template database.
SELECT wait_worker_counts(1,0,0,0);
-- Clean up the template database, removing our test utilities etc
\ir include/bgw_launcher_utils_cleanup.sql

\c :TEST_DBNAME_2
-- Now try creating a DB from a template with the extension already installed.
-- Make sure we see a scheduler start.
CREATE DATABASE :TEST_DBNAME;
SELECT wait_worker_counts(1,1,0,0);
DROP DATABASE :TEST_DBNAME;
-- Now make sure that there's no race between create database and create extension.
-- Although to be honest, this race probably wouldn't manifest in this test.
\c template1
DROP EXTENSION timescaledb;
\c :TEST_DBNAME_2
CREATE DATABASE :TEST_DBNAME;
\c :TEST_DBNAME
SET client_min_messages = ERROR;
CREATE EXTENSION timescaledb;
RESET client_min_messages;
\c :TEST_DBNAME_2
SELECT wait_worker_counts(1,1,0,0);

-- test rename database
CREATE DATABASE db_rename_test;

\c db_rename_test :ROLE_SUPERUSER
SET client_min_messages=error;
CREATE EXTENSION timescaledb;
\c :TEST_DBNAME_2 :ROLE_SUPERUSER
SELECT wait_for_bgw_scheduler('db_rename_test');
ALTER DATABASE db_rename_test RENAME TO db_rename_test2;
DROP DATABASE db_rename_test2;

-- test create database with timescaledb database as template
SELECT wait_for_bgw_scheduler(:'TEST_DBNAME');
CREATE DATABASE db_from_template WITH TEMPLATE :TEST_DBNAME;
SELECT wait_for_bgw_scheduler(:'TEST_DBNAME');
DROP DATABASE db_from_template;

-- test alter database set tablespace
SET client_min_messages TO error;
DROP TABLESPACE IF EXISTS tablespace1;
RESET client_min_messages;
CREATE TABLESPACE tablespace1 OWNER :ROLE_DEFAULT_PERM_USER LOCATION :TEST_TABLESPACE1_PATH;

SELECT wait_for_bgw_scheduler(:'TEST_DBNAME');
ALTER DATABASE :TEST_DBNAME SET TABLESPACE tablespace1;

-- clean up additional database
\c :TEST_DBNAME :ROLE_SUPERUSER
DROP DATABASE :TEST_DBNAME_2;

