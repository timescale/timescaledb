-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE VIEW tsdb_bgw AS
       SELECT datname, pid, backend_type, application_name
         FROM pg_stat_activity
        WHERE backend_type LIKE '%TimescaleDB%'
     ORDER BY datname, backend_type, application_name;

-- Show the default scheduler restart time
SHOW timescaledb.bgw_scheduler_restart_time;
ALTER SYSTEM SET timescaledb.bgw_scheduler_restart_time TO '10s';
ALTER SYSTEM SET timescaledb.debug_bgw_scheduler_exit_status TO 1;
SELECT pg_reload_conf();
SHOW timescaledb.bgw_scheduler_restart_time;

-- Launcher is running, so we need to restart it for the scheduler
-- restart time to take effect.
SELECT datname, application_name FROM tsdb_bgw;
SELECT pg_terminate_backend(pid) FROM tsdb_bgw
 WHERE datname = :'TEST_DBNAME' AND backend_type LIKE '%Launcher%';

-- It will restart automatically, but we wait one second to give it a
-- chance to start.
SELECT pg_sleep(1);

-- Verify that launcher is running. If it is not, the rest of the test
-- will fail.
SELECT datname, application_name FROM tsdb_bgw;

-- Now we can start the background workers.
SELECT _timescaledb_functions.start_background_workers();

-- They should start immediately, but let's wait a short while for
-- scheduler to start.
SELECT pg_sleep(1);

-- Check that the schedulers are running. If they are not, the rest of
-- the test is meaningless.
SELECT datname, application_name FROM tsdb_bgw;

-- Kill the schedulers and check that they restart.
SELECT pg_terminate_backend(pid) FROM tsdb_bgw
 WHERE datname = :'TEST_DBNAME' AND backend_type LIKE '%Scheduler%';

-- Wait for scheduler to exit, they should exit immediately.
SELECT pg_sleep(1);

-- Check that the schedulers really exited.
SELECT datname, application_name FROM tsdb_bgw;

SELECT pg_sleep(10);		-- Wait for scheduler to restart.

-- Make sure that the launcher and schedulers are running.
SELECT datname, application_name FROM tsdb_bgw;

-- Now, we had a previous bug where killing the launcher at this point
-- would leave the schedulers running (because the launcher did not
-- have a handle for them) and when launcher is restarting, it would
-- start more schedulers, leaving two schedulers per database.

-- Kill the launcher. Since there are new restarted schedulers, the
-- handle could not be used to terminate them, and they would be left
-- running.
SELECT pg_terminate_backend(pid) FROM tsdb_bgw
 WHERE datname = :'TEST_DBNAME' AND backend_type LIKE '%Launcher%';

-- Launcher will restart immediately, but we wait one second to give
-- it a chance to start.
SELECT pg_sleep(1);

-- Check that the launcher is running and that there are exactly one
-- scheduler per database. Here the old schedulers are killed, so it
-- will be schedulers with a different PID than the ones before the
-- launcher was killed, but we are not showing this here.
SELECT datname, application_name FROM tsdb_bgw;

ALTER SYSTEM RESET timescaledb.bgw_scheduler_restart_time;
ALTER SYSTEM RESET timescaledb.debug_bgw_scheduler_exit_status;
SELECT pg_reload_conf();

SELECT _timescaledb_functions.stop_background_workers();
