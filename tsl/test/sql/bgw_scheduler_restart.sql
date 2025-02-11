-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE VIEW tsdb_bgw AS
       SELECT datname, pid, backend_type, application_name
         FROM pg_stat_activity
        WHERE backend_type LIKE '%TimescaleDB%'
     ORDER BY datname, backend_type, application_name;

-- Wait for at least one background worker matching pattern to have
-- started.
CREATE PROCEDURE wait_for_some_started(
       min_time double precision,
       timeout double precision,
       pattern text
) AS $$
DECLARE
   backend_count int;
BEGIN
    FOR i IN 0..(timeout / min_time)::int
    LOOP
        PERFORM pg_sleep(min_time);
        SELECT count(*) INTO backend_count FROM tsdb_bgw WHERE backend_type LIKE pattern;
        IF backend_count > 0 THEN
            RETURN;
        END IF;
    END LOOP;
    RAISE EXCEPTION 'backend matching % did not start before timeout', pattern;
END;
$$ LANGUAGE plpgsql;

-- Wait for the number of background workers matching pattern to be
-- zero.
CREATE PROCEDURE wait_for_all_stopped(
       min_time double precision,
       timeout double precision,
       pattern text
) AS $$
DECLARE
   backend_count int;
BEGIN
    FOR i IN 0..(timeout / min_time)::int
    LOOP
        PERFORM pg_sleep(min_time);
        SELECT count(*) INTO backend_count FROM tsdb_bgw WHERE backend_type LIKE pattern;
        IF backend_count = 0 THEN
            RETURN;
        END IF;
    END LOOP;
    RAISE EXCEPTION 'backend matching % did not start before timeout', pattern;
END;
$$ LANGUAGE plpgsql;

CREATE PROCEDURE ts_terminate_launcher() AS $$
SELECT pg_terminate_backend(pid) FROM tsdb_bgw
 WHERE backend_type LIKE '%Launcher%';
$$ LANGUAGE SQL;

-- Show the default scheduler restart time and set it to a lower
-- value.
SHOW timescaledb.bgw_scheduler_restart_time;
ALTER SYSTEM SET timescaledb.bgw_scheduler_restart_time TO '10s';
ALTER SYSTEM SET timescaledb.debug_bgw_scheduler_exit_status TO 1;
SELECT pg_reload_conf();

-- Reconnect and check the restart time to make sure that it is
-- correct.
\c :TEST_DBNAME :ROLE_SUPERUSER
SHOW timescaledb.bgw_scheduler_restart_time;

-- Launcher is running, so we need to restart it for the scheduler
-- restart time to take effect.
SELECT datname, application_name FROM tsdb_bgw;
CALL ts_terminate_launcher();

-- It will restart automatically, but we wait for it to start.
CALL wait_for_some_started(1, 50, '%Launcher%');

-- Make sure that the new value of the scheduler restart time is
-- correct or the rest of the tests will fail.
SHOW timescaledb.bgw_scheduler_restart_time;

-- Verify that launcher is running. If it is not, the rest of the test
-- will fail.
SELECT datname, application_name FROM tsdb_bgw;

-- Now we can start the background workers.
SELECT _timescaledb_functions.start_background_workers();

-- They should start immediately, but let's wait for them to start.
CALL wait_for_some_started(1, 50, '%Scheduler%');

-- Check that the schedulers are running. If they are not, the rest of
-- the test is meaningless.
SELECT datname, application_name FROM tsdb_bgw;

-- Kill the schedulers and check that they restart.
SELECT pg_terminate_backend(pid) FROM tsdb_bgw
 WHERE datname = :'TEST_DBNAME' AND backend_type LIKE '%Scheduler%';

-- Wait for scheduler to exit, they should exit immediately.
CALL wait_for_all_stopped(1, 50, '%Scheduler%');

-- Check that the schedulers really exited.
SELECT datname, application_name FROM tsdb_bgw;

-- Wait for scheduler to restart.
CALL wait_for_some_started(10, 100, '%Scheduler%');

-- Make sure that the launcher and schedulers are running. Otherwise
-- the test will fail.
SELECT datname, application_name FROM tsdb_bgw;

-- Now, we had a previous bug where killing the launcher at this point
-- would leave the schedulers running (because the launcher did not
-- have a handle for them) and when launcher is restarting, it would
-- start more schedulers, leaving two schedulers per database.

-- Get the PID of the launcher to be able to compare it after the restart
SELECT pid AS orig_pid FROM tsdb_bgw WHERE backend_type LIKE '%Launcher%' \gset

-- Kill the launcher. Since there are new restarted schedulers, the
-- handle could not be used to terminate them, and they would be left
-- running.
CALL ts_terminate_launcher();

-- Launcher will restart immediately, but we wait one second to give
-- it a chance to start.
CALL wait_for_some_started(1, 50, '%Launcher%');

-- Check that the launcher is running and that there are exactly one
-- scheduler per database. Here the old schedulers are killed, so it
-- will be schedulers with a different PID than the ones before the
-- launcher was killed, but we are not showing this here.
SELECT (pid != :orig_pid) AS different_pid,
       datname,
       application_name
  FROM tsdb_bgw;

ALTER SYSTEM RESET timescaledb.bgw_scheduler_restart_time;
ALTER SYSTEM RESET timescaledb.debug_bgw_scheduler_exit_status;
SELECT pg_reload_conf();
SELECT _timescaledb_functions.stop_background_workers();

-- We need to restart the launcher as well to read the reset
-- configuration or it will affect other tests.
CALL ts_terminate_launcher();
