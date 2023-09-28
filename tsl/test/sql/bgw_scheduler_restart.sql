-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE VIEW tsdb_bgw AS
       SELECT datname, application_name FROM pg_stat_activity
       WHERE application_name LIKE 'TimescaleDB%'
       ORDER BY datname, application_name;

SHOW timescaledb.bgw_scheduler_restart_time;

SELECT _timescaledb_functions.start_background_workers();

SELECT pg_sleep(10);		-- Wait for scheduler to start.

SELECT * FROM tsdb_bgw;

ALTER SYSTEM SET timescaledb.shutdown_bgw_scheduler TO 'on';
ALTER SYSTEM SET timescaledb.shutdown_bgw_scheduler_exit_code TO 1;
SELECT pg_reload_conf();

SELECT pg_sleep(20);		-- Wait for scheduler to exit.

SELECT * FROM tsdb_bgw;

ALTER SYSTEM RESET timescaledb.shutdown_bgw_scheduler;
ALTER SYSTEM RESET timescaledb.shutdown_bgw_scheduler_exit_code;
SELECT pg_reload_conf();

SELECT pg_sleep(30);		-- Wait for scheduler to restart.

SELECT * FROM tsdb_bgw;

SELECT pg_terminate_backend(pid)
  FROM pg_stat_activity
 WHERE datname = :'TEST_DBNAME'
   AND application_name LIKE 'TimescaleDB%';
