-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE TABLE drop_test(time timestamp, temp float8, device text);

SELECT create_hypertable('drop_test', 'time', 'device', 2);
SELECT id, schema_name, table_name, associated_schema_name, associated_table_prefix, num_dimensions, chunk_sizing_func_schema, chunk_sizing_func_name, compression_state, status FROM _timescaledb_catalog.hypertable;
INSERT INTO drop_test VALUES('Mon Mar 20 09:17:00.936242 2017', 23.4, 'dev1');
SELECT * FROM drop_test;

\c :TEST_DBNAME :ROLE_SUPERUSER
DROP EXTENSION timescaledb CASCADE;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

-- Querying the original table should not return any rows since all of
-- them actually existed in chunks that are now gone
SELECT * FROM drop_test;

\c :TEST_DBNAME :ROLE_SUPERUSER
-- Recreate the extension
SET client_min_messages=error;
CREATE EXTENSION timescaledb;
RESET client_min_messages;

-- Test that calling twice generates proper error
\set ON_ERROR_STOP 0
CREATE EXTENSION timescaledb;
\set ON_ERROR_STOP 1
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

-- CREATE twice with IF NOT EXISTS should be OK
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Make the table a hypertable again
SELECT create_hypertable('drop_test', 'time', 'device', 2);

SELECT id, schema_name, table_name, associated_schema_name, associated_table_prefix, num_dimensions, chunk_sizing_func_schema, chunk_sizing_func_name, compression_state, status FROM _timescaledb_catalog.hypertable;
INSERT INTO drop_test VALUES('Mon Mar 20 09:18:19.100462 2017', 22.1, 'dev1');
SELECT * FROM drop_test;

--test drops thru cascades of other objects
\c :TEST_DBNAME :ROLE_SUPERUSER
-- Stop background workers to prevent them from interfering with the drop public schema
SELECT _timescaledb_functions.stop_background_workers();
SET client_min_messages TO ERROR;
REVOKE CONNECT ON DATABASE :TEST_DBNAME FROM public;
SELECT count(pg_terminate_backend(pg_stat_activity.pid)) AS TERMINATED
FROM pg_stat_activity
WHERE pg_stat_activity.datname = :'TEST_DBNAME'
AND pg_stat_activity.pid <> pg_backend_pid() \gset
RESET client_min_messages;
-- drop the public schema and all its objects
DROP SCHEMA public CASCADE;
\dn

-- Recreate the public schema and extension in the same session.
-- This should work without requiring a reconnect (issue #5884).
CREATE SCHEMA public;
SET client_min_messages=error;
CREATE EXTENSION timescaledb SCHEMA public;
RESET client_min_messages;
SELECT extname FROM pg_extension WHERE extname = 'timescaledb';

-- Verify the extension is functional after re-creation
CREATE TABLE drop_test2(time timestamptz, temp float8);
SELECT create_hypertable('drop_test2', 'time');
INSERT INTO drop_test2 VALUES('2024-01-01', 23.4);
SELECT * FROM drop_test2;
DROP TABLE drop_test2;

-- Test that dropping and recreating extension directly also works in the same session
DROP EXTENSION timescaledb CASCADE;
SET client_min_messages=error;
CREATE EXTENSION timescaledb;
RESET client_min_messages;
SELECT extname FROM pg_extension WHERE extname = 'timescaledb';

-- A logical replication slot with pending (undecoded) changes to the
-- TimescaleDB catalog tables (hypertable, chunk) must block both DROP EXTENSION
-- (process utility hook) and the ensure_catalog_replication() check,
-- since dropping those user_catalog_tables would make the slot undecodable.
-- The error detail prints non-deterministic transaction ids, so only show the
-- primary error message.
\set VERBOSITY terse

-- A hypertable and chunk created before any logical replication slot
CREATE TABLE metrics(time timestamptz NOT NULL, value float8);
SELECT create_hypertable('metrics', 'time');
INSERT INTO metrics VALUES ('2024-01-01', 1.0);

SELECT slot_name FROM pg_create_logical_replication_slot('test_slot', 'test_decoding');

-- No pending catalog changes yet in the replication slot, the check should pass
SELECT _timescaledb_functions.ensure_catalog_replication('_timescaledb_catalog.hypertable');
SELECT _timescaledb_functions.ensure_catalog_replication('_timescaledb_catalog.chunk');

-- Modify the hypertable catalog table after the slot was created.
CREATE TABLE metrics2(time timestamptz NOT NULL, value float8);
SELECT create_hypertable('metrics2', 'time');

-- The slot now has pending changes against the hypertable table: both the SQL
-- function and DROP EXTENSION must error. The chunk table is untouched since
-- slot creation and still validates.
\set ON_ERROR_STOP 0
SELECT _timescaledb_functions.ensure_catalog_replication('_timescaledb_catalog.hypertable');
DROP EXTENSION timescaledb CASCADE;
\set ON_ERROR_STOP 1
SELECT _timescaledb_functions.ensure_catalog_replication('_timescaledb_catalog.chunk');

-- Insert a new chunk -- now the check should fail on the `chunk` catalog
\set ON_ERROR_STOP 0
INSERT INTO metrics2 VALUES ('2024-01-01', 1.0);
SELECT _timescaledb_functions.ensure_catalog_replication('_timescaledb_catalog.chunk');
\set ON_ERROR_STOP 1

-- A trick to immediately advance the slot's `catalog_xmin`. `catalog_xmin`
-- advances when the slot sees an `xl_running_xacts` record which is normally
-- generated automatically, e.g. by the `bgwriter`; here we force it using
-- `pg_log_standby_snapshot()`
BEGIN;
SELECT txid_current() \gset
SELECT pg_log_standby_snapshot() \gset
COMMIT;
SELECT count(*) FROM pg_logical_slot_get_changes('test_slot', NULL, NULL) \gset

-- The block is now lifted: validation passes for both tables and the extension
-- can be dropped even though the slot still exists.
SELECT _timescaledb_functions.ensure_catalog_replication('_timescaledb_catalog.hypertable');
SELECT _timescaledb_functions.ensure_catalog_replication('_timescaledb_catalog.chunk');
SET client_min_messages=error;
DROP EXTENSION timescaledb CASCADE;
RESET client_min_messages;
SELECT extname FROM pg_extension WHERE extname = 'timescaledb';

-- Clean up the slot so the test database can be dropped.
SELECT pg_drop_replication_slot('test_slot');
\set VERBOSITY default
