-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE TABLE drop_test(time timestamp, temp float8, device text);

SELECT create_hypertable('drop_test', 'time', 'device', 2);
SELECT * FROM _timescaledb_catalog.hypertable;
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

SELECT * FROM _timescaledb_catalog.hypertable;
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
