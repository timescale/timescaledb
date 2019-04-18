-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Need to be super user to create extension and add servers
\c :TEST_DBNAME :ROLE_SUPERUSER;
ALTER ROLE :ROLE_DEFAULT_CLUSTER_USER CREATEDB PASSWORD 'pass';
GRANT USAGE ON FOREIGN DATA WRAPPER timescaledb_fdw TO :ROLE_DEFAULT_CLUSTER_USER;

-- Cleanup from other potential tests that created these databases
SET client_min_messages TO ERROR;
DROP DATABASE IF EXISTS server_1;
DROP DATABASE IF EXISTS server_2;
DROP DATABASE IF EXISTS server_3;
SET client_min_messages TO NOTICE;

-- Add test servers
SELECT * FROM add_server('server_1', database => 'server_1', local_user => :'ROLE_DEFAULT_CLUSTER_USER', remote_user => :'ROLE_DEFAULT_CLUSTER_USER', password => 'pass', bootstrap_user => :'ROLE_SUPERUSER');
SELECT * FROM add_server('server_2', database => 'server_2', local_user => :'ROLE_DEFAULT_CLUSTER_USER', remote_user => :'ROLE_DEFAULT_CLUSTER_USER', password => 'pass', bootstrap_user => :'ROLE_SUPERUSER');
SELECT * FROM add_server('server_3', database => 'server_3', local_user => :'ROLE_DEFAULT_CLUSTER_USER', remote_user => :'ROLE_DEFAULT_CLUSTER_USER', password => 'pass', bootstrap_user => :'ROLE_SUPERUSER');

-- Import testsupport.sql file to servers
\unset ECHO
\o /dev/null
\c server_1
\ir :TEST_SUPPORT_FILE
\c server_2
\ir :TEST_SUPPORT_FILE
\c server_3
\ir :TEST_SUPPORT_FILE
\c :TEST_DBNAME :ROLE_SUPERUSER;
\o
\set ECHO all

-- This SCHEMA will not be created on servers
CREATE SCHEMA disttable_schema AUTHORIZATION :ROLE_DEFAULT_CLUSTER_USER;
CREATE SCHEMA some_schema AUTHORIZATION :ROLE_DEFAULT_CLUSTER_USER;

SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

CREATE TABLE disttable(time timestamptz, device int, color int CONSTRAINT color_check CHECK (color > 0), temp float);
CREATE UNIQUE INDEX disttable_pk ON disttable(time);

-- CREATE TABLE
SELECT * FROM create_hypertable('disttable', 'time', replication_factor => 3);
SELECT * FROM test.show_columns('disttable');
SELECT * FROM test.show_constraints('disttable');
SELECT * FROM test.show_indexes('disttable');
SELECT * FROM test.show_triggers('disttable');
\c server_1
SELECT * FROM test.show_columns('disttable');
SELECT * FROM test.show_constraints('disttable');
SELECT * FROM test.show_indexes('disttable');
SELECT * FROM test.show_triggers('disttable');
\c server_2
SELECT * FROM test.show_columns('disttable');
SELECT * FROM test.show_constraints('disttable');
SELECT * FROM test.show_indexes('disttable');
SELECT * FROM test.show_triggers('disttable');
\c server_3
SELECT * FROM test.show_columns('disttable');
SELECT * FROM test.show_constraints('disttable');
SELECT * FROM test.show_indexes('disttable');
SELECT * FROM test.show_triggers('disttable');
\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

-- ADD CONSTRAINT
ALTER TABLE disttable ADD CONSTRAINT device_check CHECK (device > 0);
SELECT * FROM test.show_constraints('disttable');
\c server_1
SELECT * FROM test.show_constraints('disttable');
\c server_2
SELECT * FROM test.show_constraints('disttable');
\c server_3
SELECT * FROM test.show_constraints('disttable');
\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

-- DROP CONSTRAINT
ALTER TABLE disttable DROP CONSTRAINT device_check;
SELECT * FROM test.show_constraints('disttable');
\c server_1
SELECT * FROM test.show_constraints('disttable');
\c server_2
SELECT * FROM test.show_constraints('disttable');
\c server_3
SELECT * FROM test.show_constraints('disttable');
\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

-- DROP CONSTRAINT pre-created
ALTER TABLE disttable DROP CONSTRAINT color_check;
SELECT * FROM test.show_constraints('disttable');
\c server_1
SELECT * FROM test.show_constraints('disttable');
\c server_2
SELECT * FROM test.show_constraints('disttable');
\c server_3
SELECT * FROM test.show_constraints('disttable');
\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

-- DROP COLUMN
ALTER TABLE disttable DROP COLUMN color;
SELECT * FROM test.show_columns('disttable');
\c server_1
SELECT * FROM test.show_columns('disttable');
\c server_2
SELECT * FROM test.show_columns('disttable');
\c server_3
SELECT * FROM test.show_columns('disttable');
\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

-- ADD COLUMN
ALTER TABLE disttable ADD COLUMN description text;
SELECT * FROM test.show_columns('disttable');
\c server_1
SELECT * FROM test.show_columns('disttable');
\c server_2
SELECT * FROM test.show_columns('disttable');
\c server_3
SELECT * FROM test.show_columns('disttable');
\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

-- CREATE INDEX
CREATE INDEX disttable_description_idx ON disttable (description);
SELECT * FROM test.show_indexes('disttable');
\c server_1
SELECT * FROM test.show_indexes('disttable');
\c server_2
SELECT * FROM test.show_indexes('disttable');
\c server_3
SELECT * FROM test.show_indexes('disttable');
\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

-- Test unsupported operations on distributed hypertable
\set ON_ERROR_STOP 0

CLUSTER disttable USING disttable_description_idx;
REINDEX TABLE disttable;
VACUUM disttable;
TRUNCATE disttable;
-- test block several hypertables
TRUNCATE disttable, disttable;

ALTER TABLE disttable ALTER COLUMN description TYPE INT;
ALTER TABLE disttable RENAME TO disttable2;
ALTER TABLE disttable RENAME COLUMN description TO descr;
ALTER TABLE disttable RENAME CONSTRAINT device_check TO device_chk;
ALTER INDEX disttable_description_idx RENAME to disttable_descr_idx;
ALTER TABLE disttable SET SCHEMA some_unexist_schema;
ALTER TABLE disttable SET SCHEMA some_schema;

CREATE TABLE non_disttable1(id int);
CREATE TABLE non_disttable2(id int);

DROP TABLE non_disttable1, disttable;
DROP TABLE disttable, non_disttable2;
DROP TABLE disttable, disttable; 

\set ON_ERROR_STOP 1

-- CREATE/DROP TRIGGER
CREATE OR REPLACE FUNCTION test_trigger()
RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
BEGIN
RETURN OLD;
END
$BODY$;

CREATE TRIGGER disttable_trigger_test
BEFORE INSERT ON disttable
FOR EACH ROW EXECUTE PROCEDURE test_trigger();

DROP TRIGGER disttable_trigger_test on disttable;
DROP FUNCTION test_trigger;

-- DROP INDEX
\set ON_ERROR_STOP 0
DROP INDEX disttable_description_idx, disttable_pk;
\set ON_ERROR_STOP 1

DROP INDEX disttable_description_idx;
DROP INDEX disttable_pk;
SELECT * FROM test.show_indexes('disttable');
\c server_1
SELECT * FROM test.show_indexes('disttable');
\c server_2
SELECT * FROM test.show_indexes('disttable');
\c server_3
SELECT * FROM test.show_indexes('disttable');
\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

-- DROP TABLE
DROP TABLE disttable;
\c server_1
SELECT 1 FROM pg_tables WHERE tablename = 'disttable';
\c server_2
SELECT 1 FROM pg_tables WHERE tablename = 'disttable';
\c server_3
SELECT 1 FROM pg_tables WHERE tablename = 'disttable';
\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

DROP TABLE non_disttable1;
DROP TABLE non_disttable2;

-- Test current SCHEMA limitations
-- CREATE TABLE should fail, since remote servers has no schema
\set ON_ERROR_STOP 0
CREATE TABLE disttable_schema.disttable(time timestamptz, device int, color int, temp float);
SELECT * FROM create_hypertable('disttable_schema.disttable', 'time', replication_factor => 3);
\c server_1
SELECT 1 FROM pg_tables WHERE tablename = 'disttable';
\c server_2
SELECT 1 FROM pg_tables WHERE tablename = 'disttable';
\c server_3
SELECT 1 FROM pg_tables WHERE tablename = 'disttable';
\c :TEST_DBNAME :ROLE_SUPERUSER;
\set ON_ERROR_STOP 1

-- CREATE and DROP SCHEMA CASCADE
\c server_1
CREATE SCHEMA some_schema AUTHORIZATION :ROLE_DEFAULT_CLUSTER_USER;
\c server_2
CREATE SCHEMA some_schema AUTHORIZATION :ROLE_DEFAULT_CLUSTER_USER;
\c server_3
CREATE SCHEMA some_schema AUTHORIZATION :ROLE_DEFAULT_CLUSTER_USER;
\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

CREATE TABLE some_schema.some_dist_table(time timestamptz, device int, color int, temp float);
SELECT * FROM create_hypertable('some_schema.some_dist_table', 'time', replication_factor => 3);

\c server_1
SELECT schemaname, tablename FROM pg_tables WHERE tablename = 'some_dist_table';
\c server_2
SELECT schemaname, tablename FROM pg_tables WHERE tablename = 'some_dist_table';
\c server_3
SELECT schemaname, tablename FROM pg_tables WHERE tablename = 'some_dist_table';
\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

DROP SCHEMA some_schema CASCADE;

\c server_1
SELECT schemaname, tablename FROM pg_tables WHERE tablename = 'some_dist_table';
\c server_2
SELECT schemaname, tablename FROM pg_tables WHERE tablename = 'some_dist_table';
\c server_3
SELECT schemaname, tablename FROM pg_tables WHERE tablename = 'some_dist_table';
\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

-- DROP column cascades to index drop
CREATE TABLE some_dist_table(time timestamptz, device int, color int, temp float);
SELECT * FROM create_hypertable('some_dist_table', 'time', replication_factor => 3);
CREATE INDEX some_dist_device_idx ON some_dist_table (device);
\c server_1
SELECT * FROM test.show_indexes('some_dist_table');
\c server_2
SELECT * FROM test.show_indexes('some_dist_table');
\c server_3
SELECT * FROM test.show_indexes('some_dist_table');
\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

ALTER TABLE some_dist_table DROP COLUMN device;

\c server_1
SELECT * FROM test.show_indexes('some_dist_table');
\c server_2
SELECT * FROM test.show_indexes('some_dist_table');
\c server_3
SELECT * FROM test.show_indexes('some_dist_table');
\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;
DROP TABLE some_dist_table;

-- Creation of foreign key on distributed hypertable table will lead
-- to error, since non_htable is local
CREATE TABLE non_htable (id int PRIMARY KEY);
CREATE TABLE some_dist_table(time timestamptz, device int REFERENCES non_htable(id));
\set ON_ERROR_STOP 0
SELECT * FROM create_hypertable('some_dist_table', 'time', replication_factor => 3);
\set ON_ERROR_STOP 1
DROP TABLE some_dist_table;
DROP TABLE non_htable;

-- Transactional DDL tests
-- Single-statement transactions

-- BEGIN/COMMIT
CREATE TABLE some_dist_table(time timestamptz, device int);
SELECT * FROM create_hypertable('some_dist_table', 'time', replication_factor => 3);
BEGIN;
CREATE INDEX some_dist_device_idx ON some_dist_table (device);
COMMIT;
SELECT * FROM test.show_indexes('some_dist_table');
\c server_1
SELECT * FROM test.show_indexes('some_dist_table');
\c server_2
SELECT * FROM test.show_indexes('some_dist_table');
\c server_3
SELECT * FROM test.show_indexes('some_dist_table');
\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;
DROP TABLE some_dist_table;

-- BEGIN/ROLLBACK
CREATE TABLE some_dist_table(time timestamptz, device int);
SELECT * FROM create_hypertable('some_dist_table', 'time', replication_factor => 3);
BEGIN;
CREATE INDEX some_dist_device_idx ON some_dist_table (device);
ROLLBACK;
SELECT * FROM test.show_indexes('some_dist_table');
\c server_1
SELECT * FROM test.show_indexes('some_dist_table');
\c server_2
SELECT * FROM test.show_indexes('some_dist_table');
\c server_3
SELECT * FROM test.show_indexes('some_dist_table');
\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;
DROP TABLE some_dist_table;

-- Multi-statement transactions

-- BEGIN/COMMIT
CREATE TABLE some_dist_table(time timestamptz, device int);
SELECT * FROM create_hypertable('some_dist_table', 'time', replication_factor => 3);
BEGIN;
CREATE INDEX some_dist_device_idx ON some_dist_table (device);
ALTER TABLE some_dist_table ADD CONSTRAINT device_check CHECK (device > 0);
COMMIT;
SELECT * FROM test.show_indexes('some_dist_table');
SELECT * FROM test.show_constraints('some_dist_table');
\c server_1
SELECT * FROM test.show_indexes('some_dist_table');
SELECT * FROM test.show_constraints('some_dist_table');
\c server_2
SELECT * FROM test.show_indexes('some_dist_table');
SELECT * FROM test.show_constraints('some_dist_table');
\c server_3
SELECT * FROM test.show_indexes('some_dist_table');
SELECT * FROM test.show_constraints('some_dist_table');
\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;
DROP TABLE some_dist_table;

-- BEGIN/ROLLBACK
CREATE TABLE some_dist_table(time timestamptz, device int);
SELECT * FROM create_hypertable('some_dist_table', 'time', replication_factor => 3);
BEGIN;
CREATE INDEX some_dist_device_idx ON some_dist_table (device);
ALTER TABLE some_dist_table ADD CONSTRAINT device_check CHECK (device > 0);
ROLLBACK;
SELECT * FROM test.show_indexes('some_dist_table');
SELECT * FROM test.show_constraints('some_dist_table');
\c server_1
SELECT * FROM test.show_indexes('some_dist_table');
SELECT * FROM test.show_constraints('some_dist_table');
\c server_2
SELECT * FROM test.show_indexes('some_dist_table');
SELECT * FROM test.show_constraints('some_dist_table');
\c server_3
SELECT * FROM test.show_indexes('some_dist_table');
SELECT * FROM test.show_constraints('some_dist_table');
\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;
DROP TABLE some_dist_table;

-- Nested transactions

-- BEGIN/BEGIN/COMMIT/COMMIT
CREATE TABLE some_dist_table(time timestamptz, device int);
SELECT * FROM create_hypertable('some_dist_table', 'time', replication_factor => 3);
BEGIN;
SAVEPOINT a;
CREATE INDEX some_dist_device_idx ON some_dist_table (device);
SAVEPOINT b;
ALTER TABLE some_dist_table ADD CONSTRAINT device_check CHECK (device > 0);
COMMIT;
SELECT * FROM test.show_indexes('some_dist_table');
SELECT * FROM test.show_constraints('some_dist_table');
\c server_1
SELECT * FROM test.show_indexes('some_dist_table');
SELECT * FROM test.show_constraints('some_dist_table');
\c server_2
SELECT * FROM test.show_indexes('some_dist_table');
SELECT * FROM test.show_constraints('some_dist_table');
\c server_3
SELECT * FROM test.show_indexes('some_dist_table');
SELECT * FROM test.show_constraints('some_dist_table');
\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;
DROP TABLE some_dist_table;

-- BEGIN/BEGIN/ROLLBACK/COMMIT
CREATE TABLE some_dist_table(time timestamptz, device int);
SELECT * FROM create_hypertable('some_dist_table', 'time', replication_factor => 3);
BEGIN;
SAVEPOINT a;
CREATE INDEX some_dist_device_idx ON some_dist_table (device);
SAVEPOINT b;
ALTER TABLE some_dist_table ADD CONSTRAINT device_check CHECK (device > 0);
ROLLBACK TO SAVEPOINT b;
COMMIT;
SELECT * FROM test.show_indexes('some_dist_table');
SELECT * FROM test.show_constraints('some_dist_table');
\c server_1
SELECT * FROM test.show_indexes('some_dist_table');
SELECT * FROM test.show_constraints('some_dist_table');
\c server_2
SELECT * FROM test.show_indexes('some_dist_table');
SELECT * FROM test.show_constraints('some_dist_table');
\c server_3
SELECT * FROM test.show_indexes('some_dist_table');
SELECT * FROM test.show_constraints('some_dist_table');
\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;
DROP TABLE some_dist_table;

-- BEGIN/BEGIN/COMMIT/ROLLBACK
CREATE TABLE some_dist_table(time timestamptz, device int);
SELECT * FROM create_hypertable('some_dist_table', 'time', replication_factor => 3);
BEGIN;
SAVEPOINT a;
CREATE INDEX some_dist_device_idx ON some_dist_table (device);
SAVEPOINT b;
ALTER TABLE some_dist_table ADD CONSTRAINT device_check CHECK (device > 0);
ROLLBACK TO SAVEPOINT a;
SELECT * FROM test.show_indexes('some_dist_table');
SELECT * FROM test.show_constraints('some_dist_table');
\c server_1
SELECT * FROM test.show_indexes('some_dist_table');
SELECT * FROM test.show_constraints('some_dist_table');
\c server_2
SELECT * FROM test.show_indexes('some_dist_table');
SELECT * FROM test.show_constraints('some_dist_table');
\c server_3
SELECT * FROM test.show_indexes('some_dist_table');
SELECT * FROM test.show_constraints('some_dist_table');
\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;
DROP TABLE some_dist_table;

-- BEGIN/BEGIN/ROLLBACK/ROLLBACK
CREATE TABLE some_dist_table(time timestamptz, device int);
SELECT * FROM create_hypertable('some_dist_table', 'time', replication_factor => 3);
BEGIN;
SAVEPOINT a;
CREATE INDEX some_dist_device_idx ON some_dist_table (device);
SAVEPOINT b;
ALTER TABLE some_dist_table ADD CONSTRAINT device_check CHECK (device > 0);
ROLLBACK TO SAVEPOINT b;
ROLLBACK TO SAVEPOINT a;
SELECT * FROM test.show_indexes('some_dist_table');
SELECT * FROM test.show_constraints('some_dist_table');
\c server_1
SELECT * FROM test.show_indexes('some_dist_table');
SELECT * FROM test.show_constraints('some_dist_table');
\c server_2
SELECT * FROM test.show_indexes('some_dist_table');
SELECT * FROM test.show_constraints('some_dist_table');
\c server_3
SELECT * FROM test.show_indexes('some_dist_table');
SELECT * FROM test.show_constraints('some_dist_table');
\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;
DROP TABLE some_dist_table;

-- BEGIN/BEGIN/ABORT/ROLLBACK
CREATE TABLE some_dist_table(time timestamptz, device int);
SELECT * FROM create_hypertable('some_dist_table', 'time', replication_factor => 3);
BEGIN;
SAVEPOINT a;
CREATE INDEX some_dist_device_idx ON some_dist_table (device);
SAVEPOINT b;
ALTER TABLE some_dist_table ADD CONSTRAINT device_check CHECK (device > 0);
\set ON_ERROR_STOP 0
ALTER TABLE some_dist_table ADD CONSTRAINT device_check CHECK (device > 0);
\set ON_ERROR_STOP 1
ROLLBACK TO SAVEPOINT b;
ROLLBACK TO SAVEPOINT a;
SELECT * FROM test.show_indexes('some_dist_table');
SELECT * FROM test.show_constraints('some_dist_table');
\c server_1
SELECT * FROM test.show_indexes('some_dist_table');
SELECT * FROM test.show_constraints('some_dist_table');
\c server_2
SELECT * FROM test.show_indexes('some_dist_table');
SELECT * FROM test.show_constraints('some_dist_table');
\c server_3
SELECT * FROM test.show_indexes('some_dist_table');
SELECT * FROM test.show_constraints('some_dist_table');
\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;
DROP TABLE some_dist_table;

-- Test chunks updates
CREATE TABLE disttable(time timestamptz, device int, color int CONSTRAINT color_check CHECK (color > 0), temp float);
CREATE UNIQUE INDEX disttable_pk ON disttable(time);
SELECT * FROM create_hypertable('disttable', 'time', replication_factor => 3);

INSERT INTO disttable VALUES ('2017-01-01 06:01', 0, 1, 0.0);
SELECT show_chunks('disttable');
SELECT * FROM test.show_constraints('disttable');
SELECT * FROM test.show_constraints('_timescaledb_internal._hyper_15_1_dist_chunk');

ALTER TABLE disttable DROP CONSTRAINT color_check;
SELECT * FROM test.show_constraints('disttable');
SELECT * FROM test.show_constraints('_timescaledb_internal._hyper_15_1_dist_chunk');

\c server_1
SELECT show_chunks('disttable');
SELECT * FROM test.show_constraints('disttable');
SELECT * FROM test.show_constraints('_timescaledb_internal._hyper_15_1_dist_chunk');
\c server_2
SELECT show_chunks('disttable');
SELECT * FROM test.show_constraints('disttable');
SELECT * FROM test.show_constraints('_timescaledb_internal._hyper_15_1_dist_chunk');
\c server_3
SELECT show_chunks('disttable');
SELECT * FROM test.show_constraints('disttable');
SELECT * FROM test.show_constraints('_timescaledb_internal._hyper_15_1_dist_chunk');
\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;
DROP TABLE disttable;

-- Test event triggers behaviour
CREATE OR REPLACE FUNCTION test_event_trigger_sql_drop_function() RETURNS event_trigger
LANGUAGE plpgsql AS $$
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS unexist_table';
END
$$;

\c :TEST_DBNAME :ROLE_SUPERUSER;

CREATE EVENT TRIGGER test_event_trigger_sqldrop ON sql_drop
	WHEN TAG IN ('drop table')
	EXECUTE PROCEDURE test_event_trigger_sql_drop_function();

SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

-- Test DROP inside event trigger on local table (should not crash)
CREATE TABLE non_htable (id int PRIMARY KEY);
DROP TABLE non_htable;

\c :TEST_DBNAME :ROLE_SUPERUSER;
DROP EVENT TRIGGER test_event_trigger_sqldrop;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

-- cleanup
\c :TEST_DBNAME :ROLE_SUPERUSER;
DROP SCHEMA disttable_schema CASCADE;
DROP DATABASE server_1;
DROP DATABASE server_2;
DROP DATABASE server_3;
