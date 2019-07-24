-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Need to be super user to create extension and add data nodes
\c :TEST_DBNAME :ROLE_SUPERUSER;

-- Support for execute_sql_and_filter_server_name_on_error()
\unset ECHO
\o /dev/null
\ir include/remote_exec.sql
\ir include/filter_exec.sql
\o
\set ECHO all

-- Cleanup from other potential tests that created these databases
SET client_min_messages TO ERROR;
DROP DATABASE IF EXISTS data_node_1;
DROP DATABASE IF EXISTS data_node_2;
DROP DATABASE IF EXISTS data_node_3;
SET client_min_messages TO NOTICE;

SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

SELECT * FROM add_data_node('data_node_1',
                            database => 'data_node_1',
                            password => :'ROLE_DEFAULT_CLUSTER_USER_PASS',
                            bootstrap_user => :'ROLE_CLUSTER_SUPERUSER',
                            bootstrap_password => :'ROLE_CLUSTER_SUPERUSER_PASS');
SELECT * FROM add_data_node('data_node_2',
                            database => 'data_node_2',
                            password => :'ROLE_DEFAULT_CLUSTER_USER_PASS',
                            bootstrap_user => :'ROLE_CLUSTER_SUPERUSER',
                            bootstrap_password => :'ROLE_CLUSTER_SUPERUSER_PASS');
SELECT * FROM add_data_node('data_node_3',
                            database => 'data_node_3',
                            password => :'ROLE_DEFAULT_CLUSTER_USER_PASS',
                            bootstrap_user => :'ROLE_CLUSTER_SUPERUSER',
                            bootstrap_password => :'ROLE_CLUSTER_SUPERUSER_PASS');

-- Import testsupport.sql file to data nodes
\unset ECHO
\o /dev/null
\c data_node_1
SET client_min_messages TO ERROR;
\ir :TEST_SUPPORT_FILE
\c data_node_2
SET client_min_messages TO ERROR;
\ir :TEST_SUPPORT_FILE
\c data_node_3
SET client_min_messages TO ERROR;
\ir :TEST_SUPPORT_FILE
\c :TEST_DBNAME :ROLE_SUPERUSER;
\o
SET client_min_messages TO NOTICE;
\set ECHO all

-- This SCHEMA will not be created on data nodes
CREATE SCHEMA disttable_schema AUTHORIZATION :ROLE_DEFAULT_CLUSTER_USER;
CREATE SCHEMA some_schema AUTHORIZATION :ROLE_DEFAULT_CLUSTER_USER;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

CREATE TABLE disttable(time timestamptz, device int, color int CONSTRAINT color_check CHECK (color > 0), temp float);
CREATE UNIQUE INDEX disttable_pk ON disttable(time);

-- CREATE TABLE
SELECT * FROM create_distributed_hypertable('disttable', 'time', replication_factor => 3);
SELECT * FROM test.show_columns('disttable');
SELECT * FROM test.show_constraints('disttable');
SELECT * FROM test.show_indexes('disttable');
SELECT * FROM test.show_triggers('disttable');

SELECT * FROM test.remote_exec(NULL, $$
SELECT * FROM test.show_columns('disttable');
SELECT * FROM test.show_constraints('disttable');
SELECT * FROM test.show_indexes('disttable');
SELECT * FROM test.show_triggers('disttable');
$$);

-- ADD CONSTRAINT
ALTER TABLE disttable ADD CONSTRAINT device_check CHECK (device > 0);
SELECT * FROM test.show_constraints('disttable');
SELECT * FROM test.remote_exec(NULL, $$ SELECT * FROM test.show_constraints('disttable') $$);

-- DROP CONSTRAINT
ALTER TABLE disttable DROP CONSTRAINT device_check;
SELECT * FROM test.show_constraints('disttable');
SELECT * FROM test.remote_exec(NULL, $$ SELECT * FROM test.show_constraints('disttable') $$);

-- DROP CONSTRAINT pre-created
ALTER TABLE disttable DROP CONSTRAINT color_check;
SELECT * FROM test.remote_exec(NULL, $$ SELECT * FROM test.show_constraints('disttable') $$);

-- DROP COLUMN
ALTER TABLE disttable DROP COLUMN color;
SELECT * FROM test.remote_exec(NULL, $$ SELECT * FROM test.show_columns('disttable') $$);

-- ADD COLUMN
ALTER TABLE disttable ADD COLUMN description text;
SELECT * FROM test.remote_exec(NULL, $$ SELECT * FROM test.show_columns('disttable') $$);

-- CREATE INDEX
CREATE INDEX disttable_description_idx ON disttable (description);
SELECT * FROM test.remote_exec(NULL, $$ SELECT * FROM test.show_indexes('disttable') $$);

-- TRUNCATE
CREATE TABLE non_disttable1(time timestamptz);
CREATE TABLE non_disttable2(time timestamptz);
SELECT create_hypertable('non_disttable2', 'time');

-- Truncating two non-distribued hypertables should be OK.
TRUNCATE non_disttable1, non_disttable2;
-- Truncating one distributed hypertable should be OK
TRUNCATE disttable;

-- Test unsupported operations on distributed hypertable
\set ON_ERROR_STOP 0

-- Combining one distributed hypertable with any other tables should
-- be blocked since not all nodes might have all tables and we
-- currently don't rewrite the command.
TRUNCATE disttable, non_disttable1;
TRUNCATE disttable, non_disttable2;

CLUSTER disttable USING disttable_description_idx;
REINDEX TABLE disttable;

ALTER TABLE disttable ALTER COLUMN description TYPE INT;
ALTER TABLE disttable RENAME TO disttable2;
ALTER TABLE disttable RENAME COLUMN description TO descr;
ALTER TABLE disttable RENAME CONSTRAINT device_check TO device_chk;
ALTER INDEX disttable_description_idx RENAME to disttable_descr_idx;
ALTER TABLE disttable SET SCHEMA some_unexist_schema;
ALTER TABLE disttable SET SCHEMA some_schema;


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
SELECT * FROM test.remote_exec(NULL, $$ SELECT * FROM test.show_indexes('disttable') $$);

-- DROP TABLE
DROP TABLE disttable;
SELECT * FROM test.remote_exec(NULL, $$ SELECT 1 FROM pg_tables WHERE tablename = 'disttable' $$);

DROP TABLE non_disttable1;
DROP TABLE non_disttable2;

-- Test current SCHEMA limitations
-- CREATE TABLE should fail, since remote data nodes has no schema
\set ON_ERROR_STOP 0
CREATE TABLE disttable_schema.disttable(time timestamptz, device int, color int, temp float);
SELECT test.execute_sql_and_filter_data_node_name_on_error($$
SELECT * FROM create_hypertable('disttable_schema.disttable', 'time', replication_factor => 3)
$$);
SELECT * FROM test.remote_exec(NULL, $$ SELECT schemaname, tablename FROM pg_tables WHERE tablename = 'disttable' $$);

-- CREATE and DROP SCHEMA CASCADE
\c data_node_1
CREATE SCHEMA some_schema AUTHORIZATION :ROLE_DEFAULT_CLUSTER_USER;
\c data_node_2
CREATE SCHEMA some_schema AUTHORIZATION :ROLE_DEFAULT_CLUSTER_USER;
\c data_node_3
CREATE SCHEMA some_schema AUTHORIZATION :ROLE_DEFAULT_CLUSTER_USER;
\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

CREATE TABLE some_schema.some_dist_table(time timestamptz, device int, color int, temp float);
SELECT * FROM create_hypertable('some_schema.some_dist_table', 'time', replication_factor => 3);
SELECT * FROM test.remote_exec(NULL, $$ SELECT schemaname, tablename FROM pg_tables WHERE tablename = 'some_dist_table' $$);
DROP SCHEMA some_schema CASCADE;
SELECT * FROM test.remote_exec(NULL, $$ SELECT schemaname, tablename FROM pg_tables WHERE tablename = 'some_dist_table' $$);

-- DROP column cascades to index drop
CREATE TABLE some_dist_table(time timestamptz, device int, color int, temp float);
SELECT * FROM create_hypertable('some_dist_table', 'time', replication_factor => 3);
CREATE INDEX some_dist_device_idx ON some_dist_table (device);
SELECT * FROM test.remote_exec(NULL, $$ SELECT * FROM test.show_indexes('some_dist_table') $$);
ALTER TABLE some_dist_table DROP COLUMN device;
SELECT * FROM test.remote_exec(NULL, $$ SELECT * FROM test.show_indexes('some_dist_table') $$);
DROP TABLE some_dist_table;

-- Creation of foreign key on distributed hypertable table will lead
-- to error, since non_htable is local
CREATE TABLE non_htable (id int PRIMARY KEY);
CREATE TABLE some_dist_table(time timestamptz, device int REFERENCES non_htable(id));
\set ON_ERROR_STOP 0
SELECT test.execute_sql_and_filter_data_node_name_on_error($$
SELECT * FROM create_hypertable('some_dist_table', 'time', replication_factor => 3);
$$);
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
SELECT * FROM test.remote_exec(NULL, $$ SELECT * FROM test.show_indexes('some_dist_table') $$);
DROP TABLE some_dist_table;

-- BEGIN/ROLLBACK
CREATE TABLE some_dist_table(time timestamptz, device int);
SELECT * FROM create_hypertable('some_dist_table', 'time', replication_factor => 3);
BEGIN;
CREATE INDEX some_dist_device_idx ON some_dist_table (device);
ROLLBACK;
SELECT * FROM test.show_indexes('some_dist_table');
SELECT * FROM test.remote_exec(NULL, $$ SELECT * FROM test.show_indexes('some_dist_table') $$);
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
SELECT * FROM test.remote_exec(NULL, $$
SELECT * FROM test.show_indexes('some_dist_table');
SELECT * FROM test.show_constraints('some_dist_table');
$$);
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
SELECT * FROM test.remote_exec(NULL, $$
SELECT * FROM test.show_indexes('some_dist_table');
SELECT * FROM test.show_constraints('some_dist_table');
$$);
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
SELECT * FROM test.remote_exec(NULL, $$
SELECT * FROM test.show_indexes('some_dist_table');
SELECT * FROM test.show_constraints('some_dist_table');
$$);
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
SELECT * FROM test.remote_exec(NULL, $$
SELECT * FROM test.show_indexes('some_dist_table');
SELECT * FROM test.show_constraints('some_dist_table');
$$);
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
ROLLBACK;
SELECT * FROM test.show_indexes('some_dist_table');
SELECT * FROM test.show_constraints('some_dist_table');
SELECT * FROM test.remote_exec(NULL, $$
SELECT * FROM test.show_indexes('some_dist_table');
SELECT * FROM test.show_constraints('some_dist_table');
$$);
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
ROLLBACK;
SELECT * FROM test.show_indexes('some_dist_table');
SELECT * FROM test.show_constraints('some_dist_table');
SELECT * FROM test.remote_exec(NULL, $$
SELECT * FROM test.show_indexes('some_dist_table');
SELECT * FROM test.show_constraints('some_dist_table');
$$);
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
ROLLBACK;
SELECT * FROM test.show_indexes('some_dist_table');
SELECT * FROM test.show_constraints('some_dist_table');
SELECT * FROM test.remote_exec(NULL, $$
SELECT * FROM test.show_indexes('some_dist_table');
SELECT * FROM test.show_constraints('some_dist_table');
$$);
DROP TABLE some_dist_table;

-- Test chunks updates
CREATE TABLE disttable(time timestamptz, device int, color int CONSTRAINT color_check CHECK (color > 0), temp float);
CREATE UNIQUE INDEX disttable_pk ON disttable(time);
SELECT * FROM create_hypertable('disttable', 'time', replication_factor => 3);

INSERT INTO disttable VALUES ('2017-01-01 06:01', 0, 1, 0.0);
SELECT show_chunks('disttable');
SELECT * FROM test.show_constraints('disttable');
SELECT * FROM test.show_constraints('_timescaledb_internal._hyper_16_1_dist_chunk');
ALTER TABLE disttable DROP CONSTRAINT color_check;
SELECT * FROM test.show_constraints('disttable');
SELECT * FROM test.show_constraints('_timescaledb_internal._hyper_16_1_dist_chunk');
SELECT * FROM test.remote_exec(NULL, $$
SELECT show_chunks('disttable');
SELECT * FROM test.show_constraints('disttable');
SELECT * FROM test.show_constraints('_timescaledb_internal._hyper_16_1_dist_chunk');
$$);
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

-- Test DDL blocking from non-frontend session
--
-- We test only special corner cases since most of this functionality already
-- been tested before.
--
CREATE TABLE disttable(time timestamptz, device int);
SELECT * FROM create_hypertable('disttable', 'time', replication_factor => 3);
CREATE INDEX disttable_device_idx ON disttable (device);

\c data_node_1
SELECT schemaname, tablename FROM pg_tables WHERE tablename = 'disttable';
SELECT * FROM test.show_indexes('disttable');

\set ON_ERROR_STOP 0

-- Test TRUNCATE blocked on data node
TRUNCATE disttable;

-- Test ALTER by non-frontend session
ALTER TABLE disttable ADD CONSTRAINT device_check CHECK (device > 0);

-- Test path for delayed relid resolving
ALTER TABLE disttable RENAME TO disttable2;

-- Test for hypertables collected during drop
DROP INDEX disttable_device_idx;
DROP TABLE disttable;

\set ON_ERROR_STOP 1

-- Explicitly allow execution
SET timescaledb.enable_client_ddl_on_data_nodes TO true;
DROP INDEX disttable_device_idx;
SELECT * FROM test.show_indexes('disttable');

\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

-- Should fail because of the inconsistency
\set ON_ERROR_STOP 0
DROP INDEX disttable_device_idx;
\set ON_ERROR_STOP 1

DROP TABLE disttable;

-- cleanup
\c :TEST_DBNAME :ROLE_SUPERUSER;
DROP SCHEMA disttable_schema CASCADE;
DROP DATABASE data_node_1;
DROP DATABASE data_node_2;
DROP DATABASE data_node_3;
