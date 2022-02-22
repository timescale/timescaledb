-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Need to be super user to create extension and add data nodes
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;

-- Support for execute_sql_and_filter_server_name_on_error()
\unset ECHO
\o /dev/null
\ir include/remote_exec.sql
\ir include/filter_exec.sql
\o
\set ECHO all

\set MY_DB1 :TEST_DBNAME _1
\set MY_DB2 :TEST_DBNAME _2
\set MY_DB3 :TEST_DBNAME _3

CREATE SCHEMA some_schema AUTHORIZATION :ROLE_1;

SELECT * FROM add_data_node('data_node_1', host => 'localhost', database => :'MY_DB1');
SELECT * FROM add_data_node('data_node_2', host => 'localhost', database => :'MY_DB2');
SELECT * FROM add_data_node('data_node_3', host => 'localhost', database => :'MY_DB3');
GRANT USAGE ON FOREIGN SERVER data_node_1, data_node_2, data_node_3 TO PUBLIC;

-- Presence of non-distributed hypertables on data nodes should not cause issues
CALL distributed_exec('CREATE TABLE local(time timestamptz, measure int)', '{ "data_node_1", "data_node_3" }');
CALL distributed_exec($$ SELECT create_hypertable('local', 'time') $$, '{ "data_node_1", "data_node_3" }');

-- Import testsupport.sql file to data nodes
\unset ECHO
\o /dev/null
\c :MY_DB1
SET client_min_messages TO ERROR;
\ir :TEST_SUPPORT_FILE
\c :MY_DB2
SET client_min_messages TO ERROR;
\ir :TEST_SUPPORT_FILE
\c :MY_DB3
SET client_min_messages TO ERROR;
\ir :TEST_SUPPORT_FILE
--\c :TEST_DBNAME :ROLE_SUPERUSER;
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
\o
SET client_min_messages TO NOTICE;
\set ECHO all

SET ROLE :ROLE_1;
CREATE TABLE disttable(time timestamptz, device int, color int CONSTRAINT color_check CHECK (color > 0), temp float);
CREATE UNIQUE INDEX disttable_pk ON disttable(time, temp);

-- CREATE TABLE
SELECT * FROM create_distributed_hypertable('disttable', 'time', 'temp', replication_factor => 3);
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

-- RENAME TO
ALTER TABLE disttable RENAME TO disttable2;
ALTER TABLE disttable2 RENAME TO disttable;

-- SET SCHEMA
ALTER TABLE disttable SET SCHEMA some_schema;
ALTER TABLE some_schema.disttable SET SCHEMA public;
\set ON_ERROR_STOP 0
ALTER TABLE disttable SET SCHEMA some_unexist_schema;
\set ON_ERROR_STOP 1

-- Test unsupported operations on distributed hypertable
\set ON_ERROR_STOP 0

-- test set_replication_factor on non-hypertable
SELECT * FROM set_replication_factor('non_disttable1', 1);
-- test set_replication_factor on non-distributed
SELECT * FROM set_replication_factor('non_disttable2', 1);
-- test set_replication_factor on NULL hypertable
SELECT * FROM set_replication_factor(NULL, 1);

-- Combining one distributed hypertable with any other tables should
-- be blocked since not all nodes might have all tables and we
-- currently don't rewrite the command.
TRUNCATE disttable, non_disttable1;
TRUNCATE disttable, non_disttable2;

CLUSTER disttable USING disttable_description_idx;

DROP TABLE non_disttable1, disttable;
DROP TABLE disttable, non_disttable2;
DROP TABLE disttable, disttable;

\set ON_ERROR_STOP 1

----------------------------------------------------------------------------------------
-- Test column type change, renaming columns, constraints, indexes, and REINDEX command.
----------------------------------------------------------------------------------------
ALTER TABLE disttable ALTER COLUMN description TYPE VARCHAR(10);

ALTER TABLE disttable ADD COLUMN float_col float;
ALTER TABLE disttable ALTER COLUMN float_col TYPE INT USING float_col::int;

\set ON_ERROR_STOP 0
-- Changing the type of a hash-partitioned column should not be supported
ALTER TABLE disttable ALTER COLUMN temp TYPE numeric;
\set ON_ERROR_STOP 1

-- Should be able to change if not hash partitioned though
ALTER TABLE disttable ALTER COLUMN time TYPE timestamp;

INSERT INTO disttable VALUES
	('2017-01-01 06:01', 1, 1.2, 'test'),
	('2017-01-01 09:11', 3, 4.3, 'test'),
	('2017-01-01 08:01', 1, 7.3, 'test'),
	('2017-01-02 08:01', 2, 0.23, 'test'),
	('2018-07-02 08:01', 87, 0.0, 'test'),
	('2018-07-01 06:01', 13, 3.1, 'test'),
	('2018-07-01 09:11', 90, 10303.12, 'test'),
	('2018-07-01 08:01', 29, 64, 'test');

SELECT * FROM show_chunks('disttable');

-- Rename column
ALTER TABLE disttable RENAME COLUMN description TO descr;
SELECT * FROM test.show_columns('disttable')
WHERE "Column"='descr';

SELECT * FROM test.remote_exec('{ data_node_1 }', $$
	   SELECT chunk.relid AS chunk_relid,
	   		  (SELECT "Column" AS col FROM test.show_columns(chunk.relid) WHERE "Column"='descr')
	   FROM (SELECT "Child" AS relid FROM test.show_subtables('disttable') LIMIT 1) chunk
$$);

-- Rename constraint
ALTER TABLE disttable ADD CONSTRAINT device_check CHECK (device > 0);
ALTER TABLE disttable RENAME CONSTRAINT device_check TO device_chk;
SELECT * FROM test.show_constraints('disttable')
WHERE "Constraint"='device_chk';

SELECT * FROM test.remote_exec('{ data_node_1 }', $$
       SELECT chunk.relid AS chunk_relid,
	   		  (SELECT "Constraint" AS constr FROM test.show_constraints(chunk.relid) WHERE "Constraint"='device_chk')
	   FROM (SELECT "Child" AS relid FROM test.show_subtables('disttable') LIMIT 1) chunk
$$);

-- Rename index
ALTER INDEX disttable_description_idx RENAME to disttable_descr_idx;
SELECT * FROM test.show_indexes('disttable')
WHERE "Index"='disttable_descr_idx'::regclass;

SELECT * FROM test.remote_exec('{ data_node_1 }', $$
	   SELECT chunk.relid AS chunk_relid, (test.show_indexes(chunk.relid)).*
	   FROM (SELECT "Child" AS relid FROM test.show_subtables('disttable') LIMIT 1) chunk
$$);

-- Test REINDEX command with distributed hypertable
\c :MY_DB1
SELECT * FROM test.show_indexes('_timescaledb_internal._dist_hyper_1_1_chunk');
SELECT pg_relation_filepath('_timescaledb_internal._dist_hyper_1_1_chunk_disttable_pk'::regclass::oid) AS oid_before_reindex \gset
\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_1;

REINDEX TABLE disttable;
REINDEX (VERBOSE) TABLE disttable;

\c :MY_DB1
SELECT pg_relation_filepath('_timescaledb_internal._dist_hyper_1_1_chunk_disttable_pk'::regclass::oid) AS oid_after_reindex \gset
\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_1;

-- expect chunk index oid to change after the reindex operation
SELECT :'oid_before_reindex' <> :'oid_after_reindex';

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
FOR EACH ROW EXECUTE FUNCTION test_trigger();

DROP TRIGGER disttable_trigger_test on disttable;
DROP FUNCTION test_trigger;
CALL distributed_exec($$ DROP FUNCTION test_trigger $$);

-- DROP INDEX
\set ON_ERROR_STOP 0
DROP INDEX disttable_description_idx, disttable_pk;
\set ON_ERROR_STOP 1

DROP INDEX disttable_descr_idx;
DROP INDEX disttable_pk;
SELECT * FROM test.show_indexes('disttable');
SELECT * FROM test.remote_exec(NULL, $$ SELECT * FROM test.show_indexes('disttable') $$);

-- DROP TABLE
DROP TABLE disttable;
SELECT * FROM test.remote_exec(NULL, $$ SELECT 1 FROM pg_tables WHERE tablename = 'disttable' $$);

DROP TABLE non_disttable1;
DROP TABLE non_disttable2;

-- CREATE SCHEMA tests
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
CREATE SCHEMA dist_schema AUTHORIZATION :ROLE_1;

-- make sure schema has been created on each data node
SELECT * FROM test.remote_exec(NULL, $$
SELECT s.nspname, u.usename
FROM pg_catalog.pg_namespace s
JOIN pg_catalog.pg_user u ON u.usesysid = s.nspowner
WHERE s.nspname = 'dist_schema';
$$);

CREATE TABLE dist_schema.some_dist_table(time timestamptz, device int, color int, temp float);
SELECT * FROM create_hypertable('dist_schema.some_dist_table', 'time', replication_factor => 3);
SELECT * FROM test.remote_exec(NULL, $$ SELECT schemaname, tablename FROM pg_tables WHERE tablename = 'some_dist_table' $$);

-- DROP SCHEMA
DROP SCHEMA dist_schema CASCADE;
SELECT * FROM test.remote_exec(NULL, $$ SELECT schemaname, tablename FROM pg_tables WHERE tablename = 'some_dist_table' $$);

-- make sure schema has been dropped on each data node
SELECT * FROM test.remote_exec(NULL, $$
SELECT s.nspname, u.usename
FROM pg_catalog.pg_namespace s
JOIN pg_catalog.pg_user u ON u.usesysid = s.nspowner
WHERE s.nspname = 'dist_schema';
$$);

-- make sure empty schema schema has been created and then dropped on each data node
CREATE SCHEMA dist_schema_2;

SELECT * FROM test.remote_exec(NULL, $$
SELECT s.nspname, u.usename
FROM pg_catalog.pg_namespace s
JOIN pg_catalog.pg_user u ON u.usesysid = s.nspowner
WHERE s.nspname = 'dist_schema_2';
$$);

DROP SCHEMA dist_schema_2;

SELECT * FROM test.remote_exec(NULL, $$
SELECT s.nspname, u.usename
FROM pg_catalog.pg_namespace s
JOIN pg_catalog.pg_user u ON u.usesysid = s.nspowner
WHERE s.nspname = 'dist_schema_2';
$$);

-- transactional schema create/drop with local table
BEGIN;

CREATE SCHEMA dist_schema_3;
CREATE TABLE dist_schema_3.some_dist_table(time timestamptz, device int);

SELECT * FROM test.remote_exec(NULL, $$
SELECT s.nspname, u.usename
FROM pg_catalog.pg_namespace s
JOIN pg_catalog.pg_user u ON u.usesysid = s.nspowner
WHERE s.nspname = 'dist_schema_3';
$$);

DROP SCHEMA dist_schema_3 CASCADE;

ROLLBACK;

SELECT * FROM test.remote_exec(NULL, $$
SELECT s.nspname, u.usename
FROM pg_catalog.pg_namespace s
JOIN pg_catalog.pg_user u ON u.usesysid = s.nspowner
WHERE s.nspname = 'dist_schema_3';
$$);

-- ALTER SCHEMA RENAME TO
CREATE SCHEMA dist_schema;
CREATE TABLE dist_schema.some_dist_table(time timestamptz, device int, color int, temp float);
SELECT * FROM create_hypertable('dist_schema.some_dist_table', 'time', replication_factor => 3);
ALTER SCHEMA dist_schema RENAME TO dist_schema_2;
SELECT * FROM test.remote_exec(NULL, $$ SELECT schemaname, tablename FROM pg_tables WHERE tablename = 'some_dist_table' $$);

-- ALTER SCHEMA OWNER TO
ALTER SCHEMA dist_schema_2 OWNER TO :ROLE_1;

SELECT * FROM test.remote_exec(NULL, $$
SELECT s.nspname, u.usename
FROM pg_catalog.pg_namespace s
JOIN pg_catalog.pg_user u ON u.usesysid = s.nspowner
WHERE s.nspname = 'dist_schema_2';
$$);

DROP SCHEMA dist_schema_2 CASCADE;

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

-- Test disabling DDL commands on global objects
--
SET timescaledb_experimental.enable_distributed_ddl TO 'off';
SET client_min_messages TO DEBUG1;

-- CREATE SCHEMA
CREATE SCHEMA schema_global;

-- Ensure SCHEMA is not created on data nodes
SELECT * FROM test.remote_exec(NULL, $$
SELECT s.nspname, u.usename
FROM pg_catalog.pg_namespace s
JOIN pg_catalog.pg_user u ON u.usesysid = s.nspowner
WHERE s.nspname = 'schema_global';
$$);

-- RENAME SCHEMA
ALTER SCHEMA schema_global RENAME TO schema_global_2;

-- ALTER SCHEMA OWNER TO
ALTER SCHEMA schema_global_2 OWNER TO :ROLE_1;

-- REASSIGN OWNED BY TO
REASSIGN OWNED BY :ROLE_1 TO :ROLE_1;

-- Reset earlier to avoid different debug output between PG versions
RESET client_min_messages;

-- DROP OWNED BY schema_global_2
DROP OWNED BY :ROLE_1;

-- DROP SCHEMA
CREATE SCHEMA schema_global;
SELECT * FROM test.remote_exec(NULL, $$
SELECT s.nspname, u.usename
FROM pg_catalog.pg_namespace s
JOIN pg_catalog.pg_user u ON u.usesysid = s.nspowner
WHERE s.nspname = 'schema_global';
$$);
DROP SCHEMA schema_global;

SET timescaledb_experimental.enable_distributed_ddl TO 'on';

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

-- DDL with multiple sub-commands (ALTER)
BEGIN;
CREATE TABLE some_dist_table(time timestamptz, device int);
SELECT * FROM create_distributed_hypertable('some_dist_table', 'time');
\set ON_ERROR_STOP 0
-- Mixing SET and other options not supported. This is to protect
-- against mixing custom (compression) options with other
-- sub-commands.
ALTER TABLE some_dist_table SET (fillfactor = 10),
ADD CONSTRAINT device_check CHECK (device > 0);
\set ON_ERROR_STOP 1
ROLLBACK;

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
SELECT (test.show_constraints(chunk)).*
FROM show_chunks('disttable') AS chunk;

ALTER TABLE disttable DROP CONSTRAINT color_check;
SELECT * FROM test.show_constraints('disttable');
SELECT (test.show_constraints(chunk)).*
FROM show_chunks('disttable') AS chunk;

SELECT * FROM test.remote_exec(NULL, $$
SELECT show_chunks('disttable');
SELECT * FROM test.show_constraints('disttable');
SELECT (test.show_constraints(chunk)).*
FROM show_chunks('disttable') AS chunk;
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
    EXECUTE FUNCTION test_event_trigger_sql_drop_function();

SET ROLE :ROLE_1;

-- Test DROP inside event trigger on local table (should not crash)
CREATE TABLE non_htable (id int PRIMARY KEY);
DROP TABLE non_htable;

\c :TEST_DBNAME :ROLE_SUPERUSER;
DROP EVENT TRIGGER test_event_trigger_sqldrop;
SET ROLE :ROLE_1;

-- Test DDL blocking from non-frontend session
--
-- We test only special corner cases since most of this functionality already
-- been tested before.
--
CREATE TABLE disttable(time timestamptz, device int);
SELECT * FROM create_hypertable('disttable', 'time', replication_factor => 3);
CREATE INDEX disttable_device_idx ON disttable (device);

-- Test alter replication factor on empty table
SELECT replication_factor FROM _timescaledb_catalog.hypertable ORDER BY id;
SELECT * FROM set_replication_factor('disttable',  1);
SELECT replication_factor FROM _timescaledb_catalog.hypertable ORDER BY id;
SELECT * FROM set_replication_factor('disttable',  1);
SELECT replication_factor FROM _timescaledb_catalog.hypertable ORDER BY id;
SELECT * FROM set_replication_factor('disttable',  2);
SELECT replication_factor FROM _timescaledb_catalog.hypertable ORDER BY id;
\set ON_ERROR_STOP 0
SELECT * FROM set_replication_factor('disttable',  4);
SELECT * FROM set_replication_factor('disttable',  0);
SELECT * FROM set_replication_factor('disttable',  NULL);
\set ON_ERROR_STOP 1
SELECT replication_factor FROM _timescaledb_catalog.hypertable ORDER BY id;

\c :MY_DB1
SELECT schemaname, tablename FROM pg_tables WHERE tablename = 'disttable';
SELECT * FROM test.show_indexes('disttable');

\set ON_ERROR_STOP 0

-- fail to alter replication factor for the table on data node
SELECT * FROM set_replication_factor('disttable',  1);

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
SET ROLE :ROLE_1;

-- Should fail because of the inconsistency
\set ON_ERROR_STOP 0
DROP INDEX disttable_device_idx;
\set ON_ERROR_STOP 1

DROP TABLE disttable;

-- cleanup
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
DROP DATABASE :MY_DB1;
DROP DATABASE :MY_DB2;
DROP DATABASE :MY_DB3;
