-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE TABLE index_test(time timestamptz, temp float);

SELECT create_hypertable('index_test', 'time');

-- Default indexes created
SELECT * FROM test.show_indexes('index_test');

DROP TABLE index_test;
CREATE TABLE index_test(time timestamptz, device integer, temp float);

-- Create index before create_hypertable()
CREATE UNIQUE INDEX index_test_time_idx ON index_test (time);

\set ON_ERROR_STOP 0
-- Creating a hypertable from a table with an index that doesn't cover
-- all partitioning columns should fail
SELECT create_hypertable('index_test', 'time', 'device', 2);
\set ON_ERROR_STOP 1

-- Partitioning on only time should work
SELECT create_hypertable('index_test', 'time');

INSERT INTO index_test VALUES ('2017-01-20T09:00:01', 1, 17.5);

-- Check that index is also created on chunk
SELECT * FROM test.show_indexes('index_test');
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');
SELECT * FROM _timescaledb_catalog.chunk_index ORDER BY index_name, hypertable_index_name;

-- Create another chunk
INSERT INTO index_test VALUES ('2017-05-20T09:00:01', 3, 17.5);

SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');
SELECT * FROM _timescaledb_catalog.chunk_index ORDER BY index_name, hypertable_index_name;

-- Delete the index on only one chunk
DROP INDEX _timescaledb_internal._hyper_3_1_chunk_index_test_time_idx;
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');
SELECT * FROM _timescaledb_catalog.chunk_index ORDER BY index_name, hypertable_index_name;

-- Recreate table with new partitioning
DROP TABLE index_test;
CREATE TABLE index_test(id serial, time timestamptz, device integer, temp float);
SELECT * FROM test.show_columns('index_test');

-- Test that we can handle difference in attnos across hypertable and
-- chunks by dropping the ID column
ALTER TABLE index_test DROP COLUMN id;
SELECT * FROM test.show_columns('index_test');

-- No pre-existing UNIQUE index, so partitioning on two columns should work
SELECT create_hypertable('index_test', 'time', 'device', 2);

INSERT INTO index_test VALUES ('2017-01-20T09:00:01', 1, 17.5);

\set ON_ERROR_STOP 0
-- Create unique index without all partitioning columns should fail
CREATE UNIQUE INDEX index_test_time_device_idx ON index_test (time);
\set ON_ERROR_STOP 1

CREATE UNIQUE INDEX index_test_time_device_idx ON index_test (time, device);

-- Regular index need not cover all partitioning columns
CREATE INDEX ON index_test (time, temp);

-- Create another chunk
INSERT INTO index_test VALUES ('2017-04-20T09:00:01', 1, 17.5);

-- New index should have been recursed to chunks
SELECT * FROM test.show_indexes('index_test');
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');
SELECT * FROM _timescaledb_catalog.chunk_index ORDER BY index_name, hypertable_index_name;

ALTER INDEX index_test_time_idx RENAME TO index_test_time_idx2;

-- Metadata and index should have changed name
SELECT * FROM test.show_indexes('index_test');
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');
SELECT * FROM _timescaledb_catalog.chunk_index ORDER BY index_name, hypertable_index_name;

DROP INDEX index_test_time_idx2;
DROP INDEX index_test_time_device_idx;

-- Index should have been dropped
SELECT * FROM test.show_indexes('index_test');
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');
SELECT * FROM _timescaledb_catalog.chunk_index ORDER BY index_name, hypertable_index_name;

-- Create index with long name to see how this is handled on chunks
CREATE INDEX a_hypertable_index_with_a_very_very_long_name_that_truncates ON index_test (time, temp);
CREATE INDEX a_hypertable_index_with_a_very_very_long_name_that_truncates_2 ON index_test (time, temp);

SELECT * FROM test.show_indexes('index_test');
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');

DROP INDEX a_hypertable_index_with_a_very_very_long_name_that_truncates;
DROP INDEX a_hypertable_index_with_a_very_very_long_name_that_truncates_2;

\set ON_ERROR_STOP 0
-- Create index CONCURRENTLY
CREATE UNIQUE INDEX CONCURRENTLY index_test_time_device_idx ON index_test (time, device);
\set ON_ERROR_STOP 1

-- Test tablespaces. Chunk indexes should end up in same tablespace as
-- main index.
\c :TEST_DBNAME :ROLE_SUPERUSER
SET client_min_messages = ERROR;
DROP TABLESPACE IF EXISTS tablespace1;
DROP TABLESPACE IF EXISTS tablespace2;
SET client_min_messages = NOTICE;

CREATE TABLESPACE tablespace1 OWNER :ROLE_DEFAULT_PERM_USER LOCATION :TEST_TABLESPACE1_PATH;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
CREATE INDEX index_test_time_idx ON index_test (time) TABLESPACE tablespace1;

SELECT * FROM test.show_indexes('index_test');
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE TABLESPACE tablespace2 OWNER :ROLE_DEFAULT_PERM_USER LOCATION :TEST_TABLESPACE2_PATH;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
ALTER INDEX index_test_time_idx SET TABLESPACE tablespace2;

SELECT * FROM test.show_indexes('index_test');
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');

-- Add constraint index
ALTER TABLE index_test ADD UNIQUE (time, device);

SELECT * FROM test.show_indexes('index_test');
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');

-- Constraint indexes are added to chunk_index table.
SELECT * FROM _timescaledb_catalog.chunk_index ORDER BY index_name, hypertable_index_name;
SELECT * FROM _timescaledb_catalog.chunk_constraint;

DROP TABLE index_test;

-- Metadata removed
SELECT * FROM _timescaledb_catalog.chunk_index ORDER BY index_name, hypertable_index_name;

-- Create table in a tablespace
CREATE TABLE index_test(time timestamptz, temp float, device int) TABLESPACE tablespace1;

-- Default indexes should be in the table's tablespace
SELECT create_hypertable('index_test', 'time');

-- Explicitly defining an index tablespace should work and propagate
-- to chunks
CREATE INDEX ON index_test (time, device) TABLESPACE tablespace2;

-- New indexes without explicit tablespaces should use the default
-- tablespace
CREATE INDEX ON index_test (device);

-- Create chunk
INSERT INTO index_test VALUES ('2017-01-20T09:00:01', 17.5);

-- Check that the tablespaces of chunk indexes match the tablespace of
-- the main index
SELECT * FROM test.show_indexes('index_test');
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');

-- Creating a new index should propagate to existing chunks, including
-- the given tablespace
CREATE INDEX ON index_test (time, temp) TABLESPACE tablespace2;

SELECT * FROM test.show_indexes('index_test');
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');

-- Cleanup
DROP TABLE index_test CASCADE;
\c :TEST_DBNAME :ROLE_SUPERUSER
DROP TABLESPACE tablespace1;
DROP TABLESPACE tablespace2;

-- Test expression indexes
CREATE TABLE index_expr_test(id serial, time timestamptz, temp float, meta jsonb);

-- Screw up the attribute numbers
ALTER TABLE index_expr_test DROP COLUMN id;

CREATE INDEX ON index_expr_test ((meta ->> 'field')) ;
INSERT INTO index_expr_test VALUES ('2017-01-20T09:00:01', 17.5, '{"field": "value1"}');
INSERT INTO index_expr_test VALUES ('2017-01-20T09:00:01', 17.5, '{"field": "value2"}');

EXPLAIN (verbose, costs off)
SELECT * FROM index_expr_test WHERE meta ->> 'field' = 'value1';
SELECT * FROM index_expr_test WHERE meta ->> 'field' = 'value1';

-- Test INDEX DROP error for multiple objects
CREATE TABLE index_test(time timestamptz, temp float);
CREATE UNIQUE INDEX index_test_idx ON index_test (time, temp);
SELECT create_hypertable('index_test', 'time');

CREATE TABLE index_test_2(time timestamptz, temp float);
CREATE UNIQUE INDEX index_test_2_idx ON index_test_2 (time, temp);

\set ON_ERROR_STOP 0
DROP INDEX index_test_idx, index_test_2_idx;
\set ON_ERROR_STOP 1

-- test expression index with dropped columns
CREATE TABLE idx_expr_test(filler int, time timestamptz, meta text);
SELECT table_name FROM create_hypertable('idx_expr_test', 'time');
ALTER TABLE idx_expr_test DROP COLUMN filler;
CREATE INDEX tag_idx ON idx_expr_test(('foo'||meta));
INSERT INTO idx_expr_test(time, meta) VALUES ('2000-01-01', 'bar');
DROP TABLE idx_expr_test CASCADE;

-- test multicolumn expression index with dropped columns
CREATE TABLE idx_expr_test(filler int, time timestamptz, t1 text, t2 text, t3 text);
SELECT table_name FROM create_hypertable('idx_expr_test', 'time');
ALTER TABLE idx_expr_test DROP COLUMN filler;
CREATE INDEX tag_idx ON idx_expr_test((t1||t2||t3));
INSERT INTO idx_expr_test(time, t1, t2, t3) VALUES ('2000-01-01', 'foo', 'bar', 'baz');
DROP TABLE idx_expr_test CASCADE;

-- test index with predicate and dropped columns
CREATE TABLE idx_predicate_test(filler int, time timestamptz);
SELECT table_name FROM create_hypertable('idx_predicate_test', 'time');
ALTER TABLE idx_predicate_test DROP COLUMN filler;
ALTER TABLE idx_predicate_test ADD COLUMN b1 bool;
CREATE INDEX idx_predicate_test_b1 ON idx_predicate_test(b1) WHERE b1=true;
INSERT INTO idx_predicate_test VALUES ('2000-01-01',true);
DROP TABLE idx_predicate_test;

