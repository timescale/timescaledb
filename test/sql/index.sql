CREATE TABLE index_test(time timestamptz, temp float);

SELECT create_hypertable('index_test', 'time');

-- Default indexes created
\d+ index_test

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
\d+ index_test
\d+ _timescaledb_internal._hyper*_chunk
SELECT * FROM _timescaledb_catalog.chunk_index;

-- Create another chunk
INSERT INTO index_test VALUES ('2017-05-20T09:00:01', 3, 17.5);

\d+ _timescaledb_internal._hyper*_chunk
SELECT * FROM _timescaledb_catalog.chunk_index;

-- Delete the index on only one chunk
DROP INDEX _timescaledb_internal._hyper_3_1_chunk_index_test_time_idx;
\d+ _timescaledb_internal._hyper*_chunk
SELECT * FROM _timescaledb_catalog.chunk_index;

-- Recreate table with new partitioning
DROP TABLE index_test;
CREATE TABLE index_test(time timestamptz, device integer, temp float);

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

-- New index should have been recursed to chunk
\d+ index_test
\d+ _timescaledb_internal._hyper*_chunk
SELECT * FROM _timescaledb_catalog.chunk_index;

ALTER INDEX index_test_time_idx RENAME TO index_test_time_idx2;

-- Metadata and index should have changed name
\d+ index_test
\d+ _timescaledb_internal._hyper*_chunk
SELECT * FROM _timescaledb_catalog.chunk_index;

DROP INDEX index_test_time_idx2;
DROP INDEX index_test_time_device_idx;

-- Index should have been dropped
\d+ index_test
\d+ _timescaledb_internal._hyper*_chunk
SELECT * FROM _timescaledb_catalog.chunk_index;

-- Create index with long name to see how this is handled on chunks
CREATE INDEX a_hypertable_index_with_a_very_very_long_name_that_truncates ON index_test (time, temp);
CREATE INDEX a_hypertable_index_with_a_very_very_long_name_that_truncates_2 ON index_test (time, temp);

\d+ index_test
\d+ _timescaledb_internal._hyper*_chunk

DROP INDEX a_hypertable_index_with_a_very_very_long_name_that_truncates;
DROP INDEX a_hypertable_index_with_a_very_very_long_name_that_truncates_2;

\set ON_ERROR_STOP 0
-- Create index CONCURRENTLY
CREATE UNIQUE INDEX CONCURRENTLY index_test_time_device_idx ON index_test (time, device);
\set ON_ERROR_STOP 1

-- Test tablespaces. Chunk indexes should end up in same tablespace as
-- main index.
\c single :ROLE_SUPERUSER
SET client_min_messages = ERROR;
DROP TABLESPACE IF EXISTS tablespace1;
DROP TABLESPACE IF EXISTS tablespace2;
SET client_min_messages = NOTICE;

CREATE TABLESPACE tablespace1 OWNER :ROLE_DEFAULT_PERM_USER LOCATION :TEST_TABLESPACE1_PATH;
\c single :ROLE_DEFAULT_PERM_USER
CREATE INDEX index_test_time_idx ON index_test (time) TABLESPACE tablespace1;

\d+ index_test
\d+ _timescaledb_internal._hyper*

\c single :ROLE_SUPERUSER
CREATE TABLESPACE tablespace2 OWNER :ROLE_DEFAULT_PERM_USER LOCATION :TEST_TABLESPACE2_PATH;
\c single :ROLE_DEFAULT_PERM_USER
ALTER INDEX index_test_time_idx SET TABLESPACE tablespace2;

\d+ index_test
\d+ _timescaledb_internal._hyper*_chunk

-- Add constraint index
ALTER TABLE index_test ADD UNIQUE (time, device);

\d+ index_test
\d+ _timescaledb_internal._hyper*_chunk
-- Constraint indexes NOT added to chunk_index table (maybe they
-- should be for consistency?)
SELECT * FROM _timescaledb_catalog.chunk_index;

DROP TABLE index_test;

-- Metadata removed
SELECT * FROM _timescaledb_catalog.chunk_index;

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
\d+ index_test
\d+ _timescaledb_internal._hyper*_chunk

-- Creating a new index should propagate to existing chunks, including
-- the given tablespace
CREATE INDEX ON index_test (time, temp) TABLESPACE tablespace2;

\d+ index_test
\d+ _timescaledb_internal._hyper*_chunk

-- Cleanup
DROP TABLE index_test CASCADE;
\c single :ROLE_SUPERUSER
DROP TABLESPACE tablespace1;
DROP TABLESPACE tablespace2;
