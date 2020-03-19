-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

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
-- cannot create a UNIQUE index with transaction_per_chunk
CREATE UNIQUE INDEX index_test_time_device_idx ON index_test (time) WITH (timescaledb.transaction_per_chunk);
CREATE UNIQUE INDEX index_test_time_device_idx ON index_test (time, device) WITH(timescaledb.transaction_per_chunk);
\set ON_ERROR_STOP 1

CREATE INDEX index_test_time_device_idx ON index_test (time, device) WITH (timescaledb.transaction_per_chunk);

-- Regular index need not cover all partitioning columns
CREATE INDEX ON index_test (time, temp) WITH (timescaledb.transaction_per_chunk);

-- Create another chunk
INSERT INTO index_test VALUES ('2017-04-20T09:00:01', 1, 17.5);

-- New index should have been recursed to chunks
SELECT * FROM test.show_indexes('index_test');
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk') ORDER BY 1,2;
SELECT * FROM _timescaledb_catalog.chunk_index ORDER BY index_name;

ALTER INDEX index_test_time_idx RENAME TO index_test_time_idx2;

-- Metadata and index should have changed name
SELECT * FROM test.show_indexes('index_test');
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk') ORDER BY 1,2;
SELECT * FROM _timescaledb_catalog.chunk_index ORDER BY index_name;

DROP INDEX index_test_time_idx2;
DROP INDEX index_test_time_device_idx;

-- Index should have been dropped
SELECT * FROM test.show_indexes('index_test');
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');
SELECT * FROM _timescaledb_catalog.chunk_index;

-- Create index with long name to see how this is handled on chunks
CREATE INDEX a_hypertable_index_with_a_very_very_long_name_that_truncates ON index_test (time, temp) WITH (timescaledb.transaction_per_chunk);
CREATE INDEX a_hypertable_index_with_a_very_very_long_name_that_truncates_2 ON index_test (time, temp) WITH (timescaledb.transaction_per_chunk);

SELECT * FROM test.show_indexes('index_test');
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');

DROP INDEX a_hypertable_index_with_a_very_very_long_name_that_truncates;
DROP INDEX a_hypertable_index_with_a_very_very_long_name_that_truncates_2;


SELECT * FROM test.show_indexes('index_test');
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');

SELECT * FROM test.show_indexes('index_test');
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');

-- Add constraint index
ALTER TABLE index_test ADD UNIQUE (time, device);

SELECT * FROM test.show_indexes('index_test');
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');

-- Constraint indexes are added to chunk_index table.
SELECT * FROM _timescaledb_catalog.chunk_index;
SELECT * FROM _timescaledb_catalog.chunk_constraint;

DROP TABLE index_test;

-- Metadata removed
SELECT * FROM _timescaledb_catalog.chunk_index;

-- Test that indexes are planned correctly
CREATE TABLE index_expr_test(id serial, time timestamptz, temp float, meta int);
select create_hypertable('index_expr_test', 'time');

-- Screw up the attribute numbers
ALTER TABLE index_expr_test DROP COLUMN id;

CREATE INDEX ON index_expr_test (meta) WITH (timescaledb.transaction_per_chunk);
INSERT INTO index_expr_test VALUES ('2017-01-20T09:00:01', 17.5, 1);
INSERT INTO index_expr_test VALUES ('2017-01-20T09:00:01', 17.5, 2);


SET enable_seqscan TO false;
SET enable_bitmapscan TO false;
EXPLAIN (verbose, costs off)
SELECT * FROM index_expr_test WHERE meta = 1;
SELECT * FROM index_expr_test WHERE meta = 1;
SET enable_seqscan TO default;
SET enable_bitmapscan TO default;

\set ON_ERROR_STOP 0
-- cannot create a transaction_per_chunk index within a transaction block
BEGIN;
CREATE INDEX ON index_expr_test (temp) WITH (timescaledb.transaction_per_chunk);
ROLLBACK;
\set ON_ERROR_STOP 1

DROP TABLE index_expr_test CASCADE;

CREATE TABLE partial_index_test(time INTEGER);
SELECT create_hypertable('partial_index_test', 'time', chunk_time_interval => 1, create_default_indexes => false);

-- create 3 chunks
INSERT INTO partial_index_test(time) SELECT generate_series(0, 2);

select * from partial_index_test order by 1;

-- create indexes on only 1 of the chunks
CREATE INDEX ON partial_index_test (time) WITH (timescaledb.transaction_per_chunk, timescaledb.max_chunks='1');

SELECT * FROM test.show_indexes('partial_index_test');
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');

-- regerssion test for bug fixed by PR #1008.
-- this caused an assertion failure when a MergeAppend node contained unsorted children
SET enable_seqscan TO false;
SET enable_bitmapscan TO false;
EXPLAIN (verbose, costs off) SELECT * FROM partial_index_test WHERE time < 2 ORDER BY time LIMIT 2;
SELECT * FROM partial_index_test WHERE time < 2 ORDER BY time LIMIT 2;

-- we can drop the partially created index
DROP INDEX partial_index_test_time_idx;

SELECT * FROM test.show_indexes('partial_index_test');
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');

EXPLAIN (verbose, costs off) SELECT * FROM partial_index_test WHERE time < 2 ORDER BY time LIMIT 2;
SELECT * FROM partial_index_test WHERE time < 2 ORDER BY time LIMIT 2;

SET enable_seqscan TO true;
SET enable_bitmapscan TO true;

\c  :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2
\set ON_ERROR_STOP 0
CREATE INDEX ON partial_index_test (time) WITH (timescaledb.transaction_per_chunk, timescaledb.max_chunks='1');
\set ON_ERROR_STOP 1
