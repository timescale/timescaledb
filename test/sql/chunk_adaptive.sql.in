-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Valid chunk sizing function for testing
CREATE OR REPLACE FUNCTION calculate_chunk_interval(
        dimension_id INTEGER,
        dimension_coord BIGINT,
        chunk_target_size BIGINT
)
    RETURNS BIGINT LANGUAGE PLPGSQL AS
$BODY$
DECLARE
BEGIN
    RETURN -1;
END
$BODY$;

-- Chunk sizing function with bad signature
CREATE OR REPLACE FUNCTION bad_calculate_chunk_interval(
        dimension_id INTEGER
)
    RETURNS BIGINT LANGUAGE PLPGSQL AS
$BODY$
DECLARE
BEGIN
    RETURN -1;
END
$BODY$;

-- Set a fixed memory cache size to make tests determinstic
-- (independent of available machine memory)
SELECT * FROM test.set_memory_cache_size('2GB');

-- test NULL handling
\set ON_ERROR_STOP 0
SELECT * FROM set_adaptive_chunking(NULL,NULL);
\set ON_ERROR_STOP 1

CREATE TABLE test_adaptive(time timestamptz, temp float, location int);

\set ON_ERROR_STOP 0
-- Bad signature of sizing func should fail
SELECT create_hypertable('test_adaptive', 'time',
                         chunk_target_size => '1MB',
                         chunk_sizing_func => 'bad_calculate_chunk_interval');
\set ON_ERROR_STOP 1

-- Setting sizing func with correct signature should work
SELECT create_hypertable('test_adaptive', 'time',
                         chunk_target_size => '1MB',
                         chunk_sizing_func => 'calculate_chunk_interval');

DROP TABLE test_adaptive;
CREATE TABLE test_adaptive(time timestamptz, temp float, location int);

-- Size but no explicit func should use default func
SELECT create_hypertable('test_adaptive', 'time',
                         chunk_target_size => '1MB',
                         create_default_indexes => true);
SELECT table_name, chunk_sizing_func_schema, chunk_sizing_func_name, chunk_target_size
FROM _timescaledb_catalog.hypertable;

-- Check that adaptive chunking sets a 1 day default chunk time
-- interval => 86400000000 microseconds
SELECT * FROM _timescaledb_catalog.dimension;

-- Change the target size
SELECT * FROM set_adaptive_chunking('test_adaptive', '2MB');
SELECT table_name, chunk_sizing_func_schema, chunk_sizing_func_name, chunk_target_size
FROM _timescaledb_catalog.hypertable;

\set ON_ERROR_STOP 0
-- Setting NULL func should fail
SELECT * FROM set_adaptive_chunking('test_adaptive', '1MB', NULL);
\set ON_ERROR_STOP 1

-- Setting NULL size disables adaptive chunking
SELECT * FROM set_adaptive_chunking('test_adaptive', NULL);
SELECT table_name, chunk_sizing_func_schema, chunk_sizing_func_name, chunk_target_size
FROM _timescaledb_catalog.hypertable;

SELECT * FROM set_adaptive_chunking('test_adaptive', '1MB');

-- Setting size to 'off' should also disable
SELECT * FROM set_adaptive_chunking('test_adaptive', 'off');
SELECT table_name, chunk_sizing_func_schema, chunk_sizing_func_name, chunk_target_size
FROM _timescaledb_catalog.hypertable;

-- Setting 0 size should also disable
SELECT * FROM set_adaptive_chunking('test_adaptive', '0MB');
SELECT table_name, chunk_sizing_func_schema, chunk_sizing_func_name, chunk_target_size
FROM _timescaledb_catalog.hypertable;

SELECT * FROM set_adaptive_chunking('test_adaptive', '1MB');

-- No warning about small target size if > 10MB
SELECT * FROM set_adaptive_chunking('test_adaptive', '11MB');

-- Setting size to 'estimate' should also estimate size
SELECT * FROM set_adaptive_chunking('test_adaptive', 'estimate');
SELECT table_name, chunk_sizing_func_schema, chunk_sizing_func_name, chunk_target_size
FROM _timescaledb_catalog.hypertable;

-- Use a lower memory setting to test that the calculated chunk_target_size is reduced
SELECT * FROM test.set_memory_cache_size('512MB');
SELECT * FROM set_adaptive_chunking('test_adaptive', 'estimate');
SELECT table_name, chunk_sizing_func_schema, chunk_sizing_func_name, chunk_target_size
FROM _timescaledb_catalog.hypertable;

-- Reset memory settings
SELECT * FROM test.set_memory_cache_size('2GB');

-- Set a reasonable test value
SELECT * FROM set_adaptive_chunking('test_adaptive', '1MB');

-- Show the interval length before and after adaptation
SELECT id, hypertable_id, interval_length FROM _timescaledb_catalog.dimension;

-- Generate data to create chunks. We use the hash of the time value
-- to get determinstic location IDs so that we always spread these
-- values the same way across space partitions
INSERT INTO test_adaptive
SELECT time, random() * 35, _timescaledb_internal.get_partition_hash(time) FROM
generate_series('2017-03-07T18:18:03+00'::timestamptz - interval '175 days',
                '2017-03-07T18:18:03+00'::timestamptz,
                '2 minutes') as time;

SELECT chunk_id, chunk_table, partitioning_columns, partitioning_column_types, partitioning_hash_functions, ranges FROM chunk_relation_size('test_adaptive');

-- Do same thing without an index on the time column. This affects
-- both the calculation of fill-factor of the chunk and its size
CREATE TABLE test_adaptive_no_index(time timestamptz, temp float, location int);

-- Size but no explicit func should use default func
-- No default indexes should warn and use heap scan for min and max
SELECT create_hypertable('test_adaptive_no_index', 'time',
                         chunk_target_size => '1MB',
                         create_default_indexes => false);
SELECT id, hypertable_id, interval_length FROM _timescaledb_catalog.dimension;

INSERT INTO test_adaptive_no_index
SELECT time, random() * 35, _timescaledb_internal.get_partition_hash(time) FROM
generate_series('2017-03-07T18:18:03+00'::timestamptz - interval '175 days',
                '2017-03-07T18:18:03+00'::timestamptz,
                '2 minutes') as time;

SELECT chunk_id, chunk_table, partitioning_columns, partitioning_column_types, partitioning_hash_functions, ranges FROM chunk_relation_size('test_adaptive_no_index');

-- Test added to check that the correct index (i.e. time index) is being used
-- to find the min and max. Previously a bug selected the first index listed,
-- which in this case is location rather than time and therefore could return
-- the wrong min and max if items at the start and end of the index did not have
-- the correct min and max timestamps.
--
-- In this test, we create chunks with a lot of locations with only one reading
-- that is at the beginning of the time frame, and then one location in the middle
-- of the range that has two readings, one that is the same as the others and one
-- that is larger. The algorithm should use these two readings for min & max; however,
-- if it's broken (as it was before), it would choose just the reading that is common
-- to all the locations.
CREATE TABLE test_adaptive_correct_index(time timestamptz, temp float, location int);
SELECT create_hypertable('test_adaptive_correct_index', 'time',
                         chunk_target_size => '100MB',
                         chunk_time_interval => 86400000000,
                         create_default_indexes => false);
CREATE INDEX ON test_adaptive_correct_index(location);
CREATE INDEX ON test_adaptive_correct_index(time DESC);

-- First chunk
INSERT INTO test_adaptive_correct_index
SELECT '2018-01-01T00:00:00+00'::timestamptz, val, val + 1 FROM
generate_series(1, 1000) as val;
INSERT INTO test_adaptive_correct_index
SELECT time, 0.0, '1500' FROM
generate_series('2018-01-01T00:00:00+00'::timestamptz,
                '2018-01-01T20:00:00+00'::timestamptz,
                '10 hours') as time;
INSERT INTO test_adaptive_correct_index
SELECT '2018-01-01T00:00:00+00'::timestamptz, val, val + 1 FROM
generate_series(2001, 3000) as val;

-- Second chunk
INSERT INTO test_adaptive_correct_index
SELECT '2018-01-02T00:00:00+00'::timestamptz, val, val + 1 FROM
generate_series(1, 1000) as val;
INSERT INTO test_adaptive_correct_index
SELECT time, 0.0, '1500' FROM
generate_series('2018-01-02T00:00:00+00'::timestamptz,
                '2018-01-02T20:00:00+00'::timestamptz,
                '10 hours') as time;
INSERT INTO test_adaptive_correct_index
SELECT '2018-01-02T00:00:00+00'::timestamptz, val, val + 1 FROM
generate_series(2001, 3000) as val;

-- Third chunk
INSERT INTO test_adaptive_correct_index
SELECT '2018-01-03T00:00:00+00'::timestamptz, val, val + 1 FROM
generate_series(1, 1000) as val;
INSERT INTO test_adaptive_correct_index
SELECT time, 0.0, '1500' FROM
generate_series('2018-01-03T00:00:00+00'::timestamptz,
                '2018-01-03T20:00:00+00'::timestamptz,
                '10 hours') as time;
INSERT INTO test_adaptive_correct_index
SELECT '2018-01-03T00:00:00+00'::timestamptz, val, val + 1 FROM
generate_series(2001, 3000) as val;

-- This should be the start of the fourth chunk
INSERT INTO test_adaptive_correct_index
SELECT '2018-01-04T00:00:00+00'::timestamptz, val, val + 1 FROM
generate_series(1, 1000) as val;
INSERT INTO test_adaptive_correct_index
SELECT time, 0.0, '1500' FROM
generate_series('2018-01-04T00:00:00+00'::timestamptz,
                '2018-01-04T20:00:00+00'::timestamptz,
                '10 hours') as time;
INSERT INTO test_adaptive_correct_index
SELECT '2018-01-04T00:00:00+00'::timestamptz, val, val + 1 FROM
generate_series(2001, 3000) as val;

-- If working correctly, this goes in the 4th chunk, otherwise its a separate 5th chunk
INSERT INTO test_adaptive_correct_index
SELECT '2018-01-05T00:00:00+00'::timestamptz, val, val + 1 FROM
generate_series(1, 1000) as val;
INSERT INTO test_adaptive_correct_index
SELECT time, 0.0, '1500' FROM
generate_series('2018-01-05T00:00:00+00'::timestamptz,
                '2018-01-05T20:00:00+00'::timestamptz,
                '10 hours') as time;
INSERT INTO test_adaptive_correct_index
SELECT '2018-01-05T00:00:00+00'::timestamptz, val, val + 1 FROM
generate_series(2001, 3000) as val;

-- This should show 4 chunks, rather than 5
SELECT COUNT(*) FROM chunk_relation_size_pretty('test_adaptive_correct_index');
-- The interval_length should no longer be 86400000000 for our hypertable, so 3rd column so be true.
-- Note: the exact interval_length is non-deterministic, so we can't use its actual value for tests
SELECT id, hypertable_id, interval_length > 86400000000 FROM _timescaledb_catalog.dimension;

-- Drop because it's size and estimated chunk_interval is non-deterministic so
-- we don't want to make other tests flaky.
DROP TABLE test_adaptive_correct_index;

-- Test with space partitioning. This might affect the estimation
-- since there are more chunks in the same time interval and space
-- chunks might be unevenly filled.
CREATE TABLE test_adaptive_space(time timestamptz, temp float, location int);
SELECT create_hypertable('test_adaptive_space', 'time', 'location', 2,
                         chunk_target_size => '1MB',
                         create_default_indexes => true);

SELECT id, hypertable_id, interval_length FROM _timescaledb_catalog.dimension;

INSERT INTO test_adaptive_space
SELECT time, random() * 35, _timescaledb_internal.get_partition_hash(time) FROM
generate_series('2017-03-07T18:18:03+00'::timestamptz - interval '175 days',
                '2017-03-07T18:18:03+00'::timestamptz,
                '2 minutes') as time;

SELECT * FROM chunk_relation_size('test_adaptive_space');
SELECT id, hypertable_id, interval_length FROM _timescaledb_catalog.dimension;

-- A previous version stopped working as soon as hypertable_id stopped being
-- equal to dimension_id (i.e., there was a hypertable with more than 1 dimension).
-- This test comes after test_adaptive_space, which has 2 dimensions, and makes
-- sure that it still works.
CREATE TABLE test_adaptive_after_multiple_dims(time timestamptz, temp float, location int);
SELECT create_hypertable('test_adaptive_after_multiple_dims', 'time',
                         chunk_target_size => '100MB',
                         create_default_indexes => true);
INSERT INTO test_adaptive_after_multiple_dims VALUES('2018-01-01T00:00:00+00'::timestamptz, 0.0, 5);

\c  :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2
\set ON_ERROR_STOP 0
SELECT * FROM set_adaptive_chunking('test_adaptive', '2MB');
\set ON_ERROR_STOP 1

\c  :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
-- Now make sure renaming schema gets propagated to the func_schema
DROP TABLE test_adaptive;

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE SCHEMA IF NOT EXISTS my_chunk_func_schema;

CREATE OR REPLACE FUNCTION my_chunk_func_schema.calculate_chunk_interval(
        dimension_id INTEGER,
        dimension_coord BIGINT,
        chunk_target_size BIGINT
)
    RETURNS BIGINT LANGUAGE PLPGSQL AS
$BODY$
DECLARE
BEGIN
    RETURN 2;
END
$BODY$;

CREATE TABLE test_adaptive(time timestamptz, temp float, location int);
SELECT create_hypertable('test_adaptive', 'time',
                         chunk_target_size => '1MB',
                         chunk_sizing_func => 'my_chunk_func_schema.calculate_chunk_interval');


ALTER SCHEMA my_chunk_func_schema RENAME TO new_chunk_func_schema;
INSERT INTO test_adaptive VALUES (now(), 1.0, 1);
