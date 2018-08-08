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

SELECT * FROM chunk_relation_size('test_adaptive');

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

SELECT * FROM chunk_relation_size('test_adaptive_no_index');

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
