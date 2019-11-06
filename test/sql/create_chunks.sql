-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

--
--  This test will create chunks in two dimenisions, time (x) and
--  space (y), where the time dimension is aligned. The figure below
--  shows the expected result. The chunk number in the figure
--  indicates the creation order.
--
--  +
--  +
--  +     +-----+     +-----+
--  +     |  2  |     |  3  |
--  +     |     +---+-+     |
--  +     +-----+ 5 |6+-----+
--  +     |  1  +---+-+-----+     +---------+
--  +     |     |   |4|  7  |     |    8    |
--  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-
--  0         5         10        15        20
--
-- Partitioning:
--
-- Chunk #  |  time  | space
--    1     |   3    |   2
--    4     |   1    |   3
--    5     |   5    |   3
--

CREATE TABLE chunk_test(time integer, temp float8, tag integer, color integer);

SELECT create_hypertable('chunk_test', 'time', 'tag', 2, chunk_time_interval => 3);

INSERT INTO chunk_test VALUES (4, 24.3, 1, 1);

SELECT * FROM _timescaledb_catalog.dimension_slice;

INSERT INTO chunk_test VALUES (4, 24.3, 2, 1);
INSERT INTO chunk_test VALUES (10, 24.3, 2, 1);

SELECT c.table_name AS chunk_name, d.id AS dimension_id, ds.id AS slice_id, range_start, range_end FROM _timescaledb_catalog.chunk c
LEFT JOIN _timescaledb_catalog.chunk_constraint cc ON (c.id = cc.chunk_id)
LEFT JOIN _timescaledb_catalog.dimension_slice ds ON (ds.id = cc.dimension_slice_id)
LEFT JOIN _timescaledb_catalog.dimension d ON (d.id = ds.dimension_id)
LEFT JOIN _timescaledb_catalog.hypertable h ON (d.hypertable_id = h.id)
WHERE h.schema_name = 'public' AND h.table_name = 'chunk_test'
ORDER BY c.id, d.id;

\c :TEST_DBNAME :ROLE_SUPERUSER
SELECT set_number_partitions('chunk_test', 3);
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
SELECT set_chunk_time_interval('chunk_test', 1::bigint);

INSERT INTO chunk_test VALUES (8, 24.3, 11233, 1);

SELECT set_chunk_time_interval('chunk_test', 5::bigint);

SELECT * FROM _timescaledb_catalog.dimension;
INSERT INTO chunk_test VALUES (7, 24.3, 79669, 1);
INSERT INTO chunk_test VALUES (8, 24.3, 79669, 1);
INSERT INTO chunk_test VALUES (10, 24.3, 11233, 1);
INSERT INTO chunk_test VALUES (16, 24.3, 11233, 1);

SELECT c.table_name AS chunk_name, d.id AS dimension_id, ds.id AS slice_id, range_start, range_end FROM _timescaledb_catalog.chunk c
LEFT JOIN _timescaledb_catalog.chunk_constraint cc ON (c.id = cc.chunk_id)
LEFT JOIN _timescaledb_catalog.dimension_slice ds ON (ds.id = cc.dimension_slice_id)
LEFT JOIN _timescaledb_catalog.dimension d ON (d.id = ds.dimension_id)
LEFT JOIN _timescaledb_catalog.hypertable h ON (d.hypertable_id = h.id)
WHERE h.schema_name = 'public' AND h.table_name = 'chunk_test'
ORDER BY c.id, d.id;


--test the edges of an open partition -- INT_64_MAX and INT_64_MIN.
CREATE TABLE chunk_test_ends(time bigint, temp float8, tag integer, color integer);
SELECT create_hypertable('chunk_test_ends', 'time', chunk_time_interval => 5);

INSERT INTO chunk_test_ends VALUES ((-9223372036854775808)::bigint, 23.1, 11233, 1);
INSERT INTO chunk_test_ends VALUES (9223372036854775807::bigint, 24.1, 11233, 1);

--try to hit cache
INSERT INTO chunk_test_ends VALUES (9223372036854775807::bigint, 24.2, 11233, 1);
INSERT INTO chunk_test_ends VALUES (9223372036854775807::bigint, 24.3, 11233, 1), (9223372036854775807::bigint, 24.4, 11233, 1);

INSERT INTO chunk_test_ends VALUES ((-9223372036854775808)::bigint, 23.2, 11233, 1);
INSERT INTO chunk_test_ends VALUES ((-9223372036854775808)::bigint, 23.3, 11233, 1), ((-9223372036854775808)::bigint, 23.4, 11233, 1);

SELECT * FROM chunk_test_ends ORDER BY time asc, tag;

--further tests of set_chunk_time_interval
CREATE TABLE chunk_test2(time TIMESTAMPTZ, temp float8, tag integer, color integer);
SELECT create_hypertable('chunk_test2', 'time', 'tag', 2, chunk_time_interval => 3);
SELECT interval_length
FROM _timescaledb_catalog.dimension d
LEFT JOIN _timescaledb_catalog.hypertable h ON (d.hypertable_id = h.id)
WHERE h.schema_name = 'public' AND h.table_name = 'chunk_test2'
ORDER BY d.id;

-- should work since time column is non-INT
SELECT set_chunk_time_interval('chunk_test2', INTERVAL '1 minute');
SELECT interval_length
FROM _timescaledb_catalog.dimension d
LEFT JOIN _timescaledb_catalog.hypertable h ON (d.hypertable_id = h.id)
WHERE h.schema_name = 'public' AND h.table_name = 'chunk_test2'
ORDER BY d.id;

-- should still work for non-INT time columns
SELECT set_chunk_time_interval('chunk_test2', 1000000);
SELECT interval_length
FROM _timescaledb_catalog.dimension d
LEFT JOIN _timescaledb_catalog.hypertable h ON (d.hypertable_id = h.id)
WHERE h.schema_name = 'public' AND h.table_name = 'chunk_test2'
ORDER BY d.id;

\set ON_ERROR_STOP 0
select set_chunk_time_interval(NULL,NULL::interval);
-- should fail since time column is an int
SELECT set_chunk_time_interval('chunk_test', INTERVAL '1 minute');
-- should fail since its not a valid way to represent time
SELECT set_chunk_time_interval('chunk_test', 'foo'::TEXT);
SELECT set_chunk_time_interval('chunk_test', NULL::BIGINT);
SELECT set_chunk_time_interval('chunk_test2', NULL::BIGINT);
SELECT set_chunk_time_interval('chunk_test2', NULL::INTERVAL);
\set ON_ERROR_STOP 1
