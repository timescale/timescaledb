-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

--
-- Test chunk origin parameter functionality
--
-- The origin parameter allows specifying a reference point for aligning
-- chunk boundaries. Chunks are aligned to the origin instead of Unix epoch.
--

\c :TEST_DBNAME :ROLE_SUPERUSER
SET ROLE :ROLE_DEFAULT_PERM_USER;
\set VERBOSITY terse

SET timezone = 'UTC';

---------------------------------------------------------------
-- ORIGIN WITH FIXED-SIZE CHUNKS (MICROSECONDS)
---------------------------------------------------------------
-- Test that origin can be specified for fixed-size chunk alignment.

-- Create hypertable with origin (chunks aligned to noon instead of midnight)
CREATE TABLE origin_noon(time timestamptz NOT NULL, value int);
SELECT create_hypertable('origin_noon', 'time',
    chunk_time_interval => 86400000000,  -- 1 day in microseconds
    chunk_time_origin => '2020-01-01 12:00:00 UTC'::timestamptz);

-- Verify origin is stored in catalog
SELECT d.column_name,
       _timescaledb_functions.to_timestamp(d.interval_origin) as origin
FROM _timescaledb_catalog.dimension d
JOIN _timescaledb_catalog.hypertable h ON d.hypertable_id = h.id
WHERE h.table_name = 'origin_noon';

-- Verify dimensions view shows origin
SELECT hypertable_name, time_interval, time_origin
FROM timescaledb_information.dimensions
WHERE hypertable_name = 'origin_noon';

-- Insert data around the origin boundary (noon)
INSERT INTO origin_noon VALUES
    ('2020-01-01 11:00:00 UTC', 1),  -- Before origin, previous chunk
    ('2020-01-01 12:00:00 UTC', 2),  -- At origin, starts new chunk
    ('2020-01-01 18:00:00 UTC', 3),  -- Same chunk as origin
    ('2020-01-02 11:59:59 UTC', 4),  -- Still same chunk (ends at noon)
    ('2020-01-02 12:00:00 UTC', 5);  -- Next chunk starts at noon

-- Verify chunks are aligned to noon (origin), not midnight
SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks
WHERE hypertable_name = 'origin_noon'
ORDER BY range_start;

-- Verify data is in correct chunks
SELECT tableoid::regclass as chunk, time, value
FROM origin_noon ORDER BY time;

DROP TABLE origin_noon;

---------------------------------------------------------------
-- SET_CHUNK_TIME_INTERVAL WITH ORIGIN
---------------------------------------------------------------
-- Test set_chunk_time_interval with origin parameter

CREATE TABLE origin_update(time timestamptz NOT NULL, value int);
SELECT create_hypertable('origin_update', 'time',
    chunk_time_interval => 86400000000);  -- 1 day in microseconds

-- Initially no origin
SELECT hypertable_name, time_interval, time_origin
FROM timescaledb_information.dimensions
WHERE hypertable_name = 'origin_update';

-- Update with origin (chunks start at 6:00)
SELECT set_chunk_time_interval('origin_update', 43200000000,  -- 12 hours
    chunk_time_origin => '2020-01-01 06:00:00 UTC'::timestamptz);

-- Verify origin is now set
SELECT hypertable_name, time_interval, time_origin
FROM timescaledb_information.dimensions
WHERE hypertable_name = 'origin_update';

-- Insert data and verify chunks align to 6:00
INSERT INTO origin_update VALUES
    ('2020-01-01 05:00:00 UTC', 1),  -- Before 6:00, previous chunk
    ('2020-01-01 06:00:00 UTC', 2),  -- At origin
    ('2020-01-01 17:59:59 UTC', 3),  -- Same chunk
    ('2020-01-01 18:00:00 UTC', 4);  -- Next chunk (6:00 + 12 hours)

SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks
WHERE hypertable_name = 'origin_update'
ORDER BY range_start;

DROP TABLE origin_update;

---------------------------------------------------------------
-- INTEGER COLUMNS WITH INTEGER ORIGIN
---------------------------------------------------------------
-- Test integer columns with integer origin

CREATE TABLE int_origin(time bigint NOT NULL, value int);
SELECT create_hypertable('int_origin', 'time',
    chunk_time_interval => 1000,
    chunk_time_origin => 500);

-- Verify dimensions view shows integer_origin (not time_origin)
SELECT hypertable_name, integer_interval, integer_origin, time_interval, time_origin
FROM timescaledb_information.dimensions
WHERE hypertable_name = 'int_origin';

-- Insert data to verify chunk alignment relative to origin=500
-- Chunks should be: [..., -500 to 500, 500 to 1500, 1500 to 2500, ...]
INSERT INTO int_origin VALUES (400, 1);  -- chunk [-500, 500)
INSERT INTO int_origin VALUES (500, 2);  -- chunk [500, 1500)
INSERT INTO int_origin VALUES (1200, 3); -- chunk [500, 1500)
INSERT INTO int_origin VALUES (1500, 4); -- chunk [1500, 2500)

SELECT c.table_name AS chunk_name, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c
JOIN _timescaledb_catalog.chunk_constraint cc ON c.id = cc.chunk_id
JOIN _timescaledb_catalog.dimension_slice ds ON cc.dimension_slice_id = ds.id
WHERE c.hypertable_id = (SELECT id FROM _timescaledb_catalog.hypertable WHERE table_name = 'int_origin')
ORDER BY ds.range_start;

DROP TABLE int_origin;

---------------------------------------------------------------
-- ADD_DIMENSION WITH ORIGIN
---------------------------------------------------------------
-- Test add_dimension() with origin parameter (deprecated API)

CREATE TABLE add_dim_origin(id int NOT NULL, time timestamptz NOT NULL, value int);
SELECT create_hypertable('add_dim_origin', 'id', chunk_time_interval => 100);
SELECT add_dimension('add_dim_origin', 'time',
    chunk_time_interval => 86400000000,  -- 1 day in microseconds
    chunk_time_origin => '2024-04-01 00:00:00 UTC'::timestamptz);

-- Verify the origin was stored correctly in the dimension
SELECT d.column_name,
       _timescaledb_functions.to_timestamp(d.interval_origin) AS origin
FROM _timescaledb_catalog.dimension d
JOIN _timescaledb_catalog.hypertable h ON d.hypertable_id = h.id
WHERE h.table_name = 'add_dim_origin' AND d.column_name = 'time';

DROP TABLE add_dim_origin;

---------------------------------------------------------------
-- TYPE VALIDATION TESTS
---------------------------------------------------------------
-- Test type validation: timestamp origin with integer column (should fail)

\set ON_ERROR_STOP 0
CREATE TABLE int_origin_ts_error(time bigint NOT NULL, value int);
SELECT create_hypertable('int_origin_ts_error', 'time',
    chunk_time_interval => 1000,
    chunk_time_origin => '2001-01-01'::timestamptz);
DROP TABLE IF EXISTS int_origin_ts_error;

-- Test type validation: integer origin with timestamp column (should fail)
CREATE TABLE ts_origin_int_error(time timestamptz NOT NULL, value int);
SELECT create_hypertable('ts_origin_int_error', 'time',
    chunk_time_interval => 86400000000,
    chunk_time_origin => 0);
DROP TABLE IF EXISTS ts_origin_int_error;
\set ON_ERROR_STOP 1

---------------------------------------------------------------
-- BY_RANGE WITH PARTITION_ORIGIN
---------------------------------------------------------------
-- Test by_range() with partition_origin parameter

CREATE TABLE by_range_origin(time timestamptz NOT NULL, value int);
SELECT create_hypertable('by_range_origin',
    by_range('time', '1 day'::interval, partition_origin => '2020-01-01 06:00:00 UTC'::timestamptz));

-- Verify origin is stored correctly
SELECT hypertable_name, time_interval, time_origin
FROM timescaledb_information.dimensions
WHERE hypertable_name = 'by_range_origin';

-- Verify chunks align to 6:00
INSERT INTO by_range_origin VALUES
    ('2020-01-01 05:00:00 UTC', 1),  -- Before 6:00, previous chunk
    ('2020-01-01 06:00:00 UTC', 2),  -- At origin
    ('2020-01-02 05:59:59 UTC', 3),  -- Same chunk (ends at 6:00)
    ('2020-01-02 06:00:00 UTC', 4);  -- Next chunk starts at 6:00

SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks
WHERE hypertable_name = 'by_range_origin'
ORDER BY range_start;

DROP TABLE by_range_origin;

---------------------------------------------------------------
-- CREATE TABLE WITH (ORIGIN = ...) SYNTAX
---------------------------------------------------------------
-- Test CREATE TABLE WITH syntax for origin parameter

-- Test with timestamptz column
CREATE TABLE with_origin_ts(time timestamptz NOT NULL, value int)
    WITH (tsdb.hypertable, tsdb.partition_column='time',
          tsdb.chunk_interval='1 day',
          tsdb.origin='2020-01-01 12:00:00 UTC');

-- Verify origin is stored correctly
SELECT hypertable_name, time_interval, time_origin
FROM timescaledb_information.dimensions
WHERE hypertable_name = 'with_origin_ts';

-- Insert data to verify chunk alignment to noon
INSERT INTO with_origin_ts VALUES
    ('2020-01-01 11:00:00 UTC', 1),  -- Before origin, previous chunk
    ('2020-01-01 12:00:00 UTC', 2),  -- At origin, starts new chunk
    ('2020-01-02 11:59:59 UTC', 3),  -- Same chunk (ends at noon)
    ('2020-01-02 12:00:00 UTC', 4);  -- Next chunk starts at noon

SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks
WHERE hypertable_name = 'with_origin_ts'
ORDER BY range_start;

DROP TABLE with_origin_ts;

-- Test with partition_origin alias
CREATE TABLE with_partition_origin(time timestamptz NOT NULL, value int)
    WITH (tsdb.hypertable, tsdb.partition_column='time',
          tsdb.chunk_interval='12 hours',
          tsdb.partition_origin='2020-01-01 06:00:00 UTC');

SELECT hypertable_name, time_interval, time_origin
FROM timescaledb_information.dimensions
WHERE hypertable_name = 'with_partition_origin';

DROP TABLE with_partition_origin;

-- Test with integer column and origin
CREATE TABLE with_origin_int(id bigint NOT NULL, value int)
    WITH (tsdb.hypertable, tsdb.partition_column='id',
          tsdb.chunk_interval=1000,
          tsdb.origin='500');

SELECT hypertable_name, integer_interval, integer_origin
FROM timescaledb_information.dimensions
WHERE hypertable_name = 'with_origin_int';

-- Verify chunks align to origin=500
INSERT INTO with_origin_int VALUES (400, 1);   -- chunk [-500, 500)
INSERT INTO with_origin_int VALUES (500, 2);   -- chunk [500, 1500)
INSERT INTO with_origin_int VALUES (1500, 3);  -- chunk [1500, 2500)

SELECT c.table_name AS chunk_name, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c
JOIN _timescaledb_catalog.chunk_constraint cc ON c.id = cc.chunk_id
JOIN _timescaledb_catalog.dimension_slice ds ON cc.dimension_slice_id = ds.id
WHERE c.hypertable_id = (SELECT id FROM _timescaledb_catalog.hypertable WHERE table_name = 'with_origin_int')
ORDER BY ds.range_start;

DROP TABLE with_origin_int;

-- Test with smallint column and origin
CREATE TABLE with_origin_smallint(id smallint NOT NULL, value int)
    WITH (tsdb.hypertable, tsdb.partition_column='id',
          tsdb.chunk_interval='100',
          tsdb.origin='50');

SELECT hypertable_name, integer_interval, integer_origin
FROM timescaledb_information.dimensions
WHERE hypertable_name = 'with_origin_smallint';

INSERT INTO with_origin_smallint VALUES (40, 1);   -- chunk [-50, 50)
INSERT INTO with_origin_smallint VALUES (50, 2);   -- chunk [50, 150)

SELECT c.table_name AS chunk_name, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c
JOIN _timescaledb_catalog.chunk_constraint cc ON c.id = cc.chunk_id
JOIN _timescaledb_catalog.dimension_slice ds ON cc.dimension_slice_id = ds.id
WHERE c.hypertable_id = (SELECT id FROM _timescaledb_catalog.hypertable WHERE table_name = 'with_origin_smallint')
ORDER BY ds.range_start;

DROP TABLE with_origin_smallint;

-- Test with int column and origin
CREATE TABLE with_origin_int4(id int NOT NULL, value int)
    WITH (tsdb.hypertable, tsdb.partition_column='id',
          tsdb.chunk_interval='1000',
          tsdb.origin='500');

SELECT hypertable_name, integer_interval, integer_origin
FROM timescaledb_information.dimensions
WHERE hypertable_name = 'with_origin_int4';

INSERT INTO with_origin_int4 VALUES (400, 1);   -- chunk [-500, 500)
INSERT INTO with_origin_int4 VALUES (500, 2);   -- chunk [500, 1500)

SELECT c.table_name AS chunk_name, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c
JOIN _timescaledb_catalog.chunk_constraint cc ON c.id = cc.chunk_id
JOIN _timescaledb_catalog.dimension_slice ds ON cc.dimension_slice_id = ds.id
WHERE c.hypertable_id = (SELECT id FROM _timescaledb_catalog.hypertable WHERE table_name = 'with_origin_int4')
ORDER BY ds.range_start;

DROP TABLE with_origin_int4;

-- Test with timestamp (without timezone) column and origin
CREATE TABLE with_origin_timestamp(time timestamp NOT NULL, value int)
    WITH (tsdb.hypertable, tsdb.partition_column='time',
          tsdb.chunk_interval='1 day',
          tsdb.origin='2020-01-01 12:00:00');

SELECT hypertable_name, time_interval, time_origin
FROM timescaledb_information.dimensions
WHERE hypertable_name = 'with_origin_timestamp';

INSERT INTO with_origin_timestamp VALUES
    ('2020-01-01 11:00:00', 1),  -- Before origin, previous chunk
    ('2020-01-01 12:00:00', 2),  -- At origin, starts new chunk
    ('2020-01-02 11:59:59', 3);  -- Same chunk

SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks
WHERE hypertable_name = 'with_origin_timestamp'
ORDER BY range_start;

DROP TABLE with_origin_timestamp;

-- Test with date column and origin
CREATE TABLE with_origin_date(day date NOT NULL, value int)
    WITH (tsdb.hypertable, tsdb.partition_column='day',
          tsdb.chunk_interval='7 days',
          tsdb.origin='2020-01-01');

SELECT hypertable_name, time_interval, time_origin
FROM timescaledb_information.dimensions
WHERE hypertable_name = 'with_origin_date';

INSERT INTO with_origin_date VALUES
    ('2019-12-30', 1),  -- Before origin
    ('2020-01-01', 2),  -- At origin
    ('2020-01-07', 3);  -- Next week

SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks
WHERE hypertable_name = 'with_origin_date'
ORDER BY range_start;

DROP TABLE with_origin_date;

-- Test with chunk_origin alias (alternative name)
CREATE TABLE with_chunk_origin(time timestamptz NOT NULL, value int)
    WITH (tsdb.hypertable, tsdb.partition_column='time',
          tsdb.chunk_interval='1 day',
          tsdb.chunk_origin='2020-01-01 06:00:00 UTC');

SELECT hypertable_name, time_interval, time_origin
FROM timescaledb_information.dimensions
WHERE hypertable_name = 'with_chunk_origin';

DROP TABLE with_chunk_origin;

---------------------------------------------------------------
-- CLEANUP
---------------------------------------------------------------
RESET timezone;
\set VERBOSITY default
