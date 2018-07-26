-- Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
--
-- This file is licensed under the Apache License,
-- see LICENSE-APACHE at the top level directory.

CREATE OR REPLACE FUNCTION dimension_get_time(
    hypertable_id INT
)
    RETURNS _timescaledb_catalog.dimension LANGUAGE SQL STABLE AS
$BODY$
    SELECT *
    FROM _timescaledb_catalog.dimension d
    WHERE d.hypertable_id = dimension_get_time.hypertable_id AND
          d.interval_length IS NOT NULL
$BODY$;

CREATE TABLE PUBLIC.drop_chunk_test1(time bigint, temp float8, device_id text);
CREATE TABLE PUBLIC.drop_chunk_test2(time bigint, temp float8, device_id text);
CREATE TABLE PUBLIC.drop_chunk_test3(time bigint, temp float8, device_id text);
CREATE INDEX ON drop_chunk_test1(time DESC);

-- show_chunks() returns 0 rows when there are no hypertables
SELECT show_chunks();

SELECT create_hypertable('public.drop_chunk_test1', 'time', chunk_time_interval => 1, create_default_indexes=>false);
SELECT create_hypertable('public.drop_chunk_test2', 'time', chunk_time_interval => 1, create_default_indexes=>false);
SELECT create_hypertable('public.drop_chunk_test3', 'time', chunk_time_interval => 1, create_default_indexes=>false);

-- Add space dimensions to ensure chunks share dimension slices
SELECT add_dimension('public.drop_chunk_test1', 'device_id', 2);
SELECT add_dimension('public.drop_chunk_test2', 'device_id', 2);
SELECT add_dimension('public.drop_chunk_test3', 'device_id', 2);

--should work becasue so far all tables have time column type of bigint
SELECT show_chunks();

SELECT c.id AS chunk_id, c.hypertable_id, c.schema_name AS chunk_schema, c.table_name AS chunk_table, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.hypertable h ON (c.hypertable_id = h.id)
INNER JOIN  dimension_get_time(h.id) time_dimension ON(true)
INNER JOIN  _timescaledb_catalog.dimension_slice ds ON (ds.dimension_id = time_dimension.id)
INNER JOIN  _timescaledb_catalog.chunk_constraint cc ON (cc.dimension_slice_id = ds.id AND cc.chunk_id = c.id)
WHERE h.schema_name = 'public' AND (h.table_name = 'drop_chunk_test1' OR h.table_name = 'drop_chunk_test2');

\dt "_timescaledb_internal"._hyper*

SELECT  _timescaledb_internal.get_partition_for_key('dev1'::text);
SELECT  _timescaledb_internal.get_partition_for_key('dev7'::varchar(5));

INSERT INTO PUBLIC.drop_chunk_test1 VALUES(1, 1.0, 'dev1');
INSERT INTO PUBLIC.drop_chunk_test1 VALUES(2, 2.0, 'dev1');
INSERT INTO PUBLIC.drop_chunk_test1 VALUES(3, 3.0, 'dev1');
INSERT INTO PUBLIC.drop_chunk_test1 VALUES(4, 4.0, 'dev7');
INSERT INTO PUBLIC.drop_chunk_test1 VALUES(5, 5.0, 'dev7');
INSERT INTO PUBLIC.drop_chunk_test1 VALUES(6, 6.0, 'dev7');

INSERT INTO PUBLIC.drop_chunk_test2 VALUES(1, 1.0, 'dev1');
INSERT INTO PUBLIC.drop_chunk_test2 VALUES(2, 2.0, 'dev1');
INSERT INTO PUBLIC.drop_chunk_test2 VALUES(3, 3.0, 'dev1');
INSERT INTO PUBLIC.drop_chunk_test2 VALUES(4, 4.0, 'dev7');
INSERT INTO PUBLIC.drop_chunk_test2 VALUES(5, 5.0, 'dev7');
INSERT INTO PUBLIC.drop_chunk_test2 VALUES(6, 6.0, 'dev7');

INSERT INTO PUBLIC.drop_chunk_test3 VALUES(1, 1.0, 'dev1');
INSERT INTO PUBLIC.drop_chunk_test3 VALUES(2, 2.0, 'dev1');
INSERT INTO PUBLIC.drop_chunk_test3 VALUES(3, 3.0, 'dev1');
INSERT INTO PUBLIC.drop_chunk_test3 VALUES(4, 4.0, 'dev7');
INSERT INTO PUBLIC.drop_chunk_test3 VALUES(5, 5.0, 'dev7');
INSERT INTO PUBLIC.drop_chunk_test3 VALUES(6, 6.0, 'dev7');

SELECT c.id AS chunk_id, c.hypertable_id, c.schema_name AS chunk_schema, c.table_name AS chunk_table, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.hypertable h ON (c.hypertable_id = h.id)
INNER JOIN  dimension_get_time(h.id) time_dimension ON(true)
INNER JOIN  _timescaledb_catalog.dimension_slice ds ON (ds.dimension_id = time_dimension.id)
INNER JOIN  _timescaledb_catalog.chunk_constraint cc ON (cc.dimension_slice_id = ds.id AND cc.chunk_id = c.id)
WHERE h.schema_name = 'public' AND (h.table_name = 'drop_chunk_test1' OR h.table_name = 'drop_chunk_test2');
\dt "_timescaledb_internal"._hyper*

-- next two calls of show_chunks should give same set of chunks as above when combined
SELECT show_chunks('drop_chunk_test1');
SELECT * FROM show_chunks('drop_chunk_test2');

CREATE VIEW dependent_view AS SELECT * FROM _timescaledb_internal._hyper_1_1_chunk;

\set ON_ERROR_STOP 0
SELECT drop_chunks(2);
SELECT drop_chunks(NULL::interval);
SELECT drop_chunks(NULL::int);
-- error messages were refactored in postgres between 9.6 and 10 because of
-- which direct comparison of error messages fails.
-- The change in postgres happened here:
-- https://github.com/postgres/postgres/commit/9a34123bc315e55b33038464422ef1cd2b67dab2
SET client_min_messages TO FATAL;
-- error message is suppressed for the reason above but this test should still make sure
-- this causes an error. Not returning # rows below can be used as an indication that
-- this did cause an error.
-- should error because not a time type
SELECT drop_chunks('haha', 'drop_chunk_test3');
SET client_min_messages TO DEFAULT;

SELECT show_chunks('drop_chunk_test3', 'haha');

-- should error because wrong time type
SELECT drop_chunks(now(), 'drop_chunk_test3');
SELECT show_chunks('drop_chunk_test3', now());

-- should error because of wrong relative order of time constraints
SELECT show_chunks('drop_chunk_test1', older_than=>3, newer_than=>4);

\set ON_ERROR_STOP 1

--should always work regardless of time column types of hypertables
SELECT show_chunks();

-- should work vecause so far all tables have time column type of bigint
SELECT show_chunks(newer_than => 2);

-- show created constraints and dimension slices for each chunk
SELECT c.table_name, cc.constraint_name, ds.id AS dimension_slice_id, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.chunk_constraint cc ON (c.id = cc.chunk_id)
FULL OUTER JOIN _timescaledb_catalog.dimension_slice ds ON (ds.id = cc.dimension_slice_id);
SELECT * FROM _timescaledb_catalog.dimension_slice;

-- Drop one chunk "manually" and verify that dimension slices and
-- constraints are cleaned up. Each chunk has two constraints and two
-- dimension slices. Both constraints should be deleted, but only one
-- slice should be deleted since the space-dimension slice is shared
-- with other chunks in the same hypertable
DROP TABLE _timescaledb_internal._hyper_2_7_chunk;

-- Two constraints deleted compared to above
SELECT c.table_name, cc.constraint_name, ds.id AS dimension_slice_id, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.chunk_constraint cc ON (c.id = cc.chunk_id)
FULL OUTER JOIN _timescaledb_catalog.dimension_slice ds ON (ds.id = cc.dimension_slice_id);
-- Only one dimension slice deleted
SELECT * FROM _timescaledb_catalog.dimension_slice;

SELECT drop_chunks(2, CASCADE=>true);

SELECT c.table_name, cc.constraint_name, ds.id AS dimension_slice_id, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.chunk_constraint cc ON (c.id = cc.chunk_id)
FULL OUTER JOIN _timescaledb_catalog.dimension_slice ds ON (ds.id = cc.dimension_slice_id);
SELECT * FROM _timescaledb_catalog.dimension_slice;

SELECT c.id AS chunk_id, c.hypertable_id, c.schema_name AS chunk_schema, c.table_name AS chunk_table, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.hypertable h ON (c.hypertable_id = h.id)
INNER JOIN  dimension_get_time(h.id) time_dimension ON(true)
INNER JOIN  _timescaledb_catalog.dimension_slice ds ON (ds.dimension_id = time_dimension.id)
INNER JOIN  _timescaledb_catalog.chunk_constraint cc ON (cc.dimension_slice_id = ds.id AND cc.chunk_id = c.id)
WHERE h.schema_name = 'public' AND (h.table_name = 'drop_chunk_test1' OR h.table_name = 'drop_chunk_test2');

-- next two calls of show_chunks should give same set of chunks as above when combined
SELECT show_chunks('drop_chunk_test1');
SELECT * FROM show_chunks('drop_chunk_test2');

\dt "_timescaledb_internal"._hyper*

SELECT drop_chunks(3, 'drop_chunk_test1');

SELECT c.id AS chunk_id, c.hypertable_id, c.schema_name AS chunk_schema, c.table_name AS chunk_table, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.hypertable h ON (c.hypertable_id = h.id)
INNER JOIN  dimension_get_time(h.id) time_dimension ON(true)
INNER JOIN  _timescaledb_catalog.dimension_slice ds ON (ds.dimension_id = time_dimension.id)
INNER JOIN  _timescaledb_catalog.chunk_constraint cc ON (cc.dimension_slice_id = ds.id AND cc.chunk_id = c.id)
WHERE h.schema_name = 'public' AND (h.table_name = 'drop_chunk_test1' OR h.table_name = 'drop_chunk_test2');

\dt "_timescaledb_internal".*

-- next two calls of show_chunks should give same set of chunks as above when combined
SELECT show_chunks('drop_chunk_test1');
SELECT * FROM show_chunks('drop_chunk_test2');

-- 2,147,483,647 is the largest int so this tests that BIGINTs work
SELECT drop_chunks(2147483648, 'drop_chunk_test3');

SELECT c.id AS chunk_id, c.hypertable_id, c.schema_name AS chunk_schema, c.table_name AS chunk_table, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.hypertable h ON (c.hypertable_id = h.id)
INNER JOIN  dimension_get_time(h.id) time_dimension ON(true)
INNER JOIN  _timescaledb_catalog.dimension_slice ds ON (ds.dimension_id = time_dimension.id)
INNER JOIN  _timescaledb_catalog.chunk_constraint cc ON (cc.dimension_slice_id = ds.id AND cc.chunk_id = c.id)
WHERE h.schema_name = 'public' AND (h.table_name = 'drop_chunk_test1' OR h.table_name = 'drop_chunk_test2' OR h.table_name = 'drop_chunk_test3');

\dt "_timescaledb_internal"._hyper*
\set ON_ERROR_STOP 0
-- should fail because too dangerous to assume anything
-- Note that currently this failure is triggered by SQL typecheker when it tries and fails to resolve
-- ANYELEMENT args to a concrete type. Explicit error checking may be necessary if drop_chunks implementation
-- is fully ported to C.
-- commented out because there is discrepancy between postgres 9.6 and postgres 10 error messages for this case
-- SELECT drop_chunks();


-- should error because no hypertable
SELECT drop_chunks(5, 'drop_chunk_test4');
SELECT show_chunks('drop_chunk_test4');
SELECT show_chunks('drop_chunk_test4', 5);
\set ON_ERROR_STOP 1

DROP TABLE _timescaledb_internal._hyper_1_6_chunk;

SELECT c.id AS chunk_id, c.hypertable_id, c.schema_name AS chunk_schema, c.table_name AS chunk_table, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.hypertable h ON (c.hypertable_id = h.id)
INNER JOIN  dimension_get_time(h.id) time_dimension ON(true)
INNER JOIN  _timescaledb_catalog.dimension_slice ds ON (ds.dimension_id = time_dimension.id)
INNER JOIN  _timescaledb_catalog.chunk_constraint cc ON (cc.dimension_slice_id = ds.id AND cc.chunk_id = c.id)
WHERE h.schema_name = 'public' AND (h.table_name = 'drop_chunk_test1' OR h.table_name = 'drop_chunk_test2');

\dt "_timescaledb_internal"._hyper*

-- newer_than tests
SELECT drop_chunks(table_name=>'drop_chunk_test1', newer_than=>5);

SELECT c.id AS chunk_id, c.hypertable_id, c.schema_name AS chunk_schema, c.table_name AS chunk_table, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.hypertable h ON (c.hypertable_id = h.id)
INNER JOIN  dimension_get_time(h.id) time_dimension ON(true)
INNER JOIN  _timescaledb_catalog.dimension_slice ds ON (ds.dimension_id = time_dimension.id)
INNER JOIN  _timescaledb_catalog.chunk_constraint cc ON (cc.dimension_slice_id = ds.id AND cc.chunk_id = c.id)
WHERE h.schema_name = 'public' AND (h.table_name = 'drop_chunk_test1');

SELECT show_chunks('drop_chunk_test1');

\dt "_timescaledb_internal"._hyper*

SELECT drop_chunks(table_name=>'drop_chunk_test1', older_than=>4, newer_than=>3);

SELECT c.id AS chunk_id, c.hypertable_id, c.schema_name AS chunk_schema, c.table_name AS chunk_table, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.hypertable h ON (c.hypertable_id = h.id)
INNER JOIN  dimension_get_time(h.id) time_dimension ON(true)
INNER JOIN  _timescaledb_catalog.dimension_slice ds ON (ds.dimension_id = time_dimension.id)
INNER JOIN  _timescaledb_catalog.chunk_constraint cc ON (cc.dimension_slice_id = ds.id AND cc.chunk_id = c.id)
WHERE h.schema_name = 'public' AND (h.table_name = 'drop_chunk_test1');

-- the call of show_chunks should give same set of chunks as above
SELECT show_chunks('drop_chunk_test1');

-- testing drop_chunks when only schema is specified.
\set ON_ERROR_STOP 0
-- error messages were refactored in postgres between 9.6 and 10 because of
-- which direct comparison of error messages fails.
SET client_min_messages TO FATAL;
SELECT drop_chunks(schema_name=>'public');
SET client_min_messages TO DEFAULT;
SELECT drop_chunks(null::bigint, schema_name=>'public');
\set ON_ERROR_STOP 1

SELECT c.id AS chunk_id, c.hypertable_id, c.schema_name AS chunk_schema, c.table_name AS chunk_table, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.hypertable h ON (c.hypertable_id = h.id)
INNER JOIN  dimension_get_time(h.id) time_dimension ON(true)
INNER JOIN  _timescaledb_catalog.dimension_slice ds ON (ds.dimension_id = time_dimension.id)
INNER JOIN  _timescaledb_catalog.chunk_constraint cc ON (cc.dimension_slice_id = ds.id AND cc.chunk_id = c.id)
WHERE h.schema_name = 'public';

SELECT drop_chunks(5, schema_name=>'public', newer_than=>4);

SELECT c.id AS chunk_id, c.hypertable_id, c.schema_name AS chunk_schema, c.table_name AS chunk_table, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.hypertable h ON (c.hypertable_id = h.id)
INNER JOIN  dimension_get_time(h.id) time_dimension ON(true)
INNER JOIN  _timescaledb_catalog.dimension_slice ds ON (ds.dimension_id = time_dimension.id)
INNER JOIN  _timescaledb_catalog.chunk_constraint cc ON (cc.dimension_slice_id = ds.id AND cc.chunk_id = c.id)
WHERE h.schema_name = 'public';

CREATE TABLE PUBLIC.drop_chunk_test_ts(time timestamp, temp float8, device_id text);
SELECT create_hypertable('public.drop_chunk_test_ts', 'time', chunk_time_interval => interval '1 minute', create_default_indexes=>false);

CREATE TABLE PUBLIC.drop_chunk_test_tstz(time timestamptz, temp float8, device_id text);
SELECT create_hypertable('public.drop_chunk_test_tstz', 'time', chunk_time_interval => interval '1 minute', create_default_indexes=>false);

SET timezone = '+1';
INSERT INTO PUBLIC.drop_chunk_test_ts VALUES(now()-INTERVAL '5 minutes', 1.0, 'dev1');
INSERT INTO PUBLIC.drop_chunk_test_tstz VALUES(now()-INTERVAL '5 minutes', 1.0, 'dev1');

SELECT * FROM test.show_subtables('drop_chunk_test_ts');
SELECT * FROM test.show_subtables('drop_chunk_test_tstz');

BEGIN;
    SELECT show_chunks('drop_chunk_test_ts');
    SELECT show_chunks('drop_chunk_test_ts', now()::timestamp-interval '1 minute');
    SELECT drop_chunks(newer_than => interval '1 minute', table_name => 'drop_chunk_test_ts');
    SELECT drop_chunks(older_than => interval '6 minute', table_name => 'drop_chunk_test_ts');

    SELECT * FROM test.show_subtables('drop_chunk_test_ts');
    SELECT drop_chunks(interval '1 minute', 'drop_chunk_test_ts');
    SELECT * FROM test.show_subtables('drop_chunk_test_ts');
    SELECT show_chunks('drop_chunk_test_tstz');
    SELECT show_chunks('drop_chunk_test_tstz', now() - interval '1 minute', now() - interval '6 minute');
    SELECT show_chunks('drop_chunk_test_tstz', newer_than => now() - interval '1 minute');
    SELECT show_chunks('drop_chunk_test_tstz', older_than => now() - interval '1 minute');

    SELECT drop_chunks(interval '1 minute', 'drop_chunk_test_tstz');
    SELECT * FROM test.show_subtables('drop_chunk_test_tstz');
ROLLBACK;

BEGIN;
    SELECT drop_chunks(newer_than => interval '6 minute', table_name => 'drop_chunk_test_ts');
    SELECT * FROM test.show_subtables('drop_chunk_test_ts');
ROLLBACK;

BEGIN;
    SELECT drop_chunks(interval '1 minute', 'drop_chunk_test_ts');
    SELECT * FROM test.show_subtables('drop_chunk_test_ts');
    SELECT drop_chunks(interval '1 minute', 'drop_chunk_test_tstz');
    SELECT * FROM test.show_subtables('drop_chunk_test_tstz');
ROLLBACK;

BEGIN;
    SELECT drop_chunks(now()::timestamp-interval '1 minute', 'drop_chunk_test_ts');
    SELECT * FROM test.show_subtables('drop_chunk_test_ts');
    SELECT drop_chunks(now()-interval '1 minute', 'drop_chunk_test_tstz');
    SELECT * FROM test.show_subtables('drop_chunk_test_tstz');
ROLLBACK;

\dt "_timescaledb_internal"._hyper*
SELECT show_chunks();

\set ON_ERROR_STOP 0
SELECT show_chunks(older_than=>4);
SELECT drop_chunks(4);
SELECT drop_chunks(interval '1 minute');
SELECT drop_chunks(interval '1 minute', 'drop_chunk_test3');
SELECT drop_chunks(now()-interval '1 minute', 'drop_chunk_test_ts');
SELECT drop_chunks(now()::timestamp-interval '1 minute', 'drop_chunk_test_tstz');
SELECT drop_chunks(5, schema_name=>'public', newer_than=>4);
\set ON_ERROR_STOP 1

\dt "_timescaledb_internal"._hyper*

CREATE TABLE PUBLIC.drop_chunk_test_date(time date, temp float8, device_id text);
SELECT create_hypertable('public.drop_chunk_test_date', 'time', chunk_time_interval => interval '1 day', create_default_indexes=>false);

SET timezone = '+100';
INSERT INTO PUBLIC.drop_chunk_test_date VALUES(now()-INTERVAL '2 day', 1.0, 'dev1');

BEGIN;
    SELECT drop_chunks(interval '1 day', 'drop_chunk_test_date');
    SELECT * FROM test.show_subtables('drop_chunk_test_date');
ROLLBACK;

BEGIN;
    SELECT drop_chunks((now()-interval '1 day')::date, 'drop_chunk_test_date');
    SELECT * FROM test.show_subtables('drop_chunk_test_date');
ROLLBACK;
