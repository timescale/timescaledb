-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Set this variable to avoid using a hard-coded path each time query
-- results are compared
\set QUERY_RESULT_TEST_EQUAL_RELPATH 'include/query_result_test_equal.sql'

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

-- Make sure drop_chunks when there are no tables succeeds
-- show_chunks and drop_chunks output should be the same
\set QUERY1 'SELECT show_chunks(older_than => INTERVAL \'1 hour\')::NAME'
\set QUERY2 'SELECT drop_chunks(INTERVAL \'1 hour\', verbose => true)::NAME'
\set ECHO errors
\ir  :QUERY_RESULT_TEST_EQUAL_RELPATH
\set ECHO all
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
WHERE h.schema_name = 'public' AND (h.table_name = 'drop_chunk_test1' OR h.table_name = 'drop_chunk_test2')
ORDER BY c.id;

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
WHERE h.schema_name = 'public' AND (h.table_name = 'drop_chunk_test1' OR h.table_name = 'drop_chunk_test2')
ORDER BY c.id;
\dt "_timescaledb_internal"._hyper*

-- next two calls of show_chunks should give same set of chunks as above when combined
SELECT show_chunks('drop_chunk_test1');
SELECT * FROM show_chunks('drop_chunk_test2');

CREATE VIEW dependent_view AS SELECT * FROM _timescaledb_internal._hyper_1_1_chunk;

\set ON_ERROR_STOP 0
SELECT drop_chunks();
SELECT drop_chunks(2);
SELECT drop_chunks(NULL::interval);
SELECT drop_chunks(NULL::int);
SELECT drop_chunks('haha', 'drop_chunk_test3');
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
FULL OUTER JOIN _timescaledb_catalog.dimension_slice ds ON (ds.id = cc.dimension_slice_id)
ORDER BY c.id;
SELECT * FROM _timescaledb_catalog.dimension_slice ORDER BY id;

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
FULL OUTER JOIN _timescaledb_catalog.dimension_slice ds ON (ds.id = cc.dimension_slice_id)
ORDER BY c.id;
-- Only one dimension slice deleted
SELECT * FROM _timescaledb_catalog.dimension_slice ORDER BY id;

-- show_chunks and drop_chunks output should be the same
\set QUERY1 'SELECT show_chunks(older_than => 2)::TEXT'
\set QUERY2 'SELECT drop_chunks(2, CASCADE=>true)::TEXT'
\set ECHO errors
\ir  :QUERY_RESULT_TEST_EQUAL_RELPATH
\set ECHO all

SELECT c.table_name, cc.constraint_name, ds.id AS dimension_slice_id, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.chunk_constraint cc ON (c.id = cc.chunk_id)
FULL OUTER JOIN _timescaledb_catalog.dimension_slice ds ON (ds.id = cc.dimension_slice_id)
ORDER BY c.id;
SELECT * FROM _timescaledb_catalog.dimension_slice ORDER BY id;

SELECT c.id AS chunk_id, c.hypertable_id, c.schema_name AS chunk_schema, c.table_name AS chunk_table, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.hypertable h ON (c.hypertable_id = h.id)
INNER JOIN  dimension_get_time(h.id) time_dimension ON(true)
INNER JOIN  _timescaledb_catalog.dimension_slice ds ON (ds.dimension_id = time_dimension.id)
INNER JOIN  _timescaledb_catalog.chunk_constraint cc ON (cc.dimension_slice_id = ds.id AND cc.chunk_id = c.id)
WHERE h.schema_name = 'public' AND (h.table_name = 'drop_chunk_test1' OR h.table_name = 'drop_chunk_test2')
ORDER BY c.id;

-- next two calls of show_chunks should give same set of chunks as above when combined
SELECT show_chunks('drop_chunk_test1');
SELECT * FROM show_chunks('drop_chunk_test2');

\dt "_timescaledb_internal"._hyper*
-- show_chunks and drop_chunks output should be the same
\set QUERY1 'SELECT show_chunks(\'drop_chunk_test1\', older_than => 3)::NAME'
\set QUERY2 'SELECT drop_chunks(3, \'drop_chunk_test1\')::NAME'
\set ECHO errors
\ir  :QUERY_RESULT_TEST_EQUAL_RELPATH
\set ECHO all

SELECT c.id AS chunk_id, c.hypertable_id, c.schema_name AS chunk_schema, c.table_name AS chunk_table, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.hypertable h ON (c.hypertable_id = h.id)
INNER JOIN  dimension_get_time(h.id) time_dimension ON(true)
INNER JOIN  _timescaledb_catalog.dimension_slice ds ON (ds.dimension_id = time_dimension.id)
INNER JOIN  _timescaledb_catalog.chunk_constraint cc ON (cc.dimension_slice_id = ds.id AND cc.chunk_id = c.id)
WHERE h.schema_name = 'public' AND (h.table_name = 'drop_chunk_test1' OR h.table_name = 'drop_chunk_test2')
ORDER BY c.id;

\dt "_timescaledb_internal".*

-- next two calls of show_chunks should give same set of chunks as above when combined
SELECT show_chunks('drop_chunk_test1');
SELECT * FROM show_chunks('drop_chunk_test2');

-- 2,147,483,647 is the largest int so this tests that BIGINTs work
-- show_chunks and drop_chunks output should be the same
\set QUERY1 'SELECT show_chunks(\'drop_chunk_test3\', older_than => 2147483648)::NAME'
\set QUERY2 'SELECT drop_chunks(2147483648, \'drop_chunk_test3\')::NAME'
\set ECHO errors
\ir  :QUERY_RESULT_TEST_EQUAL_RELPATH
\set ECHO all

SELECT c.id AS chunk_id, c.hypertable_id, c.schema_name AS chunk_schema, c.table_name AS chunk_table, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.hypertable h ON (c.hypertable_id = h.id)
INNER JOIN  dimension_get_time(h.id) time_dimension ON(true)
INNER JOIN  _timescaledb_catalog.dimension_slice ds ON (ds.dimension_id = time_dimension.id)
INNER JOIN  _timescaledb_catalog.chunk_constraint cc ON (cc.dimension_slice_id = ds.id AND cc.chunk_id = c.id)
WHERE h.schema_name = 'public' AND (h.table_name = 'drop_chunk_test1' OR h.table_name = 'drop_chunk_test2' OR h.table_name = 'drop_chunk_test3')
ORDER BY c.id;

\dt "_timescaledb_internal"._hyper*
\set ON_ERROR_STOP 0
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
WHERE h.schema_name = 'public' AND (h.table_name = 'drop_chunk_test1' OR h.table_name = 'drop_chunk_test2')
ORDER BY c.id;

\dt "_timescaledb_internal"._hyper*

-- newer_than tests
-- show_chunks and drop_chunks output should be the same
\set QUERY1 'SELECT show_chunks(hypertable=>\'drop_chunk_test1\', newer_than=>5)::NAME'
\set QUERY2 'SELECT drop_chunks(table_name=>\'drop_chunk_test1\', newer_than=>5, verbose => true)::NAME'
\set ECHO errors
\ir  :QUERY_RESULT_TEST_EQUAL_RELPATH
\set ECHO all

SELECT c.id AS chunk_id, c.hypertable_id, c.schema_name AS chunk_schema, c.table_name AS chunk_table, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.hypertable h ON (c.hypertable_id = h.id)
INNER JOIN  dimension_get_time(h.id) time_dimension ON(true)
INNER JOIN  _timescaledb_catalog.dimension_slice ds ON (ds.dimension_id = time_dimension.id)
INNER JOIN  _timescaledb_catalog.chunk_constraint cc ON (cc.dimension_slice_id = ds.id AND cc.chunk_id = c.id)
WHERE h.schema_name = 'public' AND (h.table_name = 'drop_chunk_test1')
ORDER BY c.id;

SELECT show_chunks('drop_chunk_test1');

\dt "_timescaledb_internal"._hyper*

-- show_chunks and drop_chunks output should be the same
\set QUERY1 'SELECT show_chunks(hypertable=>\'drop_chunk_test1\', older_than=>4, newer_than=>3)::NAME'
\set QUERY2 'SELECT drop_chunks(table_name=>\'drop_chunk_test1\', older_than=>4, newer_than=>3)::NAME'
\set ECHO errors
\ir  :QUERY_RESULT_TEST_EQUAL_RELPATH
\set ECHO all

SELECT c.id AS chunk_id, c.hypertable_id, c.schema_name AS chunk_schema, c.table_name AS chunk_table, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.hypertable h ON (c.hypertable_id = h.id)
INNER JOIN  dimension_get_time(h.id) time_dimension ON(true)
INNER JOIN  _timescaledb_catalog.dimension_slice ds ON (ds.dimension_id = time_dimension.id)
INNER JOIN  _timescaledb_catalog.chunk_constraint cc ON (cc.dimension_slice_id = ds.id AND cc.chunk_id = c.id)
WHERE h.schema_name = 'public' AND (h.table_name = 'drop_chunk_test1')
ORDER BY c.id;

-- the call of show_chunks should give same set of chunks as above
SELECT show_chunks('drop_chunk_test1');

-- testing drop_chunks when only schema is specified.
\set ON_ERROR_STOP 0
SELECT drop_chunks(schema_name=>'public');
SELECT drop_chunks(null::bigint, schema_name=>'public');
\set ON_ERROR_STOP 1

SELECT c.id AS chunk_id, c.hypertable_id, c.schema_name AS chunk_schema, c.table_name AS chunk_table, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.hypertable h ON (c.hypertable_id = h.id)
INNER JOIN  dimension_get_time(h.id) time_dimension ON(true)
INNER JOIN  _timescaledb_catalog.dimension_slice ds ON (ds.dimension_id = time_dimension.id)
INNER JOIN  _timescaledb_catalog.chunk_constraint cc ON (cc.dimension_slice_id = ds.id AND cc.chunk_id = c.id)
WHERE h.schema_name = 'public'
ORDER BY c.id;

-- show_chunks and drop_chunks output should be the same
\set QUERY1 'SELECT show_chunks(older_than=>5, newer_than=>4)::NAME'
\set QUERY2 'SELECT drop_chunks(5, schema_name=>\'public\', newer_than=>4)::NAME'
\set ECHO errors
\ir  :QUERY_RESULT_TEST_EQUAL_RELPATH
\set ECHO all

SELECT c.id AS chunk_id, c.hypertable_id, c.schema_name AS chunk_schema, c.table_name AS chunk_table, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.hypertable h ON (c.hypertable_id = h.id)
INNER JOIN  dimension_get_time(h.id) time_dimension ON(true)
INNER JOIN  _timescaledb_catalog.dimension_slice ds ON (ds.dimension_id = time_dimension.id)
INNER JOIN  _timescaledb_catalog.chunk_constraint cc ON (cc.dimension_slice_id = ds.id AND cc.chunk_id = c.id)
WHERE h.schema_name = 'public'
ORDER BY c.id;

CREATE TABLE PUBLIC.drop_chunk_test_ts(time timestamp, temp float8, device_id text);
SELECT create_hypertable('public.drop_chunk_test_ts', 'time', chunk_time_interval => interval '1 minute', create_default_indexes=>false);

CREATE TABLE PUBLIC.drop_chunk_test_tstz(time timestamptz, temp float8, device_id text);
SELECT create_hypertable('public.drop_chunk_test_tstz', 'time', chunk_time_interval => interval '1 minute', create_default_indexes=>false);

SET timezone = '+1';
INSERT INTO PUBLIC.drop_chunk_test_ts VALUES(now()-INTERVAL '5 minutes', 1.0, 'dev1');
INSERT INTO PUBLIC.drop_chunk_test_ts VALUES(now()+INTERVAL '5 minutes', 1.0, 'dev1');
INSERT INTO PUBLIC.drop_chunk_test_tstz VALUES(now()-INTERVAL '5 minutes', 1.0, 'dev1');
INSERT INTO PUBLIC.drop_chunk_test_tstz VALUES(now()+INTERVAL '5 minutes', 1.0, 'dev1');

SELECT * FROM test.show_subtables('drop_chunk_test_ts');
SELECT * FROM test.show_subtables('drop_chunk_test_tstz');

BEGIN;
    SELECT show_chunks('drop_chunk_test_ts');
    SELECT show_chunks('drop_chunk_test_ts', now()::timestamp-interval '1 minute');
-- show_chunks and drop_chunks output should be the same
    \set QUERY1 'SELECT show_chunks(newer_than => interval \'1 minute\', hypertable => \'drop_chunk_test_ts\')::NAME'
    \set QUERY2 'SELECT drop_chunks(newer_than => interval \'1 minute\', table_name => \'drop_chunk_test_ts\')::NAME'
    \set ECHO errors
    \ir  :QUERY_RESULT_TEST_EQUAL_RELPATH
    \set ECHO all
    \set QUERY1 'SELECT show_chunks(older_than => interval \'6 minute\', hypertable => \'drop_chunk_test_ts\')::NAME'
    \set QUERY2 'SELECT drop_chunks(older_than => interval \'6 minute\', table_name => \'drop_chunk_test_ts\')::NAME'
    \set ECHO errors
    \ir  :QUERY_RESULT_TEST_EQUAL_RELPATH
    \set ECHO all

    SELECT * FROM test.show_subtables('drop_chunk_test_ts');
    \set QUERY1 'SELECT show_chunks(older_than => interval \'1 minute\', hypertable => \'drop_chunk_test_ts\')::NAME'
    \set QUERY2 'SELECT drop_chunks(interval \'1 minute\', \'drop_chunk_test_ts\')::NAME'
    \set ECHO errors
    \ir  :QUERY_RESULT_TEST_EQUAL_RELPATH
    \set ECHO all
    SELECT * FROM test.show_subtables('drop_chunk_test_ts');
    SELECT show_chunks(hypertable => 'drop_chunk_test_tstz');
    SELECT show_chunks(hypertable => 'drop_chunk_test_tstz', older_than => now() - interval '1 minute', newer_than => now() - interval '6 minute');
    SELECT show_chunks(hypertable => 'drop_chunk_test_tstz', newer_than => now() - interval '1 minute');
    SELECT show_chunks(hypertable => 'drop_chunk_test_tstz', older_than => now() - interval '1 minute');

    \set QUERY1 'SELECT show_chunks(older_than => interval \'1 minute\', hypertable => \'drop_chunk_test_tstz\')::NAME'
    \set QUERY2 'SELECT drop_chunks(interval \'1 minute\', \'drop_chunk_test_tstz\')::NAME'
    \set ECHO errors
    \ir  :QUERY_RESULT_TEST_EQUAL_RELPATH
    \set ECHO all
    SELECT * FROM test.show_subtables('drop_chunk_test_tstz');
ROLLBACK;

BEGIN;
-- show_chunks and drop_chunks output should be the same
    \set QUERY1 'SELECT show_chunks(newer_than => interval \'6 minute\', hypertable => \'drop_chunk_test_ts\')::NAME'
    \set QUERY2 'SELECT drop_chunks(newer_than => interval \'6 minute\', table_name => \'drop_chunk_test_ts\')::NAME'
    \set ECHO errors
    \ir  :QUERY_RESULT_TEST_EQUAL_RELPATH
    \set ECHO all
    SELECT * FROM test.show_subtables('drop_chunk_test_ts');
ROLLBACK;

BEGIN;
-- show_chunks and drop_chunks output should be the same
    \set QUERY1 'SELECT show_chunks(older_than => interval \'1 minute\', hypertable => \'drop_chunk_test_ts\')::NAME'
    \set QUERY2 'SELECT drop_chunks(interval \'1 minute\', \'drop_chunk_test_ts\')::NAME'
    \set ECHO errors
    \ir  :QUERY_RESULT_TEST_EQUAL_RELPATH
    \set ECHO all
    SELECT * FROM test.show_subtables('drop_chunk_test_ts');
    \set QUERY1 'SELECT show_chunks(older_than => interval \'1 minute\', hypertable => \'drop_chunk_test_tstz\')::NAME'
    \set QUERY2 'SELECT drop_chunks(interval \'1 minute\', \'drop_chunk_test_tstz\')::NAME'
    \set ECHO errors
    \ir  :QUERY_RESULT_TEST_EQUAL_RELPATH
    \set ECHO all
    SELECT * FROM test.show_subtables('drop_chunk_test_tstz');
ROLLBACK;

BEGIN;
-- show_chunks and drop_chunks output should be the same
    \set QUERY1 'SELECT show_chunks(older_than => now()::timestamp-interval \'1 minute\', hypertable => \'drop_chunk_test_ts\')::NAME'
    \set QUERY2 'SELECT drop_chunks(now()::timestamp-interval \'1 minute\', \'drop_chunk_test_ts\')::NAME'
    \set ECHO errors
    \ir  :QUERY_RESULT_TEST_EQUAL_RELPATH
    \set ECHO all
    SELECT * FROM test.show_subtables('drop_chunk_test_ts');
    \set QUERY1 'SELECT show_chunks(older_than => now()-interval \'1 minute\', hypertable => \'drop_chunk_test_tstz\')::NAME'
    \set QUERY2 'SELECT drop_chunks(now()-interval \'1 minute\', \'drop_chunk_test_tstz\')::NAME'
    \set ECHO errors
    \ir  :QUERY_RESULT_TEST_EQUAL_RELPATH
    \set ECHO all
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
-- show_chunks and drop_chunks output should be the same
    \set QUERY1 'SELECT show_chunks(older_than => interval \'1 day\', hypertable => \'drop_chunk_test_date\')::NAME'
    \set QUERY2 'SELECT drop_chunks(interval \'1 day\', \'drop_chunk_test_date\')::NAME'
    \set ECHO errors
    \ir  :QUERY_RESULT_TEST_EQUAL_RELPATH
    \set ECHO all
    SELECT * FROM test.show_subtables('drop_chunk_test_date');
ROLLBACK;

BEGIN;
-- show_chunks and drop_chunks output should be the same
    \set QUERY1 'SELECT show_chunks(older_than => (now()-interval \'1 day\')::date, hypertable => \'drop_chunk_test_date\')::NAME'
    \set QUERY2 'SELECT drop_chunks((now()-interval \'1 day\')::date, \'drop_chunk_test_date\')::NAME'
    \set ECHO errors
    \ir  :QUERY_RESULT_TEST_EQUAL_RELPATH
    \set ECHO all
    SELECT * FROM test.show_subtables('drop_chunk_test_date');
ROLLBACK;

SET timezone TO '-5';

CREATE TABLE chunk_id_from_relid_test(time bigint, temp float8, device_id int);
SELECT hypertable_id FROM create_hypertable('chunk_id_from_relid_test', 'time', chunk_time_interval => 10) \gset

INSERT INTO chunk_id_from_relid_test VALUES (0, 1.1, 0), (0, 1.3, 11), (12, 2.0, 0), (12, 0.1, 11);

SELECT _timescaledb_internal.chunk_id_from_relid(tableoid) FROM chunk_id_from_relid_test;

DROP TABLE chunk_id_from_relid_test;

CREATE TABLE chunk_id_from_relid_test(time bigint, temp float8, device_id int);
SELECT hypertable_id FROM  create_hypertable('chunk_id_from_relid_test',
    'time', chunk_time_interval => 10,
    partitioning_column => 'device_id',
    number_partitions => 3) \gset

INSERT INTO chunk_id_from_relid_test VALUES (0, 1.1, 2), (0, 1.3, 11), (12, 2.0, 2), (12, 0.1, 11);

SELECT _timescaledb_internal.chunk_id_from_relid(tableoid) FROM chunk_id_from_relid_test;

\set ON_ERROR_STOP 0
SELECT _timescaledb_internal.chunk_id_from_relid('pg_type'::regclass);
SELECT _timescaledb_internal.chunk_id_from_relid('chunk_id_from_relid_test'::regclass);

-- test drop/show_chunks on custom partition types
CREATE FUNCTION extract_time(a jsonb)
RETURNS TIMESTAMPTZ
LANGUAGE SQL
AS $$ SELECT (a->>'time')::TIMESTAMPTZ $$ IMMUTABLE;

CREATE TABLE test_weird_type(a jsonb);
SELECT create_hypertable('test_weird_type', 'a',
    time_partitioning_func=>'extract_time'::regproc,
    chunk_time_interval=>'2 hours'::interval);

INSERT INTO test_weird_type VALUES ('{"time":"2019/06/06 1:00+0"}'), ('{"time":"2019/06/06 5:00+0"}');
SELECT * FROM test.show_subtables('test_weird_type');
SELECT show_chunks(hypertable => 'test_weird_type', older_than=>'2019/06/06 4:00+0'::TIMESTAMPTZ);
SELECT show_chunks(hypertable => 'test_weird_type', older_than=>'2019/06/06 10:00+0'::TIMESTAMPTZ);

-- show_chunks and drop_chunks output should be the same
\set QUERY1 'SELECT show_chunks(older_than => \'2019/06/06 5:00+0\'::TIMESTAMPTZ, hypertable => \'test_weird_type\')::NAME'
\set QUERY2 'SELECT drop_chunks(\'2019/06/06 5:00+0\'::TIMESTAMPTZ, \'test_weird_type\')::NAME'
\set ECHO errors
\ir  :QUERY_RESULT_TEST_EQUAL_RELPATH
\set ECHO all
SELECT * FROM test.show_subtables('test_weird_type');
SELECT show_chunks('test_weird_type', older_than=>'2019/06/06 4:00+0'::TIMESTAMPTZ);
SELECT show_chunks('test_weird_type', older_than=>'2019/06/06 10:00+0'::TIMESTAMPTZ);

-- show_chunks and drop_chunks output should be the same
\set QUERY1 'SELECT show_chunks(older_than => \'2019/06/06 6:00+0\'::TIMESTAMPTZ, hypertable => \'test_weird_type\')::NAME'
\set QUERY2 'SELECT drop_chunks(\'2019/06/06 6:00+0\'::TIMESTAMPTZ, \'test_weird_type\')::NAME'
\set ECHO errors
\ir  :QUERY_RESULT_TEST_EQUAL_RELPATH
\set ECHO all
SELECT * FROM test.show_subtables('test_weird_type');
SELECT show_chunks('test_weird_type', older_than=>'2019/06/06 10:00+0'::TIMESTAMPTZ);

DROP TABLE test_weird_type;

CREATE FUNCTION extract_int_time(a jsonb)
RETURNS BIGINT
LANGUAGE SQL
AS $$ SELECT (a->>'time')::BIGINT $$ IMMUTABLE;

CREATE TABLE test_weird_type_i(a jsonb);
SELECT create_hypertable('test_weird_type_i', 'a',
    time_partitioning_func=>'extract_int_time'::regproc,
    chunk_time_interval=>5);

INSERT INTO test_weird_type_i VALUES ('{"time":"0"}'), ('{"time":"5"}');
SELECT * FROM test.show_subtables('test_weird_type_i');
SELECT show_chunks('test_weird_type_i', older_than=>5);
SELECT show_chunks('test_weird_type_i', older_than=>10);

-- show_chunks and drop_chunks output should be the same
\set QUERY1 'SELECT show_chunks(older_than=>5, hypertable => \'test_weird_type_i\')::NAME'
\set QUERY2 'SELECT drop_chunks(5, \'test_weird_type_i\')::NAME'
\set ECHO errors
\ir  :QUERY_RESULT_TEST_EQUAL_RELPATH
\set ECHO all
SELECT * FROM test.show_subtables('test_weird_type_i');
SELECT show_chunks('test_weird_type_i', older_than=>5);
SELECT show_chunks('test_weird_type_i', older_than=>10);

-- show_chunks and drop_chunks output should be the same
\set QUERY1 'SELECT show_chunks(older_than=>10, hypertable => \'test_weird_type_i\')::NAME'
\set QUERY2 'SELECT drop_chunks(10, \'test_weird_type_i\')::NAME'
\set ECHO errors
\ir  :QUERY_RESULT_TEST_EQUAL_RELPATH
\set ECHO all
SELECT * FROM test.show_subtables('test_weird_type_i');
SELECT show_chunks('test_weird_type_i', older_than=>10);

DROP TABLE test_weird_type_i CASCADE;
\c  :TEST_DBNAME :ROLE_SUPERUSER
ALTER TABLE drop_chunk_test2 OWNER TO :ROLE_DEFAULT_PERM_USER_2;

--drop chunks 3 will have a chunk we a dependent object (a view)
--we create the dependent object now
INSERT INTO PUBLIC.drop_chunk_test3 VALUES(1, 1.0, 'dev1');

SELECT c.schema_name as chunk_schema, c.table_name as chunk_table
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.hypertable h ON (c.hypertable_id = h.id)
WHERE h.schema_name = 'public' AND h.table_name = 'drop_chunk_test3'
ORDER BY c.id \gset

create view dependent_view as SELECT * FROM :"chunk_schema".:"chunk_table";
ALTER TABLE drop_chunk_test3 OWNER TO :ROLE_DEFAULT_PERM_USER_2;

\c  :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2
\set ON_ERROR_STOP 0
SELECT drop_chunks(table_name=>'drop_chunk_test1', older_than=>4, newer_than=>3);
SELECT drop_chunks(2, CASCADE=>true, verbose => true);

--works with modified owner tables
-- show_chunks and drop_chunks output should be the same
\set QUERY1 'SELECT show_chunks(hypertable=>\'drop_chunk_test2\', older_than=>4, newer_than=>3)::NAME'
\set QUERY2 'SELECT drop_chunks(table_name=>\'drop_chunk_test2\', older_than=>4, newer_than=>3)::NAME'
\set ECHO errors
\ir  :QUERY_RESULT_TEST_EQUAL_RELPATH
\set ECHO all

--this fails because there is a dependent object
SELECT drop_chunks(table_name=>'drop_chunk_test3', older_than=>100);

--this will succeed even though there is a depenent object I don't have permission
--to drop. This matches PostgreSQL semantics.
-- show_chunks and drop_chunks output should be the same
\set QUERY1 'SELECT show_chunks(hypertable=>\'drop_chunk_test3\', older_than=>100)::NAME'
\set QUERY2 'SELECT drop_chunks(table_name=>\'drop_chunk_test3\', older_than=>100, cascade=>true)::NAME'
\set ECHO errors
\ir  :QUERY_RESULT_TEST_EQUAL_RELPATH
\set ECHO all

\set ON_ERROR_STOP 1

--drop chunks from hypertable with same name in different schema
-- order of schema in search_path matters --
\c :TEST_DBNAME :ROLE_SUPERUSER
drop table chunk_id_from_relid_test;
drop table drop_chunk_test1;
drop table drop_chunk_test2;
drop table drop_chunk_test3;
CREATE SCHEMA try_schema;
GRANT CREATE ON SCHEMA try_schema TO :ROLE_DEFAULT_PERM_USER;
GRANT USAGE ON SCHEMA try_schema TO :ROLE_DEFAULT_PERM_USER;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
CREATE TABLE try_schema.drop_chunk_test_date(time date, temp float8, device_id text);
SELECT create_hypertable('try_schema.drop_chunk_test_date', 'time', chunk_time_interval => interval '1 day', create_default_indexes=>false);
INSERT INTO public.drop_chunk_test_date VALUES( '2020-01-10', 100, 'hello');
INSERT INTO try_schema.drop_chunk_test_date VALUES( '2020-01-10', 100, 'hello');
set search_path to try_schema, public;
SELECT show_chunks(hypertable=>'public.drop_chunk_test_date', older_than=>'1 day'::interval);
SELECT show_chunks(hypertable=>'try_schema.drop_chunk_test_date', older_than=>'1 day'::interval);
SELECT drop_chunks(table_name=>'drop_chunk_test_date', older_than=> '1 day'::interval);

--drop_chunks without schema_name and table_name
INSERT INTO public.drop_chunk_test_date VALUES( '2020-02-11', 100, 'hello');
INSERT INTO try_schema.drop_chunk_test_date VALUES( '2020-02-10', 100, 'hello');
SELECT show_chunks(hypertable=>'public.drop_chunk_test_date', older_than=>'1 day'::interval);
SELECT show_chunks(hypertable=>'try_schema.drop_chunk_test_date', older_than=>'1 day'::interval);
SELECT drop_chunks( older_than=> '1 day'::interval);
