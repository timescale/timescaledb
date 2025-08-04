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

CREATE TABLE PUBLIC.drop_chunk_test1(time bigint, temp float8, device_id text);
CREATE TABLE PUBLIC.drop_chunk_test2(time bigint, temp float8, device_id text);
CREATE TABLE PUBLIC.drop_chunk_test3(time bigint, temp float8, device_id text);
CREATE INDEX ON drop_chunk_test1(time DESC);

-- show_chunks() without specifying a table is not allowed
\set ON_ERROR_STOP 0
SELECT show_chunks(NULL);
\set ON_ERROR_STOP 1

SELECT create_hypertable('public.drop_chunk_test1', 'time', chunk_time_interval => 1, create_default_indexes=>false);
SELECT create_hypertable('public.drop_chunk_test2', 'time', chunk_time_interval => 1, create_default_indexes=>false);
SELECT create_hypertable('public.drop_chunk_test3', 'time', chunk_time_interval => 1, create_default_indexes=>false);

-- Add space dimensions to ensure chunks share dimension slices
SELECT add_dimension('public.drop_chunk_test1', 'device_id', 2);
SELECT add_dimension('public.drop_chunk_test2', 'device_id', 2);
SELECT add_dimension('public.drop_chunk_test3', 'device_id', 2);

SELECT c.id AS chunk_id, c.hypertable_id, c.schema_name AS chunk_schema, c.table_name AS chunk_table, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.hypertable h ON (c.hypertable_id = h.id)
INNER JOIN  dimension_get_time(h.id) time_dimension ON(true)
INNER JOIN  _timescaledb_catalog.dimension_slice ds ON (ds.dimension_id = time_dimension.id)
INNER JOIN  _timescaledb_catalog.chunk_constraint cc ON (cc.dimension_slice_id = ds.id AND cc.chunk_id = c.id)
WHERE h.schema_name = 'public' AND (h.table_name = 'drop_chunk_test1' OR h.table_name = 'drop_chunk_test2')
ORDER BY c.id;

SELECT * FROM test.relation WHERE schema = '_timescaledb_internal' AND name LIKE '\_hyper%';

SELECT  _timescaledb_functions.get_partition_for_key('dev1'::text);
SELECT  _timescaledb_functions.get_partition_for_key('dev7'::varchar(5));

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
SELECT * FROM test.relation WHERE schema = '_timescaledb_internal' AND name LIKE '\_hyper%';

-- next two calls of show_chunks should give same set of chunks as above when combined
SELECT show_chunks('drop_chunk_test1');
SELECT * FROM show_chunks('drop_chunk_test2');

CREATE VIEW dependent_view AS SELECT * FROM _timescaledb_internal._hyper_1_1_chunk;

\set ON_ERROR_STOP 0
SELECT drop_chunks('drop_chunk_test1');
SELECT drop_chunks('drop_chunk_test1', older_than => 2);
SELECT drop_chunks('drop_chunk_test1', older_than => NULL::interval);
SELECT drop_chunks('drop_chunk_test1', older_than => NULL::int);

DROP VIEW dependent_view;

-- should error because of wrong relative order of time constraints
SELECT show_chunks('drop_chunk_test1', older_than=>3, newer_than=>4);

-- Should error because NULL was used for the table name.
SELECT drop_chunks(NULL, older_than => 3);
-- should error because there is no relation with that OID.
SELECT drop_chunks(3533, older_than => 3);

\set ON_ERROR_STOP 1

-- show created constraints and dimension slices for each chunk
SELECT c.table_name, cc.constraint_name, ds.id AS dimension_slice_id, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.chunk_constraint cc ON (c.id = cc.chunk_id)
FULL OUTER JOIN _timescaledb_catalog.dimension_slice ds ON (ds.id = cc.dimension_slice_id)
ORDER BY c.id;
SELECT * FROM _timescaledb_catalog.dimension_slice ORDER BY id;

-- Test that truncating chunks works
SELECT count(*) FROM _timescaledb_internal._hyper_2_7_chunk;
TRUNCATE TABLE _timescaledb_internal._hyper_2_7_chunk;
SELECT count(*) FROM _timescaledb_internal._hyper_2_7_chunk;

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

-- We drop all chunks older than timestamp 2 in all hypertable. This
-- is added only to avoid making the diff for this commit larger than
-- necessary and make reviews easier.
SELECT drop_chunks(format('%1$I.%2$I', schema_name, table_name)::regclass, older_than => 2)
  FROM _timescaledb_catalog.hypertable;

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

SELECT * FROM test.relation WHERE schema = '_timescaledb_internal' AND name LIKE '\_hyper%';
-- show_chunks and drop_chunks output should be the same
\set QUERY1 'SELECT show_chunks(\'drop_chunk_test1\', older_than => 3)::NAME'
\set QUERY2 'SELECT drop_chunks(\'drop_chunk_test1\', older_than => 3)::NAME'
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

SELECT * FROM test.relation WHERE schema = '_timescaledb_internal' AND name LIKE '\_hyper%';

-- next two calls of show_chunks should give same set of chunks as above when combined
SELECT show_chunks('drop_chunk_test1');
SELECT * FROM show_chunks('drop_chunk_test2');

-- 2,147,483,647 is the largest int so this tests that BIGINTs work
-- show_chunks and drop_chunks output should be the same
\set QUERY1 'SELECT show_chunks(\'drop_chunk_test3\', older_than => 2147483648)::NAME'
\set QUERY2 'SELECT drop_chunks(\'drop_chunk_test3\', older_than => 2147483648)::NAME'
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

SELECT * FROM test.relation WHERE schema = '_timescaledb_internal' AND name LIKE '\_hyper%';
\set ON_ERROR_STOP 0
-- should error because no hypertable
SELECT drop_chunks('drop_chunk_test4', older_than => 5);
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

SELECT * FROM test.relation WHERE schema = '_timescaledb_internal' AND name LIKE '\_hyper%';

-- newer_than tests
-- show_chunks and drop_chunks output should be the same
\set QUERY1 'SELECT show_chunks(\'drop_chunk_test1\', newer_than=>5)::NAME'
\set QUERY2 'SELECT drop_chunks(\'drop_chunk_test1\', newer_than=>5, verbose => true)::NAME'
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

SELECT * FROM test.relation WHERE schema = '_timescaledb_internal' AND name LIKE '_hyper%';

-- show_chunks and drop_chunks output should be the same
\set QUERY1 'SELECT show_chunks(\'drop_chunk_test1\', older_than=>4, newer_than=>3)::NAME'
\set QUERY2 'SELECT drop_chunks(\'drop_chunk_test1\', older_than=>4, newer_than=>3)::NAME'
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

SELECT c.id AS chunk_id, c.hypertable_id, c.schema_name AS chunk_schema, c.table_name AS chunk_table, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.hypertable h ON (c.hypertable_id = h.id)
INNER JOIN  dimension_get_time(h.id) time_dimension ON(true)
INNER JOIN  _timescaledb_catalog.dimension_slice ds ON (ds.dimension_id = time_dimension.id)
INNER JOIN  _timescaledb_catalog.chunk_constraint cc ON (cc.dimension_slice_id = ds.id AND cc.chunk_id = c.id)
WHERE h.schema_name = 'public'
ORDER BY c.id;

-- We support show/drop chunks using timestamps/interval even with integer partitioning
-- the chunk creation time gets used for these. But we need to use "created_before, created_after"
-- for these
\set ON_ERROR_STOP 0
SELECT show_chunks('drop_chunk_test3', older_than => now());
SELECT show_chunks('drop_chunk_test2', older_than => now());
SELECT show_chunks('drop_chunk_test1', newer_than => INTERVAL '15 minutes');
SELECT show_chunks('drop_chunk_test1', older_than => now(), newer_than => INTERVAL '15 minutes');
SELECT drop_chunks('drop_chunk_test1', older_than => now());
-- mix of older_than/newer_than and created_after/created_before doesn't work
SELECT show_chunks('drop_chunk_test1', older_than => now(), created_after => INTERVAL '15 minutes');
SELECT show_chunks('drop_chunk_test1', created_before => now(), newer_than => INTERVAL '15 minutes');
\set ON_ERROR_STOP 1
SELECT show_chunks('drop_chunk_test3', created_before => now() + INTERVAL '1 hour');
SELECT show_chunks('drop_chunk_test2', created_before => now() + INTERVAL '1 hour');
SELECT show_chunks('drop_chunk_test1', created_after => INTERVAL '15 minutes');
SELECT show_chunks('drop_chunk_test1', created_before => now() + INTERVAL '1 hour', created_after => INTERVAL '1 hour');
SELECT drop_chunks('drop_chunk_test1', created_before => now() + INTERVAL '1 hour');

SELECT drop_chunks(format('%1$I.%2$I', schema_name, table_name)::regclass, older_than => 5, newer_than => 4)
  FROM _timescaledb_catalog.hypertable WHERE schema_name = 'public';



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
    \set QUERY1 'SELECT show_chunks(\'drop_chunk_test_ts\', newer_than => interval \'1 minute\')::NAME'
    \set QUERY2 'SELECT drop_chunks(\'drop_chunk_test_ts\', newer_than => interval \'1 minute\')::NAME'
    \set ECHO errors
    \ir  :QUERY_RESULT_TEST_EQUAL_RELPATH
    \set ECHO all
    \set QUERY1 'SELECT show_chunks(\'drop_chunk_test_ts\', older_than => interval \'6 minute\')::NAME'
    \set QUERY2 'SELECT drop_chunks(\'drop_chunk_test_ts\', older_than => interval \'6 minute\')::NAME'
    \set ECHO errors
    \ir  :QUERY_RESULT_TEST_EQUAL_RELPATH
    \set ECHO all

    SELECT * FROM test.show_subtables('drop_chunk_test_ts');
    \set QUERY1 'SELECT show_chunks(\'drop_chunk_test_ts\', older_than => interval \'1 minute\')::NAME'
    \set QUERY2 'SELECT drop_chunks(\'drop_chunk_test_ts\', interval \'1 minute\')::NAME'
    \set ECHO errors
    \ir  :QUERY_RESULT_TEST_EQUAL_RELPATH
    \set ECHO all
    SELECT * FROM test.show_subtables('drop_chunk_test_ts');
    SELECT show_chunks('drop_chunk_test_tstz');
    SELECT show_chunks('drop_chunk_test_tstz', older_than => now() - interval '1 minute', newer_than => now() - interval '6 minute');
    SELECT show_chunks('drop_chunk_test_tstz', newer_than => now() - interval '1 minute');
    SELECT show_chunks('drop_chunk_test_tstz', older_than => now() - interval '1 minute');

    \set QUERY1 'SELECT show_chunks(older_than => interval \'1 minute\', relation => \'drop_chunk_test_tstz\')::NAME'
    \set QUERY2 'SELECT drop_chunks(\'drop_chunk_test_tstz\', interval \'1 minute\')::NAME'
    \set ECHO errors
    \ir  :QUERY_RESULT_TEST_EQUAL_RELPATH
    \set ECHO all
    SELECT * FROM test.show_subtables('drop_chunk_test_tstz');
ROLLBACK;

BEGIN;
-- show_chunks and drop_chunks output should be the same
    \set QUERY1 'SELECT show_chunks(\'drop_chunk_test_ts\', newer_than => interval \'6 minute\')::NAME'
    \set QUERY2 'SELECT drop_chunks(\'drop_chunk_test_ts\', newer_than => interval \'6 minute\')::NAME'
    \set ECHO errors
    \ir  :QUERY_RESULT_TEST_EQUAL_RELPATH
    \set ECHO all
    SELECT * FROM test.show_subtables('drop_chunk_test_ts');
ROLLBACK;

BEGIN;
-- show_chunks and drop_chunks output should be the same
    \set QUERY1 'SELECT show_chunks(\'drop_chunk_test_ts\', older_than => interval \'1 minute\')::NAME'
    \set QUERY2 'SELECT drop_chunks(\'drop_chunk_test_ts\', older_than => interval \'1 minute\')::NAME'
    \set ECHO errors
    \ir  :QUERY_RESULT_TEST_EQUAL_RELPATH
    \set ECHO all
    SELECT * FROM test.show_subtables('drop_chunk_test_ts');
    \set QUERY1 'SELECT show_chunks(\'drop_chunk_test_tstz\', older_than => interval \'1 minute\')::NAME'
    \set QUERY2 'SELECT drop_chunks(\'drop_chunk_test_tstz\', older_than => interval \'1 minute\')::NAME'
    \set ECHO errors
    \ir  :QUERY_RESULT_TEST_EQUAL_RELPATH
    \set ECHO all
    SELECT * FROM test.show_subtables('drop_chunk_test_tstz');
ROLLBACK;

BEGIN;
-- show_chunks and drop_chunks output should be the same
    \set QUERY1 'SELECT show_chunks(\'drop_chunk_test_ts\', older_than => now()::timestamp-interval \'1 minute\')::NAME'
    \set QUERY2 'SELECT drop_chunks(\'drop_chunk_test_ts\', older_than => now()::timestamp-interval \'1 minute\')::NAME'
    \set ECHO errors
    \ir  :QUERY_RESULT_TEST_EQUAL_RELPATH
    \set ECHO all
    SELECT * FROM test.show_subtables('drop_chunk_test_ts');
    \set QUERY1 'SELECT show_chunks(\'drop_chunk_test_tstz\', older_than => now()-interval \'1 minute\')::NAME'
    \set QUERY2 'SELECT drop_chunks(\'drop_chunk_test_tstz\', older_than => now()-interval \'1 minute\')::NAME'
    \set ECHO errors
    \ir  :QUERY_RESULT_TEST_EQUAL_RELPATH
    \set ECHO all
    SELECT * FROM test.show_subtables('drop_chunk_test_tstz');
ROLLBACK;

SELECT * FROM test.relation WHERE schema = '_timescaledb_internal' AND name LIKE '\_hyper%';

\set ON_ERROR_STOP 0
SELECT drop_chunks(interval '1 minute');
SELECT drop_chunks('drop_chunk_test_ts', (now()-interval '1 minute'));
SELECT drop_chunks('drop_chunk_test3', verbose => true);
SELECT drop_chunks('drop_chunk_test3', interval '1 minute');
\set ON_ERROR_STOP 1

-- Interval boundary for INTEGER type columns. It uses chunk creation
-- time to identify the affected chunks.
SELECT drop_chunks('drop_chunk_test3', created_after => interval '1 minute');

SELECT * FROM test.relation WHERE schema = '_timescaledb_internal' AND name LIKE '\_hyper%';

CREATE TABLE PUBLIC.drop_chunk_test_date(time date, temp float8, device_id text);
SELECT create_hypertable('public.drop_chunk_test_date', 'time', chunk_time_interval => interval '1 day', create_default_indexes=>false);

SET timezone = '+100';
INSERT INTO PUBLIC.drop_chunk_test_date VALUES(now()-INTERVAL '2 day', 1.0, 'dev1');

BEGIN;
-- show_chunks and drop_chunks output should be the same
    \set QUERY1 'SELECT show_chunks(\'drop_chunk_test_date\', older_than => interval \'1 day\')::NAME'
    \set QUERY2 'SELECT drop_chunks(\'drop_chunk_test_date\', older_than => interval \'1 day\')::NAME'
    \set ECHO errors
    \ir  :QUERY_RESULT_TEST_EQUAL_RELPATH
    \set ECHO all
    SELECT * FROM test.show_subtables('drop_chunk_test_date');
ROLLBACK;

BEGIN;
-- show_chunks and drop_chunks output should be the same
    \set QUERY1 'SELECT show_chunks(\'drop_chunk_test_date\', older_than => (now()-interval \'1 day\')::date)::NAME'
    \set QUERY2 'SELECT drop_chunks(\'drop_chunk_test_date\', older_than => (now()-interval \'1 day\')::date)::NAME'
    \set ECHO errors
    \ir  :QUERY_RESULT_TEST_EQUAL_RELPATH
    \set ECHO all
    SELECT * FROM test.show_subtables('drop_chunk_test_date');
ROLLBACK;

SET timezone TO '-5';

CREATE TABLE chunk_id_from_relid_test(time bigint, temp float8, device_id int);
SELECT hypertable_id FROM create_hypertable('chunk_id_from_relid_test', 'time', chunk_time_interval => 10) \gset

INSERT INTO chunk_id_from_relid_test VALUES (0, 1.1, 0), (0, 1.3, 11), (12, 2.0, 0), (12, 0.1, 11);

SELECT _timescaledb_functions.chunk_id_from_relid(tableoid) FROM chunk_id_from_relid_test;

DROP TABLE chunk_id_from_relid_test;

CREATE TABLE chunk_id_from_relid_test(time bigint, temp float8, device_id int);
SELECT hypertable_id FROM  create_hypertable('chunk_id_from_relid_test',
    'time', chunk_time_interval => 10,
    partitioning_column => 'device_id',
    number_partitions => 3) \gset

INSERT INTO chunk_id_from_relid_test VALUES (0, 1.1, 2), (0, 1.3, 11), (12, 2.0, 2), (12, 0.1, 11);

SELECT _timescaledb_functions.chunk_id_from_relid(tableoid) FROM chunk_id_from_relid_test;

\set ON_ERROR_STOP 0
SELECT _timescaledb_functions.chunk_id_from_relid('pg_type'::regclass);
SELECT _timescaledb_functions.chunk_id_from_relid('chunk_id_from_relid_test'::regclass);

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
SELECT show_chunks('test_weird_type', older_than=>'2019/06/06 4:00+0'::TIMESTAMPTZ);
SELECT show_chunks('test_weird_type', older_than=>'2019/06/06 10:00+0'::TIMESTAMPTZ);

-- show_chunks and drop_chunks output should be the same
\set QUERY1 'SELECT show_chunks(\'test_weird_type\', older_than => \'2019/06/06 5:00+0\'::TIMESTAMPTZ)::NAME'
\set QUERY2 'SELECT drop_chunks(\'test_weird_type\', older_than => \'2019/06/06 5:00+0\'::TIMESTAMPTZ)::NAME'
\set ECHO errors
\ir  :QUERY_RESULT_TEST_EQUAL_RELPATH
\set ECHO all
SELECT * FROM test.show_subtables('test_weird_type');
SELECT show_chunks('test_weird_type', older_than=>'2019/06/06 4:00+0'::TIMESTAMPTZ);
SELECT show_chunks('test_weird_type', older_than=>'2019/06/06 10:00+0'::TIMESTAMPTZ);

-- show_chunks and drop_chunks output should be the same
\set QUERY1 'SELECT show_chunks(\'test_weird_type\', older_than => \'2019/06/06 6:00+0\'::TIMESTAMPTZ)::NAME'
\set QUERY2 'SELECT drop_chunks(\'test_weird_type\', older_than => \'2019/06/06 6:00+0\'::TIMESTAMPTZ)::NAME'
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
\set QUERY1 'SELECT show_chunks(\'test_weird_type_i\', older_than=>5)::NAME'
\set QUERY2 'SELECT drop_chunks(\'test_weird_type_i\', older_than => 5)::NAME'
\set ECHO errors
\ir  :QUERY_RESULT_TEST_EQUAL_RELPATH
\set ECHO all
SELECT * FROM test.show_subtables('test_weird_type_i');
SELECT show_chunks('test_weird_type_i', older_than=>5);
SELECT show_chunks('test_weird_type_i', older_than=>10);

-- show_chunks and drop_chunks output should be the same
\set QUERY1 'SELECT show_chunks(\'test_weird_type_i\', older_than=>10)::NAME'
\set QUERY2 'SELECT drop_chunks(\'test_weird_type_i\', older_than => 10)::NAME'
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
create view dependent_view2 as SELECT * FROM :"chunk_schema".:"chunk_table";
ALTER TABLE drop_chunk_test3 OWNER TO :ROLE_DEFAULT_PERM_USER_2;

\c  :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2
\set ON_ERROR_STOP 0
SELECT drop_chunks('drop_chunk_test1', older_than=>4, newer_than=>3);

--works with modified owner tables
-- show_chunks and drop_chunks output should be the same
\set QUERY1 'SELECT show_chunks(\'drop_chunk_test2\', older_than=>4, newer_than=>3)::NAME'
\set QUERY2 'SELECT drop_chunks(\'drop_chunk_test2\', older_than=>4, newer_than=>3)::NAME'
\set ECHO errors
\ir  :QUERY_RESULT_TEST_EQUAL_RELPATH
\set ECHO all

\set VERBOSITY default
--this fails because there are dependent objects
SELECT drop_chunks('drop_chunk_test3', older_than=>100);
\set VERBOSITY terse

\c  :TEST_DBNAME :ROLE_SUPERUSER
DROP VIEW dependent_view;
DROP VIEW dependent_view2;

\c  :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2
\set ON_ERROR_STOP 1

--drop chunks from hypertable with same name in different schema
-- order of schema in search_path matters --
\c :TEST_DBNAME :ROLE_SUPERUSER
drop table chunk_id_from_relid_test;
drop table drop_chunk_test1;
drop table drop_chunk_test2;
drop table drop_chunk_test3;
CREATE SCHEMA try_schema;
CREATE SCHEMA test1;
CREATE SCHEMA test2;
CREATE SCHEMA test3;
GRANT CREATE ON SCHEMA try_schema, test1, test2, test3 TO :ROLE_DEFAULT_PERM_USER;
GRANT USAGE ON SCHEMA try_schema, test1, test2, test3 TO :ROLE_DEFAULT_PERM_USER;

SET ROLE :ROLE_DEFAULT_PERM_USER;
CREATE TABLE try_schema.drop_chunk_test_date(time date, temp float8, device_id text);
SELECT create_hypertable('try_schema.drop_chunk_test_date', 'time', chunk_time_interval => interval '1 day', create_default_indexes=>false);
INSERT INTO public.drop_chunk_test_date VALUES( '2020-01-10', 100, 'hello');
INSERT INTO try_schema.drop_chunk_test_date VALUES( '2020-01-10', 100, 'hello');
set search_path to try_schema, test1, test2, test3, public;
SELECT show_chunks('public.drop_chunk_test_date', older_than=>'1 day'::interval);
SELECT show_chunks('try_schema.drop_chunk_test_date', older_than=>'1 day'::interval);
SELECT drop_chunks('drop_chunk_test_date', older_than=> '1 day'::interval);

-- test drop chunks across two tables within the same schema
CREATE TABLE test1.hyper1 (time bigint, temp float);
CREATE TABLE test1.hyper2 (time bigint, temp float);

SELECT create_hypertable('test1.hyper1', 'time', chunk_time_interval => 10);
SELECT create_hypertable('test1.hyper2', 'time', chunk_time_interval => 10);

INSERT INTO test1.hyper1 VALUES (10, 0.5);
INSERT INTO test1.hyper2 VALUES (10, 0.7);

SELECT show_chunks('test1.hyper1');
SELECT show_chunks('test1.hyper2');

-- test drop chunks for given table name across all schemas
CREATE TABLE test2.hyperx (time bigint, temp float);
CREATE TABLE test3.hyperx (time bigint, temp float);

SELECT create_hypertable('test2.hyperx', 'time', chunk_time_interval => 10);
SELECT create_hypertable('test3.hyperx', 'time', chunk_time_interval => 10);

INSERT INTO test2.hyperx VALUES (10, 0.5);
INSERT INTO test3.hyperx VALUES (10, 0.7);

SELECT show_chunks('test2.hyperx');
SELECT show_chunks('test3.hyperx');

-- This will only drop from one of the tables since the one that is
-- first in the search path will hide the other one.
SELECT drop_chunks('hyperx', older_than => 100);
SELECT show_chunks('test2.hyperx');
SELECT show_chunks('test3.hyperx');

-- Check CTAS behavior when internal ALTER TABLE gets fired
CREATE TABLE PUBLIC.drop_chunk_test4(time bigint, temp float8, device_id text);
CREATE TABLE drop_chunks_table_id AS SELECT hypertable_id
      FROM create_hypertable('public.drop_chunk_test4', 'time', chunk_time_interval => 1);

-- TEST for internal api that drops a single chunk
-- this drops the table and removes entry from the catalog.
-- does not affect any materialized cagg data
INSERT INTO test1.hyper1 VALUES (20, 0.5);
SELECT chunk_schema as "CHSCHEMA",  chunk_name as "CHNAME"
FROM timescaledb_information.chunks
WHERE hypertable_name = 'hyper1' and hypertable_schema = 'test1'
ORDER BY chunk_name ;
--drop one of the chunks
SELECT chunk_schema || '.' || chunk_name as "CHNAME"
FROM timescaledb_information.chunks
WHERE hypertable_name = 'hyper1' and hypertable_schema = 'test1'
ORDER BY chunk_name LIMIT 1
\gset

SELECT  _timescaledb_functions.drop_chunk(:'CHNAME');
SELECT chunk_schema as "CHSCHEMA",  chunk_name as "CHNAME"
FROM timescaledb_information.chunks
WHERE hypertable_name = 'hyper1' and hypertable_schema = 'test1'
ORDER BY chunk_name ;

-- "created_before/after" can be used with time partitioning in drop/show chunks
SELECT show_chunks('drop_chunk_test_tstz', created_before => now() - INTERVAL '1 hour');
SELECT drop_chunks('drop_chunk_test_tstz', created_before => now() + INTERVAL '1 hour');
SELECT show_chunks('drop_chunk_test_ts');
-- "created_before/after" accept timestamptz even though partitioning col is just
-- timestamp
SELECT show_chunks('drop_chunk_test_ts', created_after => now() - INTERVAL '1 hour', created_before => now());
SELECT drop_chunks('drop_chunk_test_ts', created_after => INTERVAL '1 hour', created_before => now());

-- Test views on top of hypertables
CREATE TABLE view_test (project_id INT, ts TIMESTAMPTZ NOT NULL);
SELECT create_hypertable('view_test', by_range('ts', INTERVAL '1 day'));
-- exactly one partition per project_id
SELECT * FROM add_dimension('view_test', 'project_id', chunk_time_interval => 1); -- exactly one partition per project; works for *integer* types
INSERT INTO view_test (project_id, ts)
SELECT g % 25 + 1 AS project_id, i.ts + (g * interval '1 week') / i.total AS ts
FROM (SELECT timestamptz '2024-01-01 00:00:00+0', 600) i(ts, total),
generate_series(1, i.total) g;
-- Create a view on top of this hypertable
CREATE VIEW test_view_part_few AS SELECT project_id,
    ts
   FROM view_test
  WHERE project_id = ANY (ARRAY[5, 10, 15]);
-- Complicated query on a view involving a range check and a sort
SELECT * FROM test_view_part_few WHERE ts BETWEEN '2024-01-04 00:00:00+00'AND '2024-01-05 00:00:00' ORDER BY ts LIMIT 1000;
