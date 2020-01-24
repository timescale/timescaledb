-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Set this variable to avoid using a hard-coded path each time query
-- results are compared
\set QUERY_RESULT_TEST_EQUAL_RELPATH '../../../test/sql/include/query_result_test_equal.sql'

\set ON_ERROR_STOP 0

--DDL commands on continuous aggregates

CREATE TABLE conditions (
      timec        TIMESTAMPTZ       NOT NULL,
      location    TEXT              NOT NULL,
      temperature integer  NULL,
      humidity    DOUBLE PRECISION  NULL,
      timemeasure TIMESTAMPTZ,
      timeinterval INTERVAL
);

select table_name from create_hypertable('conditions', 'timec');

-- schema tests

\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE SCHEMA rename_schema;
GRANT ALL ON SCHEMA rename_schema TO :ROLE_DEFAULT_PERM_USER;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

CREATE TABLE foo(time TIMESTAMPTZ, data INTEGER);
SELECT create_hypertable('foo', 'time');

CREATE VIEW rename_test
  WITH ( timescaledb.continuous, timescaledb.materialized_only=true)
AS SELECT time_bucket('1week', time), COUNT(data)
    FROM foo
    GROUP BY 1;

SELECT user_view_schema, user_view_name, partial_view_schema, partial_view_name
      FROM _timescaledb_catalog.continuous_agg;

ALTER VIEW rename_test SET SCHEMA rename_schema;

SELECT user_view_schema, user_view_name, partial_view_schema, partial_view_name
      FROM _timescaledb_catalog.continuous_agg;

SELECT ca.raw_hypertable_id as "RAW_HYPERTABLE_ID",
       h.schema_name AS "MAT_SCHEMA_NAME",
       h.table_name AS "MAT_TABLE_NAME",
       partial_view_name as "PART_VIEW_NAME",
       partial_view_schema as "PART_VIEW_SCHEMA",
       direct_view_name as "DIR_VIEW_NAME",
       direct_view_schema as "DIR_VIEW_SCHEMA"
FROM _timescaledb_catalog.continuous_agg ca
INNER JOIN _timescaledb_catalog.hypertable h ON(h.id = ca.mat_hypertable_id)
WHERE user_view_name = 'rename_test'
\gset

\c :TEST_DBNAME :ROLE_SUPERUSER
ALTER VIEW :"PART_VIEW_SCHEMA".:"PART_VIEW_NAME" SET SCHEMA public;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

SELECT user_view_schema, user_view_name, partial_view_schema, partial_view_name
      FROM _timescaledb_catalog.continuous_agg;

--alter direct view schema
SELECT user_view_schema, user_view_name, direct_view_schema, direct_view_name
      FROM _timescaledb_catalog.continuous_agg;
\c :TEST_DBNAME :ROLE_SUPERUSER
ALTER VIEW :"DIR_VIEW_SCHEMA".:"DIR_VIEW_NAME" SET SCHEMA public;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

SELECT user_view_schema, user_view_name, partial_view_schema, partial_view_name,
      direct_view_schema, direct_view_name
      FROM _timescaledb_catalog.continuous_agg;

\c :TEST_DBNAME :ROLE_SUPERUSER

ALTER SCHEMA rename_schema RENAME TO new_name_schema;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

SELECT user_view_schema, user_view_name, partial_view_schema, partial_view_name
      FROM _timescaledb_catalog.continuous_agg;

ALTER VIEW :"PART_VIEW_NAME" SET SCHEMA new_name_schema;

SELECT user_view_schema, user_view_name, partial_view_schema, partial_view_name
      FROM _timescaledb_catalog.continuous_agg;

\c :TEST_DBNAME :ROLE_SUPERUSER

ALTER SCHEMA new_name_schema RENAME TO foo_name_schema;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

SELECT user_view_schema, user_view_name, partial_view_schema, partial_view_name
      FROM _timescaledb_catalog.continuous_agg;

ALTER VIEW foo_name_schema.rename_test SET SCHEMA public;

SELECT user_view_schema, user_view_name, partial_view_schema, partial_view_name
      FROM _timescaledb_catalog.continuous_agg;

\c :TEST_DBNAME :ROLE_SUPERUSER

ALTER SCHEMA foo_name_schema RENAME TO rename_schema;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
SET client_min_messages TO LOG;

SELECT user_view_schema, user_view_name, partial_view_schema, partial_view_name
      FROM _timescaledb_catalog.continuous_agg;

ALTER VIEW rename_test RENAME TO rename_c_aggregate;

SELECT user_view_schema, user_view_name, partial_view_schema, partial_view_name
      FROM _timescaledb_catalog.continuous_agg;

SELECT * FROM rename_c_aggregate;

ALTER VIEW rename_schema.:"PART_VIEW_NAME" RENAME TO partial_view;

SELECT user_view_schema, user_view_name, partial_view_schema, partial_view_name,
      direct_view_schema, direct_view_name
      FROM _timescaledb_catalog.continuous_agg;

--rename direct view
ALTER VIEW :"DIR_VIEW_NAME" RENAME TO direct_view;
SELECT user_view_schema, user_view_name, partial_view_schema, partial_view_name,
      direct_view_schema, direct_view_name
      FROM _timescaledb_catalog.continuous_agg;

-- drop_chunks tests
DROP TABLE conditions CASCADE;
DROP TABLE foo CASCADE;

CREATE TABLE drop_chunks_table(time BIGINT, data INTEGER);
SELECT hypertable_id AS drop_chunks_table_id
    FROM create_hypertable('drop_chunks_table', 'time', chunk_time_interval => 10) \gset

CREATE OR REPLACE FUNCTION integer_now_test() returns bigint LANGUAGE SQL STABLE as $$ SELECT coalesce(max(time), bigint '0') FROM drop_chunks_table $$;
SELECT set_integer_now_func('drop_chunks_table', 'integer_now_test');

CREATE VIEW drop_chunks_view
  WITH (
    timescaledb.continuous,
    timescaledb.materialized_only=true,
    timescaledb.refresh_interval='72 hours'
  )
AS SELECT time_bucket('5', time), COUNT(data)
    FROM drop_chunks_table
    GROUP BY 1;

SELECT format('%s.%s', schema_name, table_name) AS drop_chunks_mat_table,
        schema_name AS drop_chunks_mat_schema,
        table_name AS drop_chunks_mat_table_name
    FROM _timescaledb_catalog.hypertable, _timescaledb_catalog.continuous_agg
    WHERE _timescaledb_catalog.continuous_agg.raw_hypertable_id = :drop_chunks_table_id
        AND _timescaledb_catalog.hypertable.id = _timescaledb_catalog.continuous_agg.mat_hypertable_id \gset

-- create 3 chunks, with 3 time bucket
INSERT INTO drop_chunks_table SELECT i, i FROM generate_series(0, 29) AS i;
REFRESH MATERIALIZED VIEW drop_chunks_view;

SELECT count(c) FROM show_chunks('drop_chunks_table') AS c;
SELECT count(c) FROM show_chunks(:'drop_chunks_mat_table') AS c;

SELECT * FROM drop_chunks_view ORDER BY 1;

-- cannot drop directly from the materialization table without specifying
-- cont. aggregate view name explicitly
\set ON_ERROR_STOP 0

SELECT drop_chunks(
    newer_than => -20,
    verbose => true,
    cascade_to_materializations=>true);

\set ON_ERROR_STOP 1

SELECT count(c) FROM show_chunks('drop_chunks_table') AS c;
SELECT count(c) FROM show_chunks(:'drop_chunks_mat_table') AS c;

SELECT * FROM drop_chunks_view ORDER BY 1;

-- cannot drop from the raw table without specifying cascade_to_materializations

\set ON_ERROR_STOP 0
SELECT drop_chunks(table_name => 'drop_chunks_table', older_than => 10);
\set ON_ERROR_STOP 1

SELECT count(c) FROM show_chunks('drop_chunks_table') AS c;
SELECT count(c) FROM show_chunks(:'drop_chunks_mat_table') AS c;

SELECT * FROM drop_chunks_view ORDER BY 1;

\set ON_ERROR_STOP 0
SELECT drop_chunks(older_than => 200);
\set ON_ERROR_STOP 1

SELECT count(c) FROM show_chunks('drop_chunks_table') AS c;
SELECT count(c) FROM show_chunks(:'drop_chunks_mat_table') AS c;

SELECT * FROM drop_chunks_view ORDER BY 1;

-- show_chunks and drop_chunks output should be the same
\set QUERY1 'SELECT show_chunks(hypertable => \'drop_chunks_table\', older_than => 13)::REGCLASS::TEXT'
\set QUERY2 'SELECT drop_chunks(table_name => \'drop_chunks_table\', older_than => 13, cascade_to_materializations => true)::TEXT'
\set ECHO errors
\ir :QUERY_RESULT_TEST_EQUAL_RELPATH
\set ECHO all

SELECT count(c) FROM show_chunks('drop_chunks_table') AS c;
SELECT count(c) FROM show_chunks(:'drop_chunks_mat_table') AS c;

SELECT * FROM drop_chunks_view ORDER BY 1;

-- drop chunks when the chunksize and time_bucket aren't aligned
DROP TABLE drop_chunks_table CASCADE;
CREATE TABLE drop_chunks_table_u(time BIGINT, data INTEGER);
SELECT hypertable_id AS drop_chunks_table_u_id
    FROM create_hypertable('drop_chunks_table_u', 'time', chunk_time_interval => 7) \gset

CREATE OR REPLACE FUNCTION integer_now_test1() returns bigint LANGUAGE SQL STABLE as $$ SELECT coalesce(max(time), bigint '0') FROM drop_chunks_table_u $$;
SELECT set_integer_now_func('drop_chunks_table_u', 'integer_now_test1');

CREATE VIEW drop_chunks_view
  WITH (
    timescaledb.continuous,
    timescaledb.materialized_only=true,
    timescaledb.refresh_interval='72 hours'
  )
AS SELECT time_bucket('3', time), COUNT(data)
    FROM drop_chunks_table_u
    GROUP BY 1;

SELECT format('%s.%s', schema_name, table_name) AS drop_chunks_mat_table_u,
        schema_name AS drop_chunks_mat_schema,
        table_name AS drop_chunks_mat_table_u_name
    FROM _timescaledb_catalog.hypertable, _timescaledb_catalog.continuous_agg
    WHERE _timescaledb_catalog.continuous_agg.raw_hypertable_id = :drop_chunks_table_u_id
        AND _timescaledb_catalog.hypertable.id = _timescaledb_catalog.continuous_agg.mat_hypertable_id \gset

-- create 3 chunks, with 3 time bucket
INSERT INTO drop_chunks_table_u SELECT i, i FROM generate_series(0, 21) AS i;
REFRESH MATERIALIZED VIEW drop_chunks_view;

SELECT count(c) FROM show_chunks('drop_chunks_table_u') AS c;
SELECT count(c) FROM show_chunks(:'drop_chunks_mat_table_u') AS c;

SELECT * FROM drop_chunks_view ORDER BY 1;

-- show_chunks and drop_chunks output should be the same
\set QUERY1 'SELECT show_chunks(hypertable => \'drop_chunks_table_u\', older_than => 13)::REGCLASS::TEXT'
\set QUERY2 'SELECT drop_chunks(table_name => \'drop_chunks_table_u\', older_than => 13, cascade_to_materializations => true)::TEXT'
\set ECHO errors
\ir :QUERY_RESULT_TEST_EQUAL_RELPATH
\set ECHO all

-- everything in the first chunk (values within [0, 6]) should be dropped
-- the time_bucket [6, 8] will lose it's first value, but should still have
-- the other two
SELECT count(c) FROM show_chunks('drop_chunks_table_u') AS c;
SELECT count(c) FROM show_chunks(:'drop_chunks_mat_table_u') AS c;

SELECT * FROM drop_chunks_view ORDER BY 1;

-- TRUNCATE test
\set ON_ERROR_STOP 0
TRUNCATE drop_chunks_table_u;
TRUNCATE :drop_chunks_mat_table_u;
\set ON_ERROR_STOP 1

-- ALTER TABLE tests
\set ON_ERROR_STOP 0
-- test a variety of ALTER TABLE statements
ALTER TABLE :drop_chunks_mat_table_u RENAME chunk_id TO bad_name;
ALTER TABLE :drop_chunks_mat_table_u ADD UNIQUE(chunk_id);
ALTER TABLE :drop_chunks_mat_table_u SET UNLOGGED;
ALTER TABLE :drop_chunks_mat_table_u ENABLE ROW LEVEL SECURITY;
ALTER TABLE :drop_chunks_mat_table_u ADD COLUMN fizzle INTEGER;
ALTER TABLE :drop_chunks_mat_table_u DROP COLUMN chunk_id;
ALTER TABLE :drop_chunks_mat_table_u ALTER COLUMN chunk_id DROP NOT NULL;
ALTER TABLE :drop_chunks_mat_table_u ALTER COLUMN chunk_id SET DEFAULT 1;
ALTER TABLE :drop_chunks_mat_table_u ALTER COLUMN chunk_id SET STORAGE EXTERNAL;
ALTER TABLE :drop_chunks_mat_table_u DISABLE TRIGGER ALL;
ALTER TABLE :drop_chunks_mat_table_u SET TABLESPACE foo;
ALTER TABLE :drop_chunks_mat_table_u NOT OF;
ALTER TABLE :drop_chunks_mat_table_u OWNER TO CURRENT_USER;
\set ON_ERROR_STOP 1

ALTER TABLE :drop_chunks_mat_table_u SET SCHEMA public;
ALTER TABLE :drop_chunks_mat_table_u_name RENAME TO new_name;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
SET client_min_messages TO LOG;

CREATE INDEX new_name_idx ON new_name(chunk_id);

SELECT * FROM new_name;

SELECT * FROM drop_chunks_view ORDER BY 1;

\set ON_ERROR_STOP 0

-- no continuous aggregates on a continuous aggregate materialization table
CREATE VIEW new_name_view
  WITH (
    timescaledb.continuous,
    timescaledb.materialized_only=true,
    timescaledb.refresh_interval='72 hours'
  )
AS SELECT time_bucket('6', time_bucket), COUNT(agg_2_2)
    FROM new_name
    GROUP BY 1;

-- cannot create a continuous aggregate on a continuous aggregate view
CREATE VIEW drop_chunks_view_view
  WITH (
    timescaledb.continuous,
    timescaledb.materialized_only=true,
    timescaledb.refresh_interval='72 hours'
  )
AS SELECT time_bucket('6', time_bucket), SUM(count)
    FROM drop_chunks_view
    GROUP BY 1;
\set ON_ERROR_STOP 1

DROP INDEX new_name_idx;

CREATE TABLE metrics(time timestamptz, device_id int, v1 float, v2 float);
SELECT create_hypertable('metrics','time');

INSERT INTO metrics SELECT generate_series('2000-01-01'::timestamptz,'2000-01-10','1m'),1,0.25,0.75;

-- check expressions in view definition
CREATE VIEW cagg_expr
  WITH (timescaledb.continuous, timescaledb.materialized_only=true)
AS
SELECT
  time_bucket('1d', time) AS time,
  'Const'::text AS Const,
  4.3::numeric AS "numeric",
  first(metrics,time),
  CASE WHEN true THEN 'foo' ELSE 'bar' END,
  COALESCE(NULL,'coalesce'),
  avg(v1) + avg(v2) AS avg1,
  avg(v1+v2) AS avg2
FROM metrics
GROUP BY 1;

SET timescaledb.current_timestamp_mock = '2000-01-10';
REFRESH MATERIALIZED VIEW cagg_expr;
SELECT * FROM cagg_expr ORDER BY time LIMIT 5;

--
-- cascade_to_materialization = false tests
--
DROP TABLE IF EXISTS drop_chunks_table CASCADE;
DROP TABLE IF EXISTS drop_chunks_table_u CASCADE;
CREATE TABLE drop_chunks_table(time BIGINT, data INTEGER);
SELECT hypertable_id AS drop_chunks_table_nid
    FROM create_hypertable('drop_chunks_table', 'time', chunk_time_interval => 10) \gset

CREATE OR REPLACE FUNCTION integer_now_test2() returns bigint LANGUAGE SQL STABLE as $$ SELECT coalesce(max(time), bigint '0') FROM drop_chunks_table $$;
SELECT set_integer_now_func('drop_chunks_table', 'integer_now_test2');

CREATE VIEW drop_chunks_view
  WITH (
    timescaledb.continuous,
    timescaledb.materialized_only=true,
    timescaledb.refresh_interval='72 hours',
    timescaledb.refresh_lag = '-5',
    timescaledb.max_interval_per_job=10
  )
AS SELECT time_bucket('5', time), max(data)
    FROM drop_chunks_table
    GROUP BY 1;

INSERT INTO drop_chunks_table SELECT i, i FROM generate_series(0, 20) AS i;

\set ON_ERROR_STOP 0
SELECT drop_chunks(table_name => 'drop_chunks_table', older_than => 13, cascade_to_materializations => false);

ALTER VIEW drop_chunks_view SET (timescaledb.ignore_invalidation_older_than = 9);

-- 9 is too small (less than timescaledb.ignore_invalidation_older_than)
SELECT drop_chunks(table_name => 'drop_chunks_table', older_than => (integer_now_test2()-8), cascade_to_materializations => false);
-- 10 works but we don't have the completion threshold far enough along
SELECT drop_chunks(table_name => 'drop_chunks_table', older_than => (integer_now_test2()-9), cascade_to_materializations => false);
\set ON_ERROR_STOP 1

REFRESH MATERIALIZED VIEW drop_chunks_view;
\set ON_ERROR_STOP 0
--still too far behind
SELECT drop_chunks(table_name => 'drop_chunks_table', older_than => (integer_now_test2()-9), cascade_to_materializations => false);
\set ON_ERROR_STOP 1
REFRESH MATERIALIZED VIEW drop_chunks_view;
REFRESH MATERIALIZED VIEW drop_chunks_view;
--now, this works
SELECT drop_chunks(table_name => 'drop_chunks_table', older_than => (integer_now_test2()-9), cascade_to_materializations => false);

\set ON_ERROR_STOP 0
--must have older_than set and no newer than
SELECT drop_chunks(table_name => 'drop_chunks_table', cascade_to_materializations => false);
SELECT drop_chunks(table_name => 'drop_chunks_table', newer_than=>10, cascade_to_materializations => false);
SELECT drop_chunks(table_name => 'drop_chunks_table', older_than => 20, newer_than=>10, cascade_to_materializations => false);
\set ON_ERROR_STOP 1

--test materialization of invalidation before drop

SELECT * FROM drop_chunks_table ORDER BY time ASC limit 1;

INSERT INTO drop_chunks_table SELECT i, i FROM generate_series(20, 35) AS i;
REFRESH MATERIALIZED VIEW drop_chunks_view;
REFRESH MATERIALIZED VIEW drop_chunks_view;
--this is invalidated but beyond ignore_invalidation_threshold so will never be seen (current time is 29)
INSERT INTO drop_chunks_table SELECT i, 100 FROM generate_series(10, 19) AS i;
--this will be seen after the drop its within the invalidation window and will be dropped
INSERT INTO drop_chunks_table VALUES (26, 100);
--this will not be processed by the drop since chunk 30-39 is not dropped but will be seen after refresh
--shows that the drop doesn't do more work than necessary
INSERT INTO drop_chunks_table VALUES (31, 200);
--move the time up to 39
INSERT INTO drop_chunks_table SELECT i, i FROM generate_series(35, 39) AS i;
--the invalidation on 25 not yet seen
SELECT * FROM drop_chunks_view ORDER BY time_bucket DESC;
--dropping tables will cause the invalidation to be processed
SELECT drop_chunks(table_name => 'drop_chunks_table', older_than => (integer_now_test2()-9), cascade_to_materializations => false);
--new values on 25 now seen in view
SELECT * FROM drop_chunks_view ORDER BY time_bucket DESC;
--earliest datapoint now in table
SELECT * FROM drop_chunks_table ORDER BY time ASC limit 1;
--we see the chunks row with the dropped flags set;
SELECT * FROM _timescaledb_catalog.chunk where dropped;
--still see data in the view
SELECT * FROM drop_chunks_view WHERE time_bucket < (integer_now_test2()-9) ORDER BY time_bucket DESC;
--no data but covers dropped chunks
SELECT * FROM drop_chunks_table WHERE time < (integer_now_test2()-9) ORDER BY time DESC;
--recreate the dropped chunk
INSERT INTO drop_chunks_table SELECT i, i FROM generate_series(0, 20) AS i;
--see data from recreated region
SELECT * FROM drop_chunks_table WHERE time < (integer_now_test2()-9) ORDER BY time DESC;
REFRESH MATERIALIZED VIEW drop_chunks_view;
REFRESH MATERIALIZED VIEW drop_chunks_view;
--change to bucket 31 also seen
SELECT * FROM drop_chunks_view ORDER BY time_bucket DESC;

--show that the invalidation processed during drop aren't limited by max_interval_per_job
ALTER VIEW drop_chunks_view SET (timescaledb.max_interval_per_job = 5);
INSERT INTO drop_chunks_table SELECT i, 300+i FROM generate_series(31, 39) AS i;
--move the time up to 49
INSERT INTO drop_chunks_table SELECT i, i FROM generate_series(40, 49) AS i;
SELECT * FROM drop_chunks_view ORDER BY time_bucket DESC;
--should see multiple rounds of invalidation in the log messages
SELECT drop_chunks(table_name => 'drop_chunks_table', older_than => (integer_now_test2()-9), cascade_to_materializations => false);
--see both 30 and 35 updated
SELECT * FROM drop_chunks_view ORDER BY time_bucket DESC;

--test splitting one range for invalidation in drop_chunks and then later
ALTER VIEW drop_chunks_view SET (timescaledb.max_interval_per_job = 100);
INSERT INTO drop_chunks_table SELECT i, i FROM generate_series(50,55) AS i;
REFRESH MATERIALIZED VIEW drop_chunks_view;
--one command and thus one range that spans 46 (which will be processed by drop_chunks) and (51 which won't, but will be later)
INSERT INTO drop_chunks_table VALUES (46, 400), (51, 500);
INSERT INTO drop_chunks_table SELECT i, i FROM generate_series(56,59) AS i;
--neither invalidation is seen
SELECT * FROM drop_chunks_view ORDER BY time_bucket DESC;
SELECT drop_chunks(table_name => 'drop_chunks_table', older_than => (integer_now_test2()-9), cascade_to_materializations => false);
--the change in bucket 45 but not 50 is seen
SELECT * FROM drop_chunks_view ORDER BY time_bucket DESC;
REFRESH MATERIALIZED VIEW drop_chunks_view;
--the change in bucket 50 is seen
SELECT * FROM drop_chunks_view ORDER BY time_bucket DESC;
--no data but covers dropped chunks
SELECT * FROM drop_chunks_table WHERE time < (integer_now_test2()-9) ORDER BY time DESC;
SELECT set_chunk_time_interval('drop_chunks_table', 1000);
SELECT chunk_table, ranges FROM chunk_relation_size('drop_chunks_table');
--recreate the dropped chunk
INSERT INTO drop_chunks_table VALUES (20, 20);
--now sees the re-entered data
SELECT * FROM drop_chunks_table WHERE time < (integer_now_test2()-9) ORDER BY time DESC;
--should show chunk with old name and old ranges
SELECT chunk_table, ranges FROM chunk_relation_size('drop_chunks_table');

-- TEST drop_chunks with cascade_to_materialization set to true (github 1644)
-- This checks if chunks from mat. hypertable are actually dropped
-- and deletes data from chunks that cannot be dropped from that mat. hypertable.
SELECT format('%s.%s', schema_name, table_name) AS drop_chunks_mat_tablen,
        schema_name AS drop_chunks_mat_schema,
        table_name AS drop_chunks_mat_table_name
    FROM _timescaledb_catalog.hypertable, _timescaledb_catalog.continuous_agg
    WHERE _timescaledb_catalog.continuous_agg.raw_hypertable_id = :drop_chunks_table_nid
        AND _timescaledb_catalog.hypertable.id = _timescaledb_catalog.continuous_agg.mat_hypertable_id \gset

SELECT drop_chunks(table_name => 'drop_chunks_table', older_than=>integer_now_test2() + 200, cascade_to_materializations => true);
SELECT count(c) FROM show_chunks('drop_chunks_table') AS c;
SELECT count(c) FROM show_chunks(:'drop_chunks_mat_tablen') AS c;

SELECT set_chunk_time_interval('drop_chunks_table', 10);
SELECT set_chunk_time_interval(:'drop_chunks_mat_tablen', 20);
INSERT INTO drop_chunks_table SELECT generate_series(1,35), 100;
update drop_chunks_table set data = 250 where time = 25;
update drop_chunks_table set data = 290 where time = 29;
REFRESH materialized view drop_chunks_view;
REFRESH materialized view drop_chunks_view;
--now we have chunks in both mat and raw hypertables
select * from drop_chunks_view order by 1;
SELECT chunk_table, ranges FROM chunk_relation_size('drop_chunks_table')
ORDER BY ranges;
SELECT chunk_table, ranges FROM chunk_relation_size(:'drop_chunks_mat_tablen')
ORDER BY ranges;
--1 chunk from the mat. hypertable will be dropped and the other will
--need deletes when the chunks from the raw hypertable are dropped.
SELECT drop_chunks(table_name => 'drop_chunks_table', older_than=>integer_now_test2() - 4 , cascade_to_materializations => true);
SELECT * from drop_chunks_view ORDER BY 1;
SELECT count(c) FROM show_chunks(:'drop_chunks_mat_tablen') AS c;
SELECT chunk_table, ranges FROM chunk_relation_size(:'drop_chunks_mat_tablen')
ORDER BY ranges;

-- TEST drop chunks from continuous aggregates by specifying view name
SELECT drop_chunks(
    table_name => 'drop_chunks_view',
    newer_than => -20,
    verbose => true);

--can also drop chunks by specifying materialized hypertable name
INSERT INTO drop_chunks_table SELECT generate_series(45, 55), 500;
REFRESH MATERIALIZED VIEW drop_chunks_view;
SELECT chunk_table, ranges FROM chunk_relation_size(:'drop_chunks_mat_tablen');
\set ON_ERROR_STOP 0
SELECT drop_chunks(
    table_name => :'drop_chunks_mat_table_name',
    older_than => 60,
    verbose => true);
\set ON_ERROR_STOP 1
SELECT drop_chunks(
    schema_name => :'drop_chunks_mat_schema',
    table_name => :'drop_chunks_mat_table_name',
    older_than => 60,
    verbose => true);
