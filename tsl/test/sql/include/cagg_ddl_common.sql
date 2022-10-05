-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Set this variable to avoid using a hard-coded path each time query
-- results are compared
\set QUERY_RESULT_TEST_EQUAL_RELPATH '../../../../test/sql/include/query_result_test_equal.sql'

\if :IS_DISTRIBUTED
\echo 'Running distributed hypertable tests'
\else
\echo 'Running local hypertable tests'
\endif

SET ROLE :ROLE_DEFAULT_PERM_USER;

--DDL commands on continuous aggregates

CREATE TABLE conditions (
      timec        TIMESTAMPTZ       NOT NULL,
      location    TEXT              NOT NULL,
      temperature integer  NULL,
      humidity    DOUBLE PRECISION  NULL,
      timemeasure TIMESTAMPTZ,
      timeinterval INTERVAL
);

\if :IS_DISTRIBUTED
SELECT table_name FROM create_distributed_hypertable('conditions', 'timec', replication_factor => 2);
\else
SELECT table_name FROM create_hypertable('conditions', 'timec');
\endif

-- schema tests

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER
CREATE TABLESPACE tablespace1 OWNER :ROLE_DEFAULT_PERM_USER LOCATION :TEST_TABLESPACE1_PATH;
CREATE TABLESPACE tablespace2 OWNER :ROLE_DEFAULT_PERM_USER LOCATION :TEST_TABLESPACE2_PATH;

CREATE SCHEMA rename_schema;
GRANT ALL ON SCHEMA rename_schema TO :ROLE_DEFAULT_PERM_USER;

SET ROLE :ROLE_DEFAULT_PERM_USER;

CREATE TABLE foo(time TIMESTAMPTZ NOT NULL, data INTEGER);

\if :IS_DISTRIBUTED
SELECT create_distributed_hypertable('foo', 'time', replication_factor => 2);
\else
SELECT create_hypertable('foo', 'time');
\endif

CREATE MATERIALIZED VIEW rename_test
  WITH ( timescaledb.continuous, timescaledb.materialized_only=true)
AS SELECT time_bucket('1week', time), COUNT(data)
    FROM foo
    GROUP BY 1 WITH NO DATA;

SELECT user_view_schema, user_view_name, partial_view_schema, partial_view_name
      FROM _timescaledb_catalog.continuous_agg;

ALTER MATERIALIZED VIEW rename_test SET SCHEMA rename_schema;

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

RESET ROLE;
SELECT current_user;

ALTER VIEW :"PART_VIEW_SCHEMA".:"PART_VIEW_NAME" SET SCHEMA public;
SET ROLE :ROLE_DEFAULT_PERM_USER;

SELECT user_view_schema, user_view_name, partial_view_schema, partial_view_name
      FROM _timescaledb_catalog.continuous_agg;

--alter direct view schema
SELECT user_view_schema, user_view_name, direct_view_schema, direct_view_name
      FROM _timescaledb_catalog.continuous_agg;

RESET ROLE;
SELECT current_user;
ALTER VIEW :"DIR_VIEW_SCHEMA".:"DIR_VIEW_NAME" SET SCHEMA public;
SET ROLE :ROLE_DEFAULT_PERM_USER;

SELECT user_view_schema, user_view_name, partial_view_schema, partial_view_name,
      direct_view_schema, direct_view_name
      FROM _timescaledb_catalog.continuous_agg;

RESET ROLE;
SELECT current_user;
ALTER SCHEMA rename_schema RENAME TO new_name_schema;
SET ROLE :ROLE_DEFAULT_PERM_USER;

SELECT user_view_schema, user_view_name, partial_view_schema, partial_view_name,
       direct_view_schema, direct_view_name
      FROM _timescaledb_catalog.continuous_agg;

ALTER VIEW :"PART_VIEW_NAME" SET SCHEMA new_name_schema;
ALTER VIEW :"DIR_VIEW_NAME" SET SCHEMA new_name_schema;

SELECT user_view_schema, user_view_name, partial_view_schema, partial_view_name,
       direct_view_schema, direct_view_name
      FROM _timescaledb_catalog.continuous_agg;

RESET ROLE;
SELECT current_user;
ALTER SCHEMA new_name_schema RENAME TO foo_name_schema;
SET ROLE :ROLE_DEFAULT_PERM_USER;

SELECT user_view_schema, user_view_name, partial_view_schema, partial_view_name
      FROM _timescaledb_catalog.continuous_agg;

ALTER MATERIALIZED VIEW foo_name_schema.rename_test SET SCHEMA public;

SELECT user_view_schema, user_view_name, partial_view_schema, partial_view_name
      FROM _timescaledb_catalog.continuous_agg;

RESET ROLE;
SELECT current_user;
ALTER SCHEMA foo_name_schema RENAME TO rename_schema;
SET ROLE :ROLE_DEFAULT_PERM_USER;

SET client_min_messages TO NOTICE;

SELECT user_view_schema, user_view_name, partial_view_schema, partial_view_name
      FROM _timescaledb_catalog.continuous_agg;

ALTER MATERIALIZED VIEW rename_test RENAME TO rename_c_aggregate;

SELECT user_view_schema, user_view_name, partial_view_schema, partial_view_name
      FROM _timescaledb_catalog.continuous_agg;

SELECT * FROM rename_c_aggregate;

ALTER VIEW rename_schema.:"PART_VIEW_NAME" RENAME TO partial_view;

SELECT user_view_schema, user_view_name, partial_view_schema, partial_view_name,
      direct_view_schema, direct_view_name
      FROM _timescaledb_catalog.continuous_agg;

--rename direct view
ALTER VIEW rename_schema.:"DIR_VIEW_NAME" RENAME TO direct_view;
SELECT user_view_schema, user_view_name, partial_view_schema, partial_view_name,
      direct_view_schema, direct_view_name
      FROM _timescaledb_catalog.continuous_agg;

-- drop_chunks tests
DROP TABLE conditions CASCADE;
DROP TABLE foo CASCADE;

CREATE TABLE drop_chunks_table(time BIGINT NOT NULL, data INTEGER);
\if :IS_DISTRIBUTED
SELECT hypertable_id AS drop_chunks_table_id
    FROM create_distributed_hypertable('drop_chunks_table', 'time', chunk_time_interval => 10, replication_factor => 2) \gset
\else
SELECT hypertable_id AS drop_chunks_table_id
    FROM create_hypertable('drop_chunks_table', 'time', chunk_time_interval => 10) \gset
\endif

CREATE OR REPLACE FUNCTION integer_now_test() returns bigint LANGUAGE SQL STABLE as $$ SELECT coalesce(max(time), bigint '0') FROM drop_chunks_table $$;
\if :IS_DISTRIBUTED
CALL distributed_exec($DIST$
CREATE OR REPLACE FUNCTION integer_now_test() returns bigint LANGUAGE SQL STABLE as $$ SELECT coalesce(max(time), bigint '0') FROM drop_chunks_table $$;
$DIST$);
\endif

SELECT set_integer_now_func('drop_chunks_table', 'integer_now_test');

CREATE MATERIALIZED VIEW drop_chunks_view
  WITH (
    timescaledb.continuous,
    timescaledb.materialized_only=true
  )
AS SELECT time_bucket('5', time), COUNT(data)
    FROM drop_chunks_table
    GROUP BY 1 WITH NO DATA;

SELECT format('%I.%I', schema_name, table_name) AS drop_chunks_mat_table,
        schema_name AS drop_chunks_mat_schema,
        table_name AS drop_chunks_mat_table_name
    FROM _timescaledb_catalog.hypertable, _timescaledb_catalog.continuous_agg
    WHERE _timescaledb_catalog.continuous_agg.raw_hypertable_id = :drop_chunks_table_id
        AND _timescaledb_catalog.hypertable.id = _timescaledb_catalog.continuous_agg.mat_hypertable_id \gset

-- create 3 chunks, with 3 time bucket
INSERT INTO drop_chunks_table SELECT i, i FROM generate_series(0, 29) AS i;
-- Only refresh up to bucket 15 initially. Matches the old refresh
-- behavior that didn't materialize everything
CALL refresh_continuous_aggregate('drop_chunks_view', 0, 15);
SELECT count(c) FROM show_chunks('drop_chunks_table') AS c;
SELECT count(c) FROM show_chunks('drop_chunks_view') AS c;

SELECT * FROM drop_chunks_view ORDER BY 1;

-- cannot drop directly from the materialization table without specifying
-- cont. aggregate view name explicitly

\set ON_ERROR_STOP 0
SELECT drop_chunks(:'drop_chunks_mat_table',
    newer_than => -20,
    verbose => true);
\set ON_ERROR_STOP 1

SELECT count(c) FROM show_chunks('drop_chunks_table') AS c;
SELECT count(c) FROM show_chunks('drop_chunks_view') AS c;

SELECT * FROM drop_chunks_view ORDER BY 1;

-- drop chunks when the chunksize and time_bucket aren't aligned
DROP TABLE drop_chunks_table CASCADE;

CREATE TABLE drop_chunks_table_u(time BIGINT NOT NULL, data INTEGER);
\if :IS_DISTRIBUTED
SELECT hypertable_id AS drop_chunks_table_u_id
    FROM create_distributed_hypertable('drop_chunks_table_u', 'time', chunk_time_interval => 7, replication_factor => 2) \gset
\else
SELECT hypertable_id AS drop_chunks_table_u_id
    FROM create_hypertable('drop_chunks_table_u', 'time', chunk_time_interval => 7) \gset
\endif

CREATE OR REPLACE FUNCTION integer_now_test1() returns bigint LANGUAGE SQL STABLE as $$ SELECT coalesce(max(time), bigint '0') FROM drop_chunks_table_u $$;
\if :IS_DISTRIBUTED
CALL distributed_exec($DIST$
CREATE OR REPLACE FUNCTION integer_now_test1() returns bigint LANGUAGE SQL STABLE as $$ SELECT coalesce(max(time), bigint '0') FROM drop_chunks_table_u $$;
$DIST$);
\endif
SELECT set_integer_now_func('drop_chunks_table_u', 'integer_now_test1');

CREATE MATERIALIZED VIEW drop_chunks_view
  WITH (
    timescaledb.continuous,
    timescaledb.materialized_only=true
  )
AS SELECT time_bucket('3', time), COUNT(data)
    FROM drop_chunks_table_u
    GROUP BY 1 WITH NO DATA;

SELECT format('%I.%I', schema_name, table_name) AS drop_chunks_mat_table_u,
        schema_name AS drop_chunks_mat_schema,
        table_name AS drop_chunks_mat_table_u_name
    FROM _timescaledb_catalog.hypertable, _timescaledb_catalog.continuous_agg
    WHERE _timescaledb_catalog.continuous_agg.raw_hypertable_id = :drop_chunks_table_u_id
        AND _timescaledb_catalog.hypertable.id = _timescaledb_catalog.continuous_agg.mat_hypertable_id \gset

-- create 3 chunks, with 3 time bucket
INSERT INTO drop_chunks_table_u SELECT i, i FROM generate_series(0, 21) AS i;
-- Refresh up to bucket 15 to match old materializer behavior
CALL refresh_continuous_aggregate('drop_chunks_view', 0, 15);
SELECT count(c) FROM show_chunks('drop_chunks_table_u') AS c;
SELECT count(c) FROM show_chunks('drop_chunks_view') AS c;

SELECT * FROM drop_chunks_view ORDER BY 1;

-- TRUNCATE test
-- Can truncate regular hypertables that have caggs
TRUNCATE drop_chunks_table_u;
\set ON_ERROR_STOP 0
-- Can't truncate materialized hypertables directly
TRUNCATE :drop_chunks_mat_table_u;
\set ON_ERROR_STOP 1

-- Check that we don't interfere with TRUNCATE of normal table and
-- partitioned table
CREATE TABLE truncate (value int);
INSERT INTO truncate VALUES (1), (2);
TRUNCATE truncate;
SELECT * FROM truncate;
CREATE TABLE truncate_partitioned (value int)
  PARTITION BY RANGE(value);
CREATE TABLE truncate_p1 PARTITION OF truncate_partitioned
  FOR VALUES FROM (1) TO (3);
INSERT INTO truncate_partitioned VALUES (1), (2);
TRUNCATE truncate_partitioned;
SELECT * FROM truncate_partitioned;

-- ALTER TABLE tests
\set ON_ERROR_STOP 0
-- test a variety of ALTER TABLE statements
ALTER TABLE :drop_chunks_mat_table_u RENAME time_bucket TO bad_name;
ALTER TABLE :drop_chunks_mat_table_u ADD UNIQUE(time_bucket);
ALTER TABLE :drop_chunks_mat_table_u SET UNLOGGED;
ALTER TABLE :drop_chunks_mat_table_u ENABLE ROW LEVEL SECURITY;
ALTER TABLE :drop_chunks_mat_table_u ADD COLUMN fizzle INTEGER;
ALTER TABLE :drop_chunks_mat_table_u DROP COLUMN time_bucket;
ALTER TABLE :drop_chunks_mat_table_u ALTER COLUMN time_bucket DROP NOT NULL;
ALTER TABLE :drop_chunks_mat_table_u ALTER COLUMN time_bucket SET DEFAULT 1;
ALTER TABLE :drop_chunks_mat_table_u ALTER COLUMN time_bucket SET STORAGE EXTERNAL;
ALTER TABLE :drop_chunks_mat_table_u DISABLE TRIGGER ALL;
ALTER TABLE :drop_chunks_mat_table_u SET TABLESPACE foo;
ALTER TABLE :drop_chunks_mat_table_u NOT OF;
ALTER TABLE :drop_chunks_mat_table_u OWNER TO CURRENT_USER;
\set ON_ERROR_STOP 1

ALTER TABLE :drop_chunks_mat_table_u SET SCHEMA public;
ALTER TABLE :drop_chunks_mat_table_u_name RENAME TO new_name;

SET ROLE :ROLE_DEFAULT_PERM_USER;
SET client_min_messages TO NOTICE;

SELECT * FROM new_name;

SELECT * FROM drop_chunks_view ORDER BY 1;

\set ON_ERROR_STOP 0

-- no continuous aggregates on a continuous aggregate materialization table
CREATE MATERIALIZED VIEW new_name_view
  WITH (
    timescaledb.continuous,
    timescaledb.materialized_only=true
  )
AS SELECT time_bucket('6', time_bucket), COUNT("count")
    FROM new_name
    GROUP BY 1 WITH NO DATA;
\set ON_ERROR_STOP 1

CREATE TABLE metrics(time timestamptz NOT NULL, device_id int, v1 float, v2 float);
\if :IS_DISTRIBUTED
SELECT create_distributed_hypertable('metrics', 'time', replication_factor => 2);
\else
SELECT create_hypertable('metrics','time');
\endif

INSERT INTO metrics SELECT generate_series('2000-01-01'::timestamptz,'2000-01-10','1m'),1,0.25,0.75;

-- check expressions in view definition
CREATE MATERIALIZED VIEW cagg_expr
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
GROUP BY 1 WITH NO DATA;

CALL refresh_continuous_aggregate('cagg_expr', NULL, NULL);
SELECT * FROM cagg_expr ORDER BY time LIMIT 5;

--test materialization of invalidation before drop
DROP TABLE IF EXISTS drop_chunks_table CASCADE;
DROP TABLE IF EXISTS drop_chunks_table_u CASCADE;

CREATE TABLE drop_chunks_table(time BIGINT NOT NULL, data INTEGER);
\if :IS_DISTRIBUTED
SELECT hypertable_id AS drop_chunks_table_nid
    FROM create_distributed_hypertable('drop_chunks_table', 'time', chunk_time_interval => 10, replication_factor => 2) \gset
\else
SELECT hypertable_id AS drop_chunks_table_nid
    FROM create_hypertable('drop_chunks_table', 'time', chunk_time_interval => 10) \gset
\endif

CREATE OR REPLACE FUNCTION integer_now_test2() returns bigint LANGUAGE SQL STABLE as $$ SELECT coalesce(max(time), bigint '0') FROM drop_chunks_table $$;
\if :IS_DISTRIBUTED
CALL distributed_exec($DIST$
CREATE OR REPLACE FUNCTION integer_now_test2() returns bigint LANGUAGE SQL STABLE as $$ SELECT coalesce(max(time), bigint '0') FROM drop_chunks_table $$;
$DIST$);
\endif
SELECT set_integer_now_func('drop_chunks_table', 'integer_now_test2');

CREATE MATERIALIZED VIEW drop_chunks_view
  WITH (
    timescaledb.continuous,
    timescaledb.materialized_only=true
  )
AS SELECT time_bucket('5', time), max(data)
    FROM drop_chunks_table
    GROUP BY 1 WITH NO DATA;

INSERT INTO drop_chunks_table SELECT i, i FROM generate_series(0, 20) AS i;
--dropping chunks will process the invalidations
SELECT drop_chunks('drop_chunks_table', older_than => (integer_now_test2()-9));
SELECT * FROM drop_chunks_table ORDER BY time ASC limit 1;

INSERT INTO drop_chunks_table SELECT i, i FROM generate_series(20, 35) AS i;
CALL refresh_continuous_aggregate('drop_chunks_view', 10, 40);

--this will be seen after the drop its within the invalidation window and will be dropped
INSERT INTO drop_chunks_table VALUES (26, 100);
--this will not be processed by the drop since chunk 30-39 is not dropped but will be seen after refresh
--shows that the drop doesn't do more work than necessary
INSERT INTO drop_chunks_table VALUES (31, 200);
--move the time up to 39
INSERT INTO drop_chunks_table SELECT i, i FROM generate_series(35, 39) AS i;

--the chunks and ranges we have thus far
SELECT chunk_name, range_start_integer, range_end_integer
FROM timescaledb_information.chunks
WHERE hypertable_name = 'drop_chunks_table';

--the invalidation on 25 not yet seen
SELECT * FROM drop_chunks_view ORDER BY time_bucket DESC;

--refresh to process the invalidations and then drop
CALL refresh_continuous_aggregate('drop_chunks_view', NULL, (integer_now_test2()-9));
SELECT drop_chunks('drop_chunks_table', older_than => (integer_now_test2()-9));

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

--should show chunk with old name and old ranges
SELECT chunk_name, range_start_integer, range_end_integer
FROM timescaledb_information.chunks
WHERE hypertable_name = 'drop_chunks_table'
ORDER BY range_start_integer;

--We dropped everything up to the bucket starting at 30 and then
--inserted new data up to and including time 20. Therefore, the
--dropped data should stay the same as long as we only refresh
--buckets that have non-dropped data.
CALL refresh_continuous_aggregate('drop_chunks_view', 30, 40);
SELECT * FROM drop_chunks_view ORDER BY time_bucket DESC;


SELECT format('%I.%I', schema_name, table_name) AS drop_chunks_mat_tablen,
        schema_name AS drop_chunks_mat_schema,
        table_name AS drop_chunks_mat_table_name
    FROM _timescaledb_catalog.hypertable, _timescaledb_catalog.continuous_agg
    WHERE _timescaledb_catalog.continuous_agg.raw_hypertable_id = :drop_chunks_table_nid
        AND _timescaledb_catalog.hypertable.id = _timescaledb_catalog.continuous_agg.mat_hypertable_id \gset

-- TEST drop chunks from continuous aggregates by specifying view name
SELECT drop_chunks('drop_chunks_view',
    newer_than => -20,
    verbose => true);

-- Test that we cannot drop chunks when specifying materialized
-- hypertable
INSERT INTO drop_chunks_table SELECT generate_series(45, 55), 500;
CALL refresh_continuous_aggregate('drop_chunks_view', 45, 55);

SELECT chunk_name, range_start_integer, range_end_integer
FROM timescaledb_information.chunks
WHERE hypertable_name = :'drop_chunks_mat_table_name' ORDER BY range_start_integer;
\set ON_ERROR_STOP 0
\set VERBOSITY default
SELECT drop_chunks(:'drop_chunks_mat_tablen', older_than => 60);
\set VERBOSITY terse
\set ON_ERROR_STOP 1

-----------------------------------------------------------------
-- Test that refresh_continuous_aggregate on chunk will refresh,
-- but only in the regions covered by the show chunks.
-----------------------------------------------------------------
SELECT chunk_name, range_start_integer, range_end_integer
FROM timescaledb_information.chunks
WHERE hypertable_name = 'drop_chunks_table'
ORDER BY 2,3;

-- Pick the second chunk as the one to drop
WITH numbered_chunks AS (
     SELECT row_number() OVER (ORDER BY range_start_integer), chunk_schema, chunk_name, range_start_integer, range_end_integer
     FROM timescaledb_information.chunks
     WHERE hypertable_name = 'drop_chunks_table'
     ORDER BY 1
)
SELECT format('%I.%I', chunk_schema, chunk_name) AS chunk_to_drop, range_start_integer, range_end_integer
FROM numbered_chunks
WHERE row_number = 2 \gset

-- There's data in the table for the chunk/range we will drop
SELECT * FROM drop_chunks_table
WHERE time >= :range_start_integer
AND time < :range_end_integer
ORDER BY 1;

-- Make sure there is also data in the continuous aggregate
-- CARE:
-- Note that this behaviour of dropping the materialization table chunks and expecting a refresh
-- that overlaps that time range to NOT update those chunks is undefined. Since CAGGs over
-- distributed hypertables merge the invalidations the refresh region is updated in the distributed
-- case, which may be different than what happens in the normal hypertable case. The command was:
-- SELECT drop_chunks('drop_chunks_view', newer_than => -20, verbose => true);

CALL refresh_continuous_aggregate('drop_chunks_view', 0, 50);

SELECT * FROM drop_chunks_view
ORDER BY 1;

-- Drop the second chunk, to leave a gap in the data
\if :IS_DISTRIBUTED
CALL distributed_exec(format('DROP TABLE IF EXISTS %s', :'chunk_to_drop'));
DROP FOREIGN TABLE :chunk_to_drop;
\else
DROP TABLE :chunk_to_drop;
\endif

-- Verify that the second chunk is dropped
SELECT chunk_name, range_start_integer, range_end_integer
FROM timescaledb_information.chunks
WHERE hypertable_name = 'drop_chunks_table'
ORDER BY 2,3;

-- Data is no longer in the table but still in the view
SELECT * FROM drop_chunks_table
WHERE time >= :range_start_integer
AND time < :range_end_integer
ORDER BY 1;

SELECT * FROM drop_chunks_view
WHERE time_bucket >= :range_start_integer
AND time_bucket < :range_end_integer
ORDER BY 1;

-- Insert a large value in one of the chunks that will be dropped
INSERT INTO drop_chunks_table VALUES (:range_start_integer-1, 100);
-- Now refresh and drop the two adjecent chunks
CALL refresh_continuous_aggregate('drop_chunks_view', NULL, 30);
SELECT drop_chunks('drop_chunks_table', older_than=>30);

-- Verify that the chunks are dropped
SELECT chunk_name, range_start_integer, range_end_integer
FROM timescaledb_information.chunks
WHERE hypertable_name = 'drop_chunks_table'
ORDER BY 2,3;

-- The continuous aggregate should be refreshed in the regions covered
-- by the dropped chunks, but not in the "gap" region, i.e., the
-- region of the chunk that was dropped via DROP TABLE.
SELECT * FROM drop_chunks_view
ORDER BY 1;

-- Now refresh in the region of the first two dropped chunks
CALL refresh_continuous_aggregate('drop_chunks_view', 0, :range_end_integer);

-- Aggregate data in the refreshed range should no longer exist since
-- the underlying data was dropped.
SELECT * FROM drop_chunks_view
ORDER BY 1;

--------------------------------------------------------------------
-- Check that we can create a materialized table in a tablespace. We
-- create one with tablespace and one without and compare them.

CREATE VIEW cagg_info AS
WITH
  caggs AS (
    SELECT format('%I.%I', user_view_schema, user_view_name)::regclass AS user_view,
           format('%I.%I', direct_view_schema, direct_view_name)::regclass AS direct_view,
           format('%I.%I', partial_view_schema, partial_view_name)::regclass AS partial_view,
           format('%I.%I', ht.schema_name, ht.table_name)::regclass AS mat_relid
      FROM _timescaledb_catalog.hypertable ht,
           _timescaledb_catalog.continuous_agg cagg
     WHERE ht.id = cagg.mat_hypertable_id
  )
SELECT user_view,
       pg_get_userbyid(relowner) AS user_view_owner,
       relname AS mat_table,
       (SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = mat_relid) AS mat_table_owner,
       direct_view,
       (SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = direct_view) AS direct_view_owner,
       partial_view,
       (SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = partial_view) AS partial_view_owner,
       (SELECT spcname FROM pg_tablespace WHERE oid = reltablespace) AS tablespace
  FROM pg_class JOIN caggs ON pg_class.oid = caggs.mat_relid;
GRANT SELECT ON cagg_info TO PUBLIC;

CREATE VIEW chunk_info AS
SELECT ht.schema_name, ht.table_name, relname AS chunk_name,
       (SELECT spcname FROM pg_tablespace WHERE oid = reltablespace) AS tablespace
  FROM pg_class c,
       _timescaledb_catalog.hypertable ht,
       _timescaledb_catalog.chunk ch
 WHERE ch.table_name = c.relname AND ht.id = ch.hypertable_id;

CREATE TABLE whatever(time BIGINT NOT NULL, data INTEGER);

\if :IS_DISTRIBUTED
SELECT hypertable_id AS whatever_nid
  FROM create_distributed_hypertable('whatever', 'time', chunk_time_interval => 10, replication_factor => 2)
\gset
\else
SELECT hypertable_id AS whatever_nid
  FROM create_hypertable('whatever', 'time', chunk_time_interval => 10)
\gset
\endif

SELECT set_integer_now_func('whatever', 'integer_now_test');

CREATE MATERIALIZED VIEW whatever_view_1
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT time_bucket('5', time), COUNT(data)
  FROM whatever GROUP BY 1 WITH NO DATA;

CREATE MATERIALIZED VIEW whatever_view_2
WITH (timescaledb.continuous, timescaledb.materialized_only=true)
TABLESPACE tablespace1 AS
SELECT time_bucket('5', time), COUNT(data)
  FROM whatever GROUP BY 1 WITH NO DATA;

INSERT INTO whatever SELECT i, i FROM generate_series(0, 29) AS i;
CALL refresh_continuous_aggregate('whatever_view_1', NULL, NULL);
CALL refresh_continuous_aggregate('whatever_view_2', NULL, NULL);

SELECT user_view,
       mat_table,
       cagg_info.tablespace AS mat_tablespace,
       chunk_name,
       chunk_info.tablespace AS chunk_tablespace
  FROM cagg_info, chunk_info
 WHERE mat_table::text = table_name
   AND user_view::text LIKE 'whatever_view%';

ALTER MATERIALIZED VIEW whatever_view_1 SET TABLESPACE tablespace2;

SELECT user_view,
       mat_table,
       cagg_info.tablespace AS mat_tablespace,
       chunk_name,
       chunk_info.tablespace AS chunk_tablespace
  FROM cagg_info, chunk_info
 WHERE mat_table::text = table_name
   AND user_view::text LIKE 'whatever_view%';

DROP MATERIALIZED VIEW whatever_view_1;
DROP MATERIALIZED VIEW whatever_view_2;

-- test bucket width expressions on integer hypertables
CREATE TABLE metrics_int2 (
  time int2 NOT NULL,
  device_id int,
  v1 float,
  v2 float
);

CREATE TABLE metrics_int4 (
  time int4 NOT NULL,
  device_id int,
  v1 float,
  v2 float
);

CREATE TABLE metrics_int8 (
  time int8 NOT NULL,
  device_id int,
  v1 float,
  v2 float
);

\if :IS_DISTRIBUTED
SELECT create_distributed_hypertable (('metrics_' || dt)::regclass, 'time', chunk_time_interval => 10, replication_factor => 2)
FROM (
  VALUES ('int2'),
    ('int4'),
    ('int8')) v (dt);
\else
SELECT create_hypertable (('metrics_' || dt)::regclass, 'time', chunk_time_interval => 10)
FROM (
  VALUES ('int2'),
    ('int4'),
    ('int8')) v (dt);
\endif

CREATE OR REPLACE FUNCTION int2_now ()
  RETURNS int2
  LANGUAGE SQL
  STABLE
  AS $$
  SELECT 10::int2
$$;
\if :IS_DISTRIBUTED
CALL distributed_exec($DIST$
CREATE OR REPLACE FUNCTION int2_now ()
  RETURNS int2
  LANGUAGE SQL
  STABLE
  AS $$
  SELECT 10::int2
$$;
$DIST$);
\endif

CREATE OR REPLACE FUNCTION int4_now ()
  RETURNS int4
  LANGUAGE SQL
  STABLE
  AS $$
  SELECT 10::int4
$$;
\if :IS_DISTRIBUTED
CALL distributed_exec($DIST$
CREATE OR REPLACE FUNCTION int4_now ()
  RETURNS int4
  LANGUAGE SQL
  STABLE
  AS $$
  SELECT 10::int4
$$;
$DIST$);
\endif

CREATE OR REPLACE FUNCTION int8_now ()
  RETURNS int8
  LANGUAGE SQL
  STABLE
  AS $$
  SELECT 10::int8
$$;
\if :IS_DISTRIBUTED
CALL distributed_exec($DIST$
CREATE OR REPLACE FUNCTION int8_now ()
  RETURNS int8
  LANGUAGE SQL
  STABLE
  AS $$
  SELECT 10::int8
$$;
$DIST$);
\endif

SELECT set_integer_now_func (('metrics_' || dt)::regclass, (dt || '_now')::regproc)
FROM (
  VALUES ('int2'),
    ('int4'),
    ('int8')) v (dt);

-- width expression for int2 hypertables
CREATE MATERIALIZED VIEW width_expr WITH (timescaledb.continuous) AS
SELECT time_bucket(1::smallint, time)
FROM metrics_int2
GROUP BY 1;

DROP MATERIALIZED VIEW width_expr;

CREATE MATERIALIZED VIEW width_expr WITH (timescaledb.continuous) AS
SELECT time_bucket(1::smallint + 2::smallint, time)
FROM metrics_int2
GROUP BY 1;

DROP MATERIALIZED VIEW width_expr;

-- width expression for int4 hypertables
CREATE MATERIALIZED VIEW width_expr WITH (timescaledb.continuous) AS
SELECT time_bucket(1, time)
FROM metrics_int4
GROUP BY 1;

DROP MATERIALIZED VIEW width_expr;

CREATE MATERIALIZED VIEW width_expr WITH (timescaledb.continuous) AS
SELECT time_bucket(1 + 2, time)
FROM metrics_int4
GROUP BY 1;

DROP MATERIALIZED VIEW width_expr;

-- width expression for int8 hypertables
CREATE MATERIALIZED VIEW width_expr WITH (timescaledb.continuous) AS
SELECT time_bucket(1, time)
FROM metrics_int8
GROUP BY 1;

DROP MATERIALIZED VIEW width_expr;

CREATE MATERIALIZED VIEW width_expr WITH (timescaledb.continuous) AS
SELECT time_bucket(1 + 2, time)
FROM metrics_int8
GROUP BY 1;

DROP MATERIALIZED VIEW width_expr;

\set ON_ERROR_STOP 0
-- non-immutable expresions should be rejected
CREATE MATERIALIZED VIEW width_expr WITH (timescaledb.continuous) AS
SELECT time_bucket(extract(year FROM now())::smallint, time)
FROM metrics_int2
GROUP BY 1;

CREATE MATERIALIZED VIEW width_expr WITH (timescaledb.continuous) AS
SELECT time_bucket(extract(year FROM now())::int, time)
FROM metrics_int4
GROUP BY 1;

CREATE MATERIALIZED VIEW width_expr WITH (timescaledb.continuous) AS
SELECT time_bucket(extract(year FROM now())::int, time)
FROM metrics_int8
GROUP BY 1;
\set ON_ERROR_STOP 1

-- Test various ALTER MATERIALIZED VIEW statements.

SET ROLE :ROLE_DEFAULT_PERM_USER;

CREATE MATERIALIZED VIEW owner_check WITH (timescaledb.continuous) AS
SELECT time_bucket(1 + 2, time)
FROM metrics_int8
GROUP BY 1
WITH NO DATA;

\x on
SELECT * FROM cagg_info WHERE user_view::text = 'owner_check';
\x off

-- This should not work since the target user has the wrong role, but
-- we test that the normal checks are done when changing the owner.
\set ON_ERROR_STOP 0
ALTER MATERIALIZED VIEW owner_check OWNER TO :ROLE_1;
\set ON_ERROR_STOP 1

-- Superuser can always change owner
SET ROLE :ROLE_CLUSTER_SUPERUSER;
ALTER MATERIALIZED VIEW owner_check OWNER TO :ROLE_1;

\x on
SELECT * FROM cagg_info WHERE user_view::text = 'owner_check';
\x off

--
-- Test drop continuous aggregate cases
--
-- Issue: #2608
--
CREATE OR REPLACE FUNCTION test_int_now()
  RETURNS INT LANGUAGE SQL STABLE AS
$BODY$
  SELECT 50;
$BODY$;
\if :IS_DISTRIBUTED
CALL distributed_exec($DIST$
  CREATE OR REPLACE FUNCTION test_int_now()
    RETURNS INT LANGUAGE SQL STABLE AS
  $BODY$
    SELECT 50;
  $BODY$;
$DIST$);
\endif

CREATE TABLE conditionsnm(time_int INT NOT NULL, device INT, value FLOAT);
\if :IS_DISTRIBUTED
SELECT create_distributed_hypertable('conditionsnm', 'time_int', chunk_time_interval => 10, replication_factor => 2);
\else
SELECT create_hypertable('conditionsnm', 'time_int', chunk_time_interval => 10);
\endif
SELECT set_integer_now_func('conditionsnm', 'test_int_now');

INSERT INTO conditionsnm
SELECT time_val, time_val % 4, 3.14 FROM generate_series(0,100,1) AS time_val;

-- Case 1: DROP
CREATE MATERIALIZED VIEW conditionsnm_4
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE)
AS
SELECT time_bucket(7, time_int) as bucket,
SUM(value), COUNT(value)
FROM conditionsnm GROUP BY bucket WITH DATA;

DROP materialized view conditionsnm_4;

-- Case 2: DROP CASCADE should have similar behaviour as DROP
CREATE MATERIALIZED VIEW conditionsnm_4
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE)
AS
SELECT time_bucket(7, time_int) as bucket,
SUM(value), COUNT(value)
FROM conditionsnm GROUP BY bucket WITH DATA;

DROP materialized view conditionsnm_4 CASCADE;

-- Case 3: require CASCADE in case of dependent object
CREATE MATERIALIZED VIEW conditionsnm_4
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE)
AS
SELECT time_bucket(7, time_int) as bucket,
SUM(value), COUNT(value)
FROM conditionsnm GROUP BY bucket WITH DATA;

CREATE VIEW see_cagg as select * from conditionsnm_4;
\set ON_ERROR_STOP 0
DROP MATERIALIZED VIEW conditionsnm_4;
\set ON_ERROR_STOP 1

-- Case 4: DROP CASCADE with dependency
DROP MATERIALIZED VIEW conditionsnm_4 CASCADE;

-- Test DROP SCHEMA CASCADE with continuous aggregates
--
-- Issue: #2350
--

-- Case 1: DROP SCHEMA CASCADE
CREATE SCHEMA test_schema;

CREATE TABLE test_schema.telemetry_raw (
  ts        TIMESTAMP WITH TIME ZONE NOT NULL,
  value     DOUBLE PRECISION
);

\if :IS_DISTRIBUTED
SELECT create_distributed_hypertable('test_schema.telemetry_raw', 'ts', replication_factor => 2);
\else
SELECT create_hypertable('test_schema.telemetry_raw', 'ts');
\endif

CREATE MATERIALIZED VIEW test_schema.telemetry_1s
  WITH (timescaledb.continuous)
    AS
SELECT time_bucket(INTERVAL '1s', ts) AS ts_1s,
       avg(value)
  FROM test_schema.telemetry_raw
 GROUP BY ts_1s WITH NO DATA;

SELECT ca.raw_hypertable_id,
       h.schema_name,
       h.table_name AS "MAT_TABLE_NAME",
       partial_view_name as "PART_VIEW_NAME",
       partial_view_schema
FROM _timescaledb_catalog.continuous_agg ca
INNER JOIN _timescaledb_catalog.hypertable h ON (h.id = ca.mat_hypertable_id)
WHERE user_view_name = 'telemetry_1s';
\gset

DROP SCHEMA test_schema CASCADE;

SELECT count(*) FROM pg_class WHERE relname = :'MAT_TABLE_NAME';
SELECT count(*) FROM pg_class WHERE relname = :'PART_VIEW_NAME';
SELECT count(*) FROM pg_class WHERE relname = 'telemetry_1s';
SELECT count(*) FROM pg_namespace WHERE nspname = 'test_schema';

-- Case 2: DROP SCHEMA CASCADE with multiple caggs
CREATE SCHEMA test_schema;

CREATE TABLE test_schema.telemetry_raw (
  ts        TIMESTAMP WITH TIME ZONE NOT NULL,
  value     DOUBLE PRECISION
);

\if :IS_DISTRIBUTED
SELECT create_distributed_hypertable('test_schema.telemetry_raw', 'ts', replication_factor => 2);
\else
SELECT create_hypertable('test_schema.telemetry_raw', 'ts');
\endif

CREATE MATERIALIZED VIEW test_schema.cagg1
  WITH (timescaledb.continuous)
    AS
SELECT time_bucket(INTERVAL '1s', ts) AS ts_1s,
       avg(value)
  FROM test_schema.telemetry_raw
 GROUP BY ts_1s WITH NO DATA;

CREATE MATERIALIZED VIEW test_schema.cagg2
  WITH (timescaledb.continuous)
    AS
SELECT time_bucket(INTERVAL '1s', ts) AS ts_1s,
       avg(value)
  FROM test_schema.telemetry_raw
 GROUP BY ts_1s WITH NO DATA;

SELECT ca.raw_hypertable_id,
       h.schema_name,
       h.table_name AS "MAT_TABLE_NAME1",
       partial_view_name as "PART_VIEW_NAME1",
       partial_view_schema
FROM _timescaledb_catalog.continuous_agg ca
INNER JOIN _timescaledb_catalog.hypertable h ON (h.id = ca.mat_hypertable_id)
WHERE user_view_name = 'cagg1';
\gset

SELECT ca.raw_hypertable_id,
       h.schema_name,
       h.table_name AS "MAT_TABLE_NAME2",
       partial_view_name as "PART_VIEW_NAME2",
       partial_view_schema
FROM _timescaledb_catalog.continuous_agg ca
INNER JOIN _timescaledb_catalog.hypertable h ON (h.id = ca.mat_hypertable_id)
WHERE user_view_name = 'cagg2';
\gset

DROP SCHEMA test_schema CASCADE;

SELECT count(*) FROM pg_class WHERE relname = :'MAT_TABLE_NAME1';
SELECT count(*) FROM pg_class WHERE relname = :'PART_VIEW_NAME1';
SELECT count(*) FROM pg_class WHERE relname = 'cagg1';
SELECT count(*) FROM pg_class WHERE relname = :'MAT_TABLE_NAME2';
SELECT count(*) FROM pg_class WHERE relname = :'PART_VIEW_NAME2';
SELECT count(*) FROM pg_class WHERE relname = 'cagg2';
SELECT count(*) FROM pg_namespace WHERE nspname = 'test_schema';

DROP TABLESPACE tablespace1;
DROP TABLESPACE tablespace2;

-- Check that we can rename a column of a materialized view and still
-- rebuild it after (#3051, #3405)
CREATE TABLE conditions (
       time TIMESTAMPTZ NOT NULL,
       location TEXT NOT NULL,
       temperature DOUBLE PRECISION NULL
);

\if :IS_DISTRIBUTED
SELECT create_distributed_hypertable('conditions', 'time', replication_factor => 2);
\else
SELECT create_hypertable('conditions', 'time');
\endif

INSERT INTO conditions VALUES ( '2018-01-01 09:20:00-08', 'SFO', 55);
INSERT INTO conditions VALUES ( '2018-01-02 09:30:00-08', 'por', 100);
INSERT INTO conditions VALUES ( '2018-01-02 09:20:00-08', 'SFO', 65);
INSERT INTO conditions VALUES ( '2018-01-02 09:10:00-08', 'NYC', 65);
INSERT INTO conditions VALUES ( '2018-11-01 09:20:00-08', 'NYC', 45);
INSERT INTO conditions VALUES ( '2018-11-01 10:40:00-08', 'NYC', 55);
INSERT INTO conditions VALUES ( '2018-11-01 11:50:00-08', 'NYC', 65);
INSERT INTO conditions VALUES ( '2018-11-01 12:10:00-08', 'NYC', 75);
INSERT INTO conditions VALUES ( '2018-11-01 13:10:00-08', 'NYC', 85);
INSERT INTO conditions VALUES ( '2018-11-02 09:20:00-08', 'NYC', 10);
INSERT INTO conditions VALUES ( '2018-11-02 10:30:00-08', 'NYC', 20);

CREATE MATERIALIZED VIEW conditions_daily
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT location,
       time_bucket(INTERVAL '1 day', time) AS bucket,
       AVG(temperature)
  FROM conditions
GROUP BY location, bucket
WITH NO DATA;

SELECT format('%I.%I', '_timescaledb_internal', h.table_name) AS "MAT_TABLE_NAME",
       format('%I.%I', '_timescaledb_internal', partial_view_name) AS "PART_VIEW_NAME",
       format('%I.%I', '_timescaledb_internal', direct_view_name) AS "DIRECT_VIEW_NAME"
FROM _timescaledb_catalog.continuous_agg ca
INNER JOIN _timescaledb_catalog.hypertable h ON (h.id = ca.mat_hypertable_id)
WHERE user_view_name = 'conditions_daily'
\gset

-- Show both the columns and the view definitions to see that
-- references are correct in the view as well.
SELECT * FROM test.show_columns('conditions_daily');
SELECT * FROM test.show_columns(:'DIRECT_VIEW_NAME');
SELECT * FROM test.show_columns(:'PART_VIEW_NAME');
SELECT * FROM test.show_columns(:'MAT_TABLE_NAME');

ALTER MATERIALIZED VIEW conditions_daily RENAME COLUMN bucket to "time";

-- Show both the columns and the view definitions to see that
-- references are correct in the view as well.
SELECT * FROM test.show_columns(' conditions_daily');
SELECT * FROM test.show_columns(:'DIRECT_VIEW_NAME');
SELECT * FROM test.show_columns(:'PART_VIEW_NAME');
SELECT * FROM test.show_columns(:'MAT_TABLE_NAME');

-- This will rebuild the materialized view and should succeed.
ALTER MATERIALIZED VIEW conditions_daily SET (timescaledb.materialized_only = false);

-- Refresh the continuous aggregate to check that it works after the
-- rename.
\set VERBOSITY verbose
CALL refresh_continuous_aggregate('conditions_daily', NULL, NULL);
\set VERBOSITY terse

--
-- Indexes on continuous aggregate
--
\set ON_ERROR_STOP 0
-- unique indexes are not supported
CREATE UNIQUE INDEX index_unique_error ON conditions_daily ("time", location);
-- concurrently index creation not supported
CREATE INDEX CONCURRENTLY index_concurrently_avg ON conditions_daily (avg);
\set ON_ERROR_STOP 1

CREATE INDEX index_avg ON conditions_daily (avg);
CREATE INDEX index_avg_only ON ONLY conditions_daily (avg);
CREATE INDEX index_avg_include ON conditions_daily (avg) INCLUDE (location);
CREATE INDEX index_avg_expr ON conditions_daily ((avg + 1));
CREATE INDEX index_avg_location_sfo ON conditions_daily (avg) WHERE location = 'SFO';
CREATE INDEX index_avg_expr_location_sfo ON conditions_daily ((avg + 2)) WHERE location = 'SFO';
SELECT * FROM test.show_indexespred(:'MAT_TABLE_NAME');

-- #3696 assertion failure when referencing columns not present in result
CREATE TABLE i3696(time timestamptz NOT NULL, search_query text, cnt integer, cnt2 integer);
\if :IS_DISTRIBUTED
SELECT create_distributed_hypertable('i3696', 'time', replication_factor => 2);
\else
SELECT table_name FROM create_hypertable('i3696','time');
\endif

CREATE MATERIALIZED VIEW i3696_cagg1 WITH (timescaledb.continuous)
AS
 SELECT  search_query,count(search_query) as count, sum(cnt), time_bucket(INTERVAL '1 minute', time) AS bucket
 FROM i3696 GROUP BY cnt +cnt2 , bucket, search_query;

ALTER MATERIALIZED VIEW i3696_cagg1 SET (timescaledb.materialized_only = 'true');

CREATE MATERIALIZED VIEW i3696_cagg2 WITH (timescaledb.continuous)
AS
 SELECT  search_query,count(search_query) as count, sum(cnt), time_bucket(INTERVAL '1 minute', time) AS bucket
 FROM i3696 GROUP BY cnt + cnt2, bucket, search_query
 HAVING cnt + cnt2 + sum(cnt) > 2 or count(cnt2) > 10;

ALTER MATERIALIZED VIEW i3696_cagg2 SET (timescaledb.materialized_only = 'true');

--TEST test with multiple settings on continuous aggregates --
-- test for materialized_only + compress combinations (real time aggs enabled initially)
CREATE TABLE test_setting(time timestamptz not null, val numeric);
\if :IS_DISTRIBUTED
SELECT create_distributed_hypertable('test_setting', 'time', replication_factor => 2);
\else
SELECT create_hypertable('test_setting', 'time');
\endif

CREATE MATERIALIZED VIEW test_setting_cagg with (timescaledb.continuous)
AS SELECT time_bucket('1h',time), avg(val), count(*) FROM test_setting GROUP BY 1;

INSERT INTO test_setting
SELECT generate_series( '2020-01-10 8:00'::timestamp, '2020-01-30 10:00+00'::timestamptz, '1 day'::interval), 10.0;
CALL refresh_continuous_aggregate('test_setting_cagg', NULL, '2020-05-30 10:00+00'::timestamptz);
SELECT count(*) from test_setting_cagg ORDER BY 1;

--this row is not in the materialized result ---
INSERT INTO test_setting VALUES( '2020-11-01', 20);

--try out 2 settings here --
ALTER MATERIALIZED VIEW test_setting_cagg SET (timescaledb.materialized_only = 'true', timescaledb.compress='true');
SELECT view_name, compression_enabled, materialized_only
FROM timescaledb_information.continuous_aggregates
where view_name = 'test_setting_cagg';
--real time aggs is off now , should return 20 --
SELECT count(*) from test_setting_cagg ORDER BY 1;

--now set it back to false --
ALTER MATERIALIZED VIEW test_setting_cagg SET (timescaledb.materialized_only = 'false', timescaledb.compress='true');
SELECT view_name, compression_enabled, materialized_only
FROM timescaledb_information.continuous_aggregates
where view_name = 'test_setting_cagg';
--count should return additional data since we have real time aggs on
SELECT count(*) from test_setting_cagg ORDER BY 1;

ALTER MATERIALIZED VIEW test_setting_cagg SET (timescaledb.materialized_only = 'true', timescaledb.compress='false');
SELECT view_name, compression_enabled, materialized_only
FROM timescaledb_information.continuous_aggregates
where view_name = 'test_setting_cagg';
--real time aggs is off now , should return 20 --
SELECT count(*) from test_setting_cagg ORDER BY 1;

ALTER MATERIALIZED VIEW test_setting_cagg SET (timescaledb.materialized_only = 'false', timescaledb.compress='false');
SELECT view_name, compression_enabled, materialized_only
FROM timescaledb_information.continuous_aggregates
where view_name = 'test_setting_cagg';
--count should return additional data since we have real time aggs on
SELECT count(*) from test_setting_cagg ORDER BY 1;

DELETE FROM test_setting WHERE val = 20;

--TEST test with multiple settings on continuous aggregates with real time aggregates turned off initially --
-- test for materialized_only + compress combinations (real time aggs enabled initially)
DROP MATERIALIZED VIEW test_setting_cagg;

CREATE MATERIALIZED VIEW test_setting_cagg with (timescaledb.continuous, timescaledb.materialized_only = true)
AS SELECT time_bucket('1h',time), avg(val), count(*) FROM test_setting GROUP BY 1;
CALL refresh_continuous_aggregate('test_setting_cagg', NULL, '2020-05-30 10:00+00'::timestamptz);
SELECT count(*) from test_setting_cagg ORDER BY 1;

--this row is not in the materialized result ---
INSERT INTO test_setting VALUES( '2020-11-01', 20);

--try out 2 settings here --
ALTER MATERIALIZED VIEW test_setting_cagg SET (timescaledb.materialized_only = 'false', timescaledb.compress='true');
SELECT view_name, compression_enabled, materialized_only
FROM timescaledb_information.continuous_aggregates
where view_name = 'test_setting_cagg';
--count should return additional data since we have real time aggs on
SELECT count(*) from test_setting_cagg ORDER BY 1;

--now set it back to false --
ALTER MATERIALIZED VIEW test_setting_cagg SET (timescaledb.materialized_only = 'true', timescaledb.compress='true');
SELECT view_name, compression_enabled, materialized_only
FROM timescaledb_information.continuous_aggregates
where view_name = 'test_setting_cagg';
--real time aggs is off now , should return 20 --
SELECT count(*) from test_setting_cagg ORDER BY 1;

ALTER MATERIALIZED VIEW test_setting_cagg SET (timescaledb.materialized_only = 'false', timescaledb.compress='false');
SELECT view_name, compression_enabled, materialized_only
FROM timescaledb_information.continuous_aggregates
where view_name = 'test_setting_cagg';
--count should return additional data since we have real time aggs on
SELECT count(*) from test_setting_cagg ORDER BY 1;

ALTER MATERIALIZED VIEW test_setting_cagg SET (timescaledb.materialized_only = 'true', timescaledb.compress='false');
SELECT view_name, compression_enabled, materialized_only
FROM timescaledb_information.continuous_aggregates
where view_name = 'test_setting_cagg';
--real time aggs is off now , should return 20 --
SELECT count(*) from test_setting_cagg ORDER BY 1;

-- END TEST with multiple settings

-- Test View Target Entries that contain both aggrefs and Vars in the same expression
CREATE TABLE transactions
(
    "time" timestamp with time zone NOT NULL,
    dummy1 integer,
    dummy2 integer,
    dummy3 integer,
    dummy4 integer,
    dummy5 integer,
    amount integer,
    fiat_value integer
);

\if :IS_DISTRIBUTED
SELECT create_distributed_hypertable('transactions', 'time', replication_factor => 2);
\else
SELECT create_hypertable('transactions', 'time');
\endif

INSERT INTO transactions VALUES ( '2018-01-01 09:20:00-08', 0, 0, 0, 0, 0, 1, 10);

INSERT INTO transactions VALUES ( '2018-01-02 09:30:00-08', 0, 0, 0, 0, 0, -1, 10);
INSERT INTO transactions VALUES ( '2018-01-02 09:20:00-08', 0, 0, 0, 0, 0, -1, 10);
INSERT INTO transactions VALUES ( '2018-01-02 09:10:00-08', 0, 0, 0, 0, 0, -1, 10);

INSERT INTO transactions VALUES ( '2018-11-01 09:20:00-08', 0, 0, 0, 0, 0, 1, 10);
INSERT INTO transactions VALUES ( '2018-11-01 10:40:00-08', 0, 0, 0, 0, 0, 1, 10);
INSERT INTO transactions VALUES ( '2018-11-01 11:50:00-08', 0, 0, 0, 0, 0, 1, 10);
INSERT INTO transactions VALUES ( '2018-11-01 12:10:00-08', 0, 0, 0, 0, 0, -1, 10);
INSERT INTO transactions VALUES ( '2018-11-01 13:10:00-08', 0, 0, 0, 0, 0, -1, 10);

INSERT INTO transactions VALUES ( '2018-11-02 09:20:00-08', 0, 0, 0, 0, 0, 1, 10);
INSERT INTO transactions VALUES ( '2018-11-02 10:30:00-08', 0, 0, 0, 0, 0, -1, 10);

CREATE materialized view cashflows(
    bucket,
  	amount,
    cashflow,
    cashflow2
) WITH (
    timescaledb.continuous,
    timescaledb.materialized_only = true
) AS
SELECT time_bucket ('1 day', time) AS bucket,
	amount,
  CASE
      WHEN amount < 0 THEN (0 - sum(fiat_value))
      ELSE sum(fiat_value)
  END AS cashflow,
  amount + sum(fiat_value)
FROM transactions
GROUP BY bucket, amount;

SELECT h.table_name AS "MAT_TABLE_NAME",
       partial_view_name AS "PART_VIEW_NAME",
       direct_view_name AS "DIRECT_VIEW_NAME"
FROM _timescaledb_catalog.continuous_agg ca
INNER JOIN _timescaledb_catalog.hypertable h ON (h.id = ca.mat_hypertable_id)
WHERE user_view_name = 'cashflows'
\gset

-- Show both the columns and the view definitions to see that
-- references are correct in the view as well.
\d+ "_timescaledb_internal".:"DIRECT_VIEW_NAME"
\d+ "_timescaledb_internal".:"PART_VIEW_NAME"
\d+ "_timescaledb_internal".:"MAT_TABLE_NAME"
\d+ 'cashflows'

SELECT * FROM cashflows;
