-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set EXPLAIN_ANALYZE 'EXPLAIN (analyze,costs off,timing off,summary off)'

CREATE TABLE continuous_agg_test(time int, data int);
SELECT create_hypertable('continuous_agg_test', 'time', chunk_time_interval=> 10);
CREATE OR REPLACE FUNCTION integer_now_test1() returns int LANGUAGE SQL STABLE as $$ SELECT coalesce(max(time), 0) FROM continuous_agg_test $$;
SELECT set_integer_now_func('continuous_agg_test', 'integer_now_test1');

-- watermark tabels start out empty
SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;
SELECT * from _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;

-- inserting into a table that does not have continuous_agg_insert_trigger doesn't change the watermark
INSERT INTO continuous_agg_test VALUES (10, 1), (11, 2), (21, 3), (22, 4);

SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;
SELECT * from _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE TABLE continuous_agg_test_mat(time int);
SELECT create_hypertable('continuous_agg_test_mat', 'time', chunk_time_interval=> 10);
INSERT INTO _timescaledb_catalog.continuous_agg VALUES (2, 1, NULL, '', '', '', '', '', '');
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

-- create the trigger
CREATE TRIGGER continuous_agg_insert_trigger
    AFTER INSERT ON continuous_agg_test
    FOR EACH ROW EXECUTE FUNCTION _timescaledb_functions.continuous_agg_invalidation_trigger(1);

-- inserting into the table still doesn't change the watermark since there's no
-- continuous_aggs_invalidation_threshold. We treat that case as a invalidation_watermark of
-- BIG_INT_MIN, since the first run of the aggregation will need to scan the
-- entire table anyway.
INSERT INTO continuous_agg_test VALUES (10, 1), (11, 2), (21, 3), (22, 4);

SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;
SELECT * from _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;

-- set the continuous_aggs_invalidation_threshold to 15, any insertions below that value need an invalidation
\c :TEST_DBNAME :ROLE_SUPERUSER
INSERT INTO _timescaledb_catalog.continuous_aggs_invalidation_threshold VALUES (1, 15);
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

INSERT INTO continuous_agg_test VALUES (10, 1), (11, 2), (21, 3), (22, 4);

SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;
SELECT * from _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;

-- INSERTs only above the continuous_aggs_invalidation_threshold won't change the continuous_aggs_hypertable_invalidation_log
INSERT INTO continuous_agg_test VALUES (21, 3), (22, 4);

SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;
SELECT * from _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;

-- INSERTs only below the continuous_aggs_invalidation_threshold will change the continuous_aggs_hypertable_invalidation_log
INSERT INTO continuous_agg_test VALUES (10, 1), (11, 2);

SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;
SELECT * from _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;

-- test INSERTing other values
INSERT INTO continuous_agg_test VALUES (1, 7), (12, 6), (24, 5), (51, 4);

SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;
SELECT * from _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;

-- INSERT after dropping a COLUMN
ALTER TABLE continuous_agg_test DROP COLUMN data;

INSERT INTO continuous_agg_test VALUES (-1), (-2), (-3), (-4);

SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;
SELECT * from _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;

INSERT INTO continuous_agg_test VALUES (100);

SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;
SELECT * from _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;

-- INSERT after adding a COLUMN
ALTER TABLE continuous_agg_test ADD COLUMN d BOOLEAN;

INSERT INTO continuous_agg_test VALUES (-6, true), (-7, false), (-3, true), (-4, false);

SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;
SELECT * from _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;

INSERT INTO continuous_agg_test VALUES (120, false), (200, true);

SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;
SELECT * from _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;

\c :TEST_DBNAME :ROLE_SUPERUSER
DELETE FROM _timescaledb_catalog.continuous_agg where mat_hypertable_id =  2;
DELETE FROM _timescaledb_config.bgw_job WHERE id = 2;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

DROP TABLE continuous_agg_test CASCADE;
\c :TEST_DBNAME :ROLE_SUPERUSER
TRUNCATE _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;
TRUNCATE _timescaledb_catalog.continuous_aggs_invalidation_threshold;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

-- CREATE VIEW creates the invalidation trigger correctly
CREATE TABLE ca_inval_test(time int);
SELECT create_hypertable('ca_inval_test', 'time', chunk_time_interval=> 10);
CREATE OR REPLACE FUNCTION integer_now_test2() returns int LANGUAGE SQL STABLE as $$ SELECT coalesce(max(time), 0) FROM ca_inval_test $$;
SELECT set_integer_now_func('ca_inval_test', 'integer_now_test2');

CREATE MATERIALIZED VIEW cit_view
    WITH (timescaledb.continuous, timescaledb.materialized_only=false)
    AS SELECT time_bucket('5', time), COUNT(time)
        FROM ca_inval_test
        GROUP BY 1 WITH NO DATA;

INSERT INTO ca_inval_test SELECT generate_series(0, 5);

SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;
SELECT * from _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;

\c :TEST_DBNAME :ROLE_SUPERUSER
UPDATE _timescaledb_catalog.continuous_aggs_invalidation_threshold
SET watermark = 15
WHERE hypertable_id = 3;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

INSERT INTO ca_inval_test SELECT generate_series(5, 15);

SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;
SELECT * from _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;

INSERT INTO ca_inval_test SELECT generate_series(16, 20);

SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;
SELECT * from _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;

\c :TEST_DBNAME :ROLE_SUPERUSER
TRUNCATE _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

-- updates below the threshold update both the old and new values
UPDATE ca_inval_test SET time = 5 WHERE time = 6;
UPDATE ca_inval_test SET time = 7 WHERE time = 5;
UPDATE ca_inval_test SET time = 17 WHERE time = 14;
UPDATE ca_inval_test SET time = 12 WHERE time = 16;

-- updates purely above the threshold are not logged
UPDATE ca_inval_test SET time = 19 WHERE time = 18;
UPDATE ca_inval_test SET time = 17 WHERE time = 19;

SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;
SELECT * from _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;

DROP TABLE ca_inval_test CASCADE;
\c :TEST_DBNAME :ROLE_SUPERUSER
TRUNCATE _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;
TRUNCATE _timescaledb_catalog.continuous_aggs_invalidation_threshold;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

-- invalidation trigger is created correctly on chunks that existed before
-- the view was created
CREATE TABLE ts_continuous_test(time INTEGER, location INTEGER);
    SELECT create_hypertable('ts_continuous_test', 'time', chunk_time_interval => 10);
CREATE OR REPLACE FUNCTION integer_now_test3() returns int LANGUAGE SQL STABLE as $$ SELECT coalesce(max(time), 0) FROM ts_continuous_test $$;
SELECT set_integer_now_func('ts_continuous_test', 'integer_now_test3');
INSERT INTO ts_continuous_test SELECT i, i FROM
    (SELECT generate_series(0, 29) AS i) AS i;
CREATE MATERIALIZED VIEW continuous_view
    WITH (timescaledb.continuous, timescaledb.materialized_only=false)
    AS SELECT time_bucket('5', time), COUNT(location)
        FROM ts_continuous_test
        GROUP BY 1 WITH NO DATA;

SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;
SELECT * from _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;

\c :TEST_DBNAME :ROLE_SUPERUSER
UPDATE _timescaledb_catalog.continuous_aggs_invalidation_threshold
SET watermark = 2
WHERE hypertable_id = 5;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

INSERT INTO ts_continuous_test VALUES (1, 1);

SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;
SELECT * from _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;

-- aborts don't get written
BEGIN;
    INSERT INTO ts_continuous_test VALUES (-20, -20);
ABORT;

SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;
SELECT * from _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;

DROP TABLE ts_continuous_test CASCADE;

----
-- Test watermark invalidation and chunk exclusion with prepared and ad-hoc queries
----
CREATE TABLE chunks(time timestamptz, device int, value float);
SELECT FROM create_hypertable('chunks','time',chunk_time_interval:='1d'::interval);

CREATE MATERIALIZED VIEW chunks_1h WITH (timescaledb.continuous, timescaledb.materialized_only = false)
    AS SELECT time_bucket('1 hour', time) AS bucket, device, max(value) AS max FROM chunks GROUP BY 1, 2;

-- Get id of the materialization hypertable
SELECT id AS "MAT_HT_ID_1H" FROM _timescaledb_catalog.hypertable
    WHERE table_name=(
        SELECT materialization_hypertable_name
            FROM timescaledb_information.continuous_aggregates
            WHERE view_name='chunks_1h'
    ) \gset


SELECT materialization_hypertable_schema || '.' || materialization_hypertable_name AS "MAT_HT_NAME_1H"
    FROM timescaledb_information.continuous_aggregates
    WHERE view_name='chunks_1h'
\gset

-- Prepared scan on hypertable (identical to the query of a real-time CAgg)
PREPARE ht_scan_realtime_1h AS
   SELECT bucket, device, max
   FROM :MAT_HT_NAME_1H
  WHERE bucket < COALESCE(_timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(:MAT_HT_ID_1H)), '-infinity'::timestamp with time zone)
UNION ALL
 SELECT time_bucket('01:00:00'::interval, chunks."time") AS bucket,
    chunks.device,
    max(chunks.value) AS max
   FROM chunks
  WHERE chunks."time" >= COALESCE(_timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(:MAT_HT_ID_1H)), '-infinity'::timestamp with time zone)
  GROUP BY (time_bucket('01:00:00'::interval, chunks."time")), chunks.device;

PREPARE cagg_scan_1h AS SELECT * FROM chunks_1h;

:EXPLAIN_ANALYZE EXECUTE ht_scan_realtime_1h;

INSERT INTO chunks VALUES ('1901-08-01 01:01:01+01', 1, 2);
CALL refresh_continuous_aggregate('chunks_1h', '1900-01-01', '2021-06-01');
SELECT * FROM _timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(:MAT_HT_ID_1H));
:EXPLAIN_ANALYZE EXECUTE ht_scan_realtime_1h;

-- Compare prepared statement with ad-hoc query
EXECUTE cagg_scan_1h;
SELECT * FROM chunks_1h;

-- Add new chunks to the non materialized part of the CAgg
INSERT INTO chunks VALUES ('1910-08-01 01:01:01+01', 1, 2);
:EXPLAIN_ANALYZE EXECUTE cagg_scan_1h;
:EXPLAIN_ANALYZE SELECT * FROM chunks_1h;

INSERT INTO chunks VALUES ('1911-08-01 01:01:01+01', 1, 2);
:EXPLAIN_ANALYZE EXECUTE cagg_scan_1h;
:EXPLAIN_ANALYZE SELECT * FROM chunks_1h;

-- Materialize CAgg and check for plan time chunk exclusion
CALL refresh_continuous_aggregate('chunks_1h', '1900-01-01', '2021-06-01');
:EXPLAIN_ANALYZE EXECUTE cagg_scan_1h;
:EXPLAIN_ANALYZE SELECT * FROM chunks_1h;

-- Check plan when chunk_append and constraint_aware_append cannot be used
-- There should be no plans for scans of chunks that are materialized in the CAgg
-- on the underlying hypertable
SET timescaledb.enable_chunk_append = OFF;
SET timescaledb.enable_constraint_aware_append = OFF;
:EXPLAIN_ANALYZE SELECT * FROM chunks_1h;
RESET timescaledb.enable_chunk_append;
RESET timescaledb.enable_constraint_aware_append;

-- Insert new values and check watermark changes
INSERT INTO chunks VALUES ('1920-08-01 01:01:01+01', 1, 2);
CALL refresh_continuous_aggregate('chunks_1h', '1900-01-01', '2021-06-01');
SELECT * FROM _timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(:MAT_HT_ID_1H));
:EXPLAIN_ANALYZE EXECUTE ht_scan_realtime_1h;

-- Compare prepared statement with ad-hoc query
EXECUTE cagg_scan_1h;
SELECT * FROM chunks_1h;

INSERT INTO chunks VALUES ('1930-08-01 01:01:01+01', 1, 2);
CALL refresh_continuous_aggregate('chunks_1h', '1900-01-01', '2021-06-01');
SELECT * FROM _timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(:MAT_HT_ID_1H));
:EXPLAIN_ANALYZE EXECUTE ht_scan_realtime_1h;

-- Two invalidations without prepared statement execution between
INSERT INTO chunks VALUES ('1931-08-01 01:01:01+01', 1, 2);
CALL refresh_continuous_aggregate('chunks_1h', '1900-01-01', '2021-06-01');
INSERT INTO chunks VALUES ('1932-08-01 01:01:01+01', 1, 2);
CALL refresh_continuous_aggregate('chunks_1h', '1900-01-01', '2021-06-01');
SELECT * FROM _timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(:MAT_HT_ID_1H));
:EXPLAIN_ANALYZE EXECUTE ht_scan_realtime_1h;

-- Multiple prepared statement executions followed by one invalidation
:EXPLAIN_ANALYZE EXECUTE ht_scan_realtime_1h;
:EXPLAIN_ANALYZE EXECUTE ht_scan_realtime_1h;
INSERT INTO chunks VALUES ('1940-08-01 01:01:01+01', 1, 2);
CALL refresh_continuous_aggregate('chunks_1h', '1900-01-01', '2021-06-01');
:EXPLAIN_ANALYZE EXECUTE ht_scan_realtime_1h;

-- Compare prepared statement with ad-hoc query
EXECUTE cagg_scan_1h;
SELECT * FROM chunks_1h;

-- Delete data from hypertable - data is only present in cagg after this point. If the watermark in the prepared
-- statement is not moved to the most-recent watermark, we would see an empty result.
TRUNCATE chunks;

EXECUTE cagg_scan_1h;
SELECT * FROM chunks_1h;

-- Refresh the CAgg
CALL refresh_continuous_aggregate('chunks_1h', NULL, NULL);
EXECUTE cagg_scan_1h;
SELECT * FROM chunks_1h;

-- Check new watermark
SELECT * FROM _timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(:MAT_HT_ID_1H));
:EXPLAIN_ANALYZE EXECUTE ht_scan_realtime_1h;

-- Update after truncate
INSERT INTO chunks VALUES ('1950-08-01 01:01:01+01', 1, 2);
CALL refresh_continuous_aggregate('chunks_1h', '1900-01-01', '2021-06-01');
SELECT * FROM _timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(:MAT_HT_ID_1H));
:EXPLAIN_ANALYZE EXECUTE ht_scan_realtime_1h;

-- Test with CAgg on CAgg
CREATE MATERIALIZED VIEW chunks_1d WITH (timescaledb.continuous, timescaledb.materialized_only = false)
    AS SELECT time_bucket('1 days', bucket) AS bucket, device, max(max) AS max FROM chunks_1h GROUP BY 1, 2;

SELECT id AS "MAT_HT_ID_1D" FROM _timescaledb_catalog.hypertable
    WHERE table_name=(
        SELECT materialization_hypertable_name
            FROM timescaledb_information.continuous_aggregates
            WHERE view_name='chunks_1d'
    ) \gset


SELECT materialization_hypertable_schema || '.' || materialization_hypertable_name AS "MAT_HT_NAME_1D"
    FROM timescaledb_information.continuous_aggregates
    WHERE view_name='chunks_1d'
\gset

-- Prepared scan on hypertable (identical to the query of a real-time CAgg)
PREPARE ht_scan_realtime_1d AS
 SELECT bucket, device, max
   FROM :MAT_HT_NAME_1D
  WHERE bucket < COALESCE(_timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(:MAT_HT_ID_1D)), '-infinity'::timestamp with time zone)
UNION ALL
 SELECT time_bucket('@ 1 day'::interval, chunks_1h.bucket) AS bucket,
    chunks_1h.device,
    max(chunks_1h.max) AS max
   FROM chunks_1h
  WHERE chunks_1h.bucket >= COALESCE(_timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(:MAT_HT_ID_1D)), '-infinity'::timestamp with time zone)
  GROUP BY (time_bucket('@ 1 day'::interval, chunks_1h.bucket)), chunks_1h.device;


PREPARE cagg_scan_1d AS SELECT * FROM chunks_1d;

:EXPLAIN_ANALYZE EXECUTE ht_scan_realtime_1d;

INSERT INTO chunks VALUES ('2000-08-01 01:01:01+01', 1, 2);
CALL refresh_continuous_aggregate('chunks_1h', '1900-01-01', '2021-06-01');
CALL refresh_continuous_aggregate('chunks_1d', '1900-01-01', '2021-06-01');
:EXPLAIN_ANALYZE EXECUTE ht_scan_realtime_1d;

INSERT INTO chunks VALUES ('2010-08-01 01:01:01+01', 1, 2);
CALL refresh_continuous_aggregate('chunks_1h', '1900-01-01', '2021-06-01');
CALL refresh_continuous_aggregate('chunks_1d', '1900-01-01', '2021-06-01');
:EXPLAIN_ANALYZE EXECUTE ht_scan_realtime_1d;

-- Stored procedure - watermark
CREATE FUNCTION cur_watermark_plsql(mat_table int) RETURNS timestamptz
AS $$
DECLARE
cur_watermark_value timestamptz;
BEGIN
    SELECT _timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(mat_table)) INTO cur_watermark_value;
    RETURN cur_watermark_value;
END$$ LANGUAGE plpgsql;

SELECT * FROM cur_watermark_plsql(:MAT_HT_ID_1H);

INSERT INTO chunks VALUES ('2011-08-01 01:01:01+01', 1, 2);
CALL refresh_continuous_aggregate('chunks_1h', '1900-01-01', '2021-06-01');
SELECT * FROM cur_watermark_plsql(:MAT_HT_ID_1H);

INSERT INTO chunks VALUES ('2012-08-01 01:01:01+01', 1, 2);
CALL refresh_continuous_aggregate('chunks_1h', '1900-01-01', '2021-06-01');
SELECT * FROM cur_watermark_plsql(:MAT_HT_ID_1H);

-- Stored procedure - result
CREATE FUNCTION cur_cagg_result_count() RETURNS int
AS $$
DECLARE
count_value int;
BEGIN
    SELECT count(*) FROM chunks_1h INTO count_value;
    RETURN count_value;
END$$ LANGUAGE plpgsql;

-- Cache function value
SELECT * FROM cur_cagg_result_count();

-- Add to non-materialized part
INSERT INTO chunks VALUES ('2013-08-01 01:01:01+01', 1, 2);
SELECT * FROM cur_cagg_result_count();

-- Materialize
CALL refresh_continuous_aggregate('chunks_1h', '1900-01-01', '2021-06-01');
SELECT * FROM cur_cagg_result_count();

-- Ensure all elements are materialized (i.e., watermark is moved properly)
TRUNCATE chunks;
SELECT * FROM cur_cagg_result_count();
SELECT count(*) FROM chunks_1h;

-- Test watermark call directly
PREPARE watermark_query AS
    SELECT _timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(:MAT_HT_ID_1H));

SELECT _timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(:MAT_HT_ID_1H));
EXECUTE watermark_query;

INSERT INTO chunks VALUES ('2013-09-01 01:01:01+01', 1, 2);
CALL refresh_continuous_aggregate('chunks_1h', '1900-01-01', '2021-06-01');

SELECT _timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(:MAT_HT_ID_1H));
EXECUTE watermark_query;

-- Disable constification of watermark values
SET timescaledb.enable_cagg_watermark_constify = OFF;
INSERT INTO chunks VALUES ('2014-01-01 01:01:01+01', 1, 2);
CALL refresh_continuous_aggregate('chunks_1h', '1900-01-01', '2021-06-01');
:EXPLAIN_ANALYZE EXECUTE ht_scan_realtime_1h;
RESET timescaledb.enable_cagg_watermark_constify;

-- Select with projection
INSERT INTO chunks VALUES ('2015-01-01 01:01:01+01', 1, 2);
CALL refresh_continuous_aggregate('chunks_1h', '1900-01-01', '2021-06-01');
:EXPLAIN_ANALYZE SELECT device FROM chunks_1h;

-- Watermark function use other tables in WHERE condition (should not be constified)
CREATE TABLE continuous_agg_test(time int, data int);
:EXPLAIN_ANALYZE (SELECT * FROM continuous_agg_test AS t1) UNION ALL (SELECT * from continuous_agg_test AS t2 WHERE COALESCE(_timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(:MAT_HT_ID_1H)), '-infinity'::timestamp with time zone) IS NOT NULL);

-- Query without COALESCE - should not be optimized
:EXPLAIN_ANALYZE (SELECT * FROM chunks_1h AS t1) UNION ALL (SELECT * from chunks_1h AS t2 WHERE _timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(:MAT_HT_ID_1H)) IS NOT NULL);

-- Aggregation query over CAgg should be constified
:EXPLAIN_ANALYZE SELECT max(device) from chunks_1h;

-- Test with integer partitioning
CREATE TABLE integer_ht(time int, data int);
SELECT create_hypertable('integer_ht', 'time', chunk_time_interval => 10);
CREATE FUNCTION integer_now_integer_ht() RETURNS INTEGER LANGUAGE SQL STABLE AS $$ SELECT coalesce(max(time), 0) FROM integer_ht $$;

SELECT set_integer_now_func('integer_ht', 'integer_now_integer_ht');
INSERT INTO integer_ht SELECT i, i FROM generate_series(0, 25) AS i;

CREATE MATERIALIZED VIEW integer_ht_cagg
    WITH (timescaledb.continuous, timescaledb.materialized_only=false)
    AS SELECT time_bucket('5', time), COUNT(time)
        FROM integer_ht
        GROUP BY 1;

SELECT * FROM integer_ht_cagg;
:EXPLAIN_ANALYZE SELECT * FROM integer_ht_cagg;

-- Test with big integer partitioning
CREATE TABLE big_integer_ht(time bigint, data bigint);
SELECT create_hypertable('big_integer_ht', 'time', chunk_time_interval => 10);
CREATE FUNCTION integer_now_big_integer_ht() RETURNS BIGINT LANGUAGE SQL STABLE AS $$ SELECT coalesce(max(time), 0) FROM big_integer_ht $$;

SELECT set_integer_now_func('big_integer_ht', 'integer_now_big_integer_ht');
INSERT INTO big_integer_ht SELECT i, i FROM generate_series(0, 25) AS i;

CREATE MATERIALIZED VIEW big_integer_ht_cagg
    WITH (timescaledb.continuous, timescaledb.materialized_only=false)
    AS SELECT time_bucket('5', time), COUNT(time)
        FROM big_integer_ht
        GROUP BY 1;

SELECT * FROM big_integer_ht_cagg;
:EXPLAIN_ANALYZE SELECT * FROM big_integer_ht_cagg;

-- Test with small integer partitioning
CREATE TABLE small_integer_ht(time bigint, data bigint);
SELECT create_hypertable('small_integer_ht', 'time', chunk_time_interval => 10);
CREATE FUNCTION integer_now_small_integer_ht() RETURNS BIGINT LANGUAGE SQL STABLE AS $$ SELECT coalesce(max(time), 0) FROM small_integer_ht $$;

SELECT set_integer_now_func('small_integer_ht', 'integer_now_small_integer_ht');
INSERT INTO small_integer_ht SELECT i, i FROM generate_series(0, 25) AS i;

CREATE MATERIALIZED VIEW small_integer_ht_cagg
    WITH (timescaledb.continuous, timescaledb.materialized_only=false)
    AS SELECT time_bucket('5', time), COUNT(time)
        FROM small_integer_ht
        GROUP BY 1;

SELECT * FROM small_integer_ht_cagg;
:EXPLAIN_ANALYZE SELECT * FROM small_integer_ht_cagg;

-- Test handling of multiple watermark functions on integer based hypertables
-- This is not a usual CAgg query. So, no constification should be done. However,
-- the constification code should detect this and do nothing.
SELECT id AS "MAT_HT_ID_SMALL_INTEGER" FROM _timescaledb_catalog.hypertable
    WHERE table_name=(
        SELECT materialization_hypertable_name
            FROM timescaledb_information.continuous_aggregates
            WHERE view_name='small_integer_ht_cagg'
    ) \gset

:EXPLAIN_ANALYZE SELECT time_bucket(5, time) AS time_bucket,
    count(time) AS count
   FROM small_integer_ht
  WHERE small_integer_ht."time" >= COALESCE(_timescaledb_functions.cagg_watermark(:MAT_HT_ID_SMALL_INTEGER)::integer, _timescaledb_functions.cagg_watermark(:MAT_HT_ID_SMALL_INTEGER)::integer)
  GROUP BY (time_bucket(5, small_integer_ht."time"))
UNION ALL
  SELECT time_bucket(5, time) AS time_bucket,
    count(time) AS count
   FROM small_integer_ht
  WHERE small_integer_ht."time" < COALESCE(_timescaledb_functions.cagg_watermark(:MAT_HT_ID_SMALL_INTEGER)::integer, _timescaledb_functions.cagg_watermark(:MAT_HT_ID_SMALL_INTEGER)::integer)
  GROUP BY (time_bucket(5, small_integer_ht."time"));

-- test with non constant value of the watermark function (should not be constified)
:EXPLAIN_ANALYZE  SELECT bucket, device, max
   FROM :MAT_HT_NAME_1H
  WHERE bucket < COALESCE(_timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(:MAT_HT_ID_1H)), '-infinity'::timestamp with time zone)
UNION ALL
 SELECT time_bucket('@ 1 day'::interval, chunks_1h.bucket) AS bucket,
    chunks_1h.device,
    max(chunks_1h.max) AS max
   FROM chunks_1h
  WHERE chunks_1h.bucket >= COALESCE(_timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(COALESCE(:MAT_HT_ID_1H, :MAT_HT_ID_1H))), '-infinity'::timestamp with time zone)
  GROUP BY (time_bucket('@ 1 day'::interval, chunks_1h.bucket)), chunks_1h.device;

-- test with NULL constant value of the watermark function (should not be constified)
:EXPLAIN_ANALYZE  SELECT bucket, device, max
   FROM :MAT_HT_NAME_1H
  WHERE bucket < COALESCE(_timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(:MAT_HT_ID_1H)), '-infinity'::timestamp with time zone)
UNION ALL
 SELECT time_bucket('@ 1 day'::interval, chunks_1h.bucket) AS bucket,
    chunks_1h.device,
    max(chunks_1h.max) AS max
   FROM chunks_1h
  WHERE chunks_1h.bucket >= COALESCE(_timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(COALESCE(:MAT_HT_ID_1H, :MAT_HT_ID_1H))), '-infinity'::timestamp with time zone)
  GROUP BY (time_bucket('@ 1 day'::interval, chunks_1h.bucket)), chunks_1h.device;

-- test with double COALESCE function (should be constified)
:EXPLAIN_ANALYZE  SELECT bucket, device, max
   FROM :MAT_HT_NAME_1H
  WHERE bucket < COALESCE(_timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(:MAT_HT_ID_1H)), '-infinity'::timestamp with time zone)
UNION ALL
 SELECT time_bucket('@ 1 day'::interval, chunks_1h.bucket) AS bucket,
    chunks_1h.device,
    max(chunks_1h.max) AS max
   FROM chunks_1h
  WHERE chunks_1h.bucket >= COALESCE(COALESCE(_timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(:MAT_HT_ID_1H)), '-infinity'::timestamp with time zone), '-infinity'::timestamp with time zone)
  GROUP BY (time_bucket('@ 1 day'::interval, chunks_1h.bucket)), chunks_1h.device;

\c :TEST_DBNAME :ROLE_SUPERUSER
TRUNCATE _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;
TRUNCATE _timescaledb_catalog.continuous_aggs_invalidation_threshold;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

-- Issue #6722: constify cagg_watermark using window func when querying a cagg
:EXPLAIN_ANALYZE
SELECT time_bucket, lead(count) OVER (ORDER BY time_bucket) FROM small_integer_ht_cagg;

-- SDC #1905: Using cagg on CTE should be constified
:EXPLAIN_ANALYZE
WITH cagg AS (
    SELECT * FROM small_integer_ht_cagg
)
SELECT * FROM cagg WHERE time_bucket > 10;

:EXPLAIN_ANALYZE
WITH cagg AS (
    SELECT * FROM small_integer_ht_cagg
),
other AS (
    SELECT * FROM generate_series(1,10)
)
SELECT * FROM cagg, other WHERE time_bucket > 10;
