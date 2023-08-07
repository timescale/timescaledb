-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE TABLE continuous_agg_test(time int, data int);
select create_hypertable('continuous_agg_test', 'time', chunk_time_interval=> 10);
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
select create_hypertable('continuous_agg_test_mat', 'time', chunk_time_interval=> 10);
INSERT INTO _timescaledb_catalog.continuous_agg VALUES (2, 1, NULL, '','','','',0,'','');
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
    WITH ( timescaledb.continuous)
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
    WITH ( timescaledb.continuous)
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
\c :TEST_DBNAME :ROLE_SUPERUSER
TRUNCATE _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;
TRUNCATE _timescaledb_catalog.continuous_aggs_invalidation_threshold;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
