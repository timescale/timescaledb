-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE OR REPLACE FUNCTION run_continuous_agg_materialization(
    hypertable_id INTEGER,
    materialization_id INTEGER,
    input_view NAME,
    lowest_modified_value ANYELEMENT,
    greatest_modified_value ANYELEMENT,
    bucket_width "any",
    input_view_schema NAME = 'public'
) RETURNS VOID AS :TSL_MODULE_PATHNAME, 'ts_run_continuous_agg_materialization' LANGUAGE C VOLATILE;

-- stop the continous aggregate background workers from interfering
SELECT _timescaledb_internal.stop_background_workers();
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

CREATE TABLE continuous_agg_test(time BIGINT, data BIGINT);
select create_hypertable('continuous_agg_test', 'time', chunk_time_interval=> 10);

-- simulated materialization table
CREATE TABLE materialization(time_bucket BIGINT, value BIGINT);
select create_hypertable('materialization', 'time_bucket', chunk_time_interval => 100);

\c :TEST_DBNAME :ROLE_SUPERUSER
INSERT INTO _timescaledb_catalog.continuous_aggs_invalidation_threshold VALUES (1, 14);
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

-- simulated continuous_agg insert view
CREATE VIEW test_view(time_bucket, value) AS
    SELECT time_bucket(2, time), COUNT(data) as value
    FROM continuous_agg_test
    GROUP BY 1;

INSERT INTO continuous_agg_test VALUES (10, 1), (11, 2), (12, 3), (13, 4), (14, 1), (15, 1), (16, 1), (17, 1);

SELECT * FROM test_view ORDER BY 1;

SELECT * FROM materialization ORDER BY 1;
SELECT * FROM _timescaledb_catalog.continuous_aggs_completed_threshold;
SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;


-- materialize some of the data into the view
SELECT run_continuous_agg_materialization(1, 2, 'test_view', 9, 12, 2);

SELECT * FROM materialization ORDER BY 1;
SELECT * FROM _timescaledb_catalog.continuous_aggs_completed_threshold;
SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;


-- materialize out of bounds is a nop
SELECT run_continuous_agg_materialization(1, 2, 'test_view', 16, 19, 2);

SELECT * FROM materialization ORDER BY 1;
SELECT * FROM _timescaledb_catalog.continuous_aggs_completed_threshold;
SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;

-- materialize the rest of the data
\c :TEST_DBNAME :ROLE_SUPERUSER
DELETE FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold WHERE hypertable_id = 1;
INSERT INTO _timescaledb_catalog.continuous_aggs_invalidation_threshold VALUES (1, 16);
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
SELECT run_continuous_agg_materialization(1, 2, 'test_view', 12, 17, 2);

SELECT * FROM materialization ORDER BY 1;
SELECT * FROM _timescaledb_catalog.continuous_aggs_completed_threshold;
SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;


-- insert some additional data into the materialization table
-- (as if we had stale materializations)
INSERT INTO materialization VALUES
    (11, 1),
    (13, 3),
    (15, 4);

SELECT * FROM materialization ORDER BY 1;

-- materializing should delete the invalid data, and leave the correct data
SELECT run_continuous_agg_materialization(1, 2, 'test_view', 10, 15, 2);

SELECT * FROM materialization ORDER BY 1;
SELECT * FROM _timescaledb_catalog.continuous_aggs_completed_threshold;
SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;

-- test gaps
INSERT INTO continuous_agg_test VALUES
    (9, 0), (10, 1), (11, 2), (12, 3), (13, 4), (20, 1), (21, 1);

\c :TEST_DBNAME :ROLE_SUPERUSER
DELETE FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold WHERE hypertable_id = 1;
INSERT INTO _timescaledb_catalog.continuous_aggs_invalidation_threshold VALUES (1, 22);
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

SELECT run_continuous_agg_materialization(1, 2, 'test_view', 9, 9, 2);

SELECT * FROM materialization ORDER BY 1;
SELECT * FROM _timescaledb_catalog.continuous_aggs_completed_threshold;
SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;

-- fill in the remaining
SELECT run_continuous_agg_materialization(1, 2, 'test_view', 10, 12, 2);
SELECT * FROM materialization ORDER BY 1;
SELECT * FROM _timescaledb_catalog.continuous_aggs_completed_threshold;
SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;

-- the bucket containing big_int_max should not be materialized, but all others should
SELECT  9223372036854775807 as big_int_max \gset
SELECT -9223372036854775808	 as big_int_min \gset

\c :TEST_DBNAME :ROLE_SUPERUSER
DELETE FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold WHERE hypertable_id = 1;
INSERT INTO _timescaledb_catalog.continuous_aggs_invalidation_threshold VALUES (1, :big_int_max);
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

INSERT INTO continuous_agg_test VALUES
    (:big_int_max - 4, 1), (:big_int_max - 3, 5), (:big_int_max - 2, 7), (:big_int_max - 1, 9), (:big_int_max, 11),
    (:big_int_min, 22), (:big_int_min + 1, 23);

SELECT run_continuous_agg_materialization(1, 2, 'test_view', 10, 12, 2);

SELECT * FROM materialization ORDER BY 1;
SELECT * FROM _timescaledb_catalog.continuous_aggs_completed_threshold;
SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;

-- test invalidations
SELECT run_continuous_agg_materialization(1, 2, 'test_view', :big_int_max-6, :big_int_max, 2);

SELECT * FROM materialization ORDER BY 1;
SELECT * FROM _timescaledb_catalog.continuous_aggs_completed_threshold;
SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;

SELECT run_continuous_agg_materialization(1, 2, 'test_view', :big_int_min, :big_int_max, 2);

SELECT * FROM materialization ORDER BY 1;
SELECT * FROM _timescaledb_catalog.continuous_aggs_completed_threshold;
SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;

\c :TEST_DBNAME :ROLE_SUPERUSER
DELETE FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold WHERE hypertable_id = 1;
INSERT INTO _timescaledb_catalog.continuous_aggs_invalidation_threshold VALUES (1, 22);
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

TRUNCATE materialization;
-- name and view that needs quotes
CREATE VIEW "view with spaces"(time_bucket, value) AS
    SELECT time_bucket(2, time), COUNT(data) as value
    FROM continuous_agg_test
    GROUP BY 1;

CREATE TABLE "table with spaces"(time_bucket BIGINT, value BIGINT);
select create_hypertable('"table with spaces"'::REGCLASS, 'time_bucket', chunk_time_interval => 100);
SELECT run_continuous_agg_materialization(1, 3, 'view with spaces', 9, 21, 2);
SELECT * FROM "view with spaces" ORDER BY 1;
SELECT * FROM materialization ORDER BY 1;
SELECT * FROM _timescaledb_catalog.continuous_aggs_completed_threshold;
SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;


DROP TABLE materialization;

-- test with a time type
CREATE TABLE materialization(time_bucket TIMESTAMPTZ, value BIGINT);
select create_hypertable('materialization', 'time_bucket');

CREATE TABLE continuous_agg_test_t(dummy BOOLEAN, time TIMESTAMPTZ, data int);
select create_hypertable('continuous_agg_test_t', 'time');

INSERT INTO continuous_agg_test_t VALUES
    (false, '2019-02-02', 1),
    (true, '2019-02-03', 1),
    (true, '2019-02-04', 1),
    (false, '2019-02-05', 1);

TRUNCATE materialization;

\c :TEST_DBNAME :ROLE_SUPERUSER
INSERT INTO _timescaledb_catalog.continuous_aggs_invalidation_threshold VALUES (5, _timescaledb_internal.time_to_internal('Sun Feb 03 16:00:00 2019 PST'::TIMESTAMPTZ));
TRUNCATE _timescaledb_catalog.continuous_aggs_completed_threshold;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

CREATE VIEW test_view_t(time_bucket, value) AS
    SELECT time_bucket('2 days', time) as time_bucket, COUNT(data) as value
    FROM continuous_agg_test_t
    GROUP BY 1;

SELECT run_continuous_agg_materialization(5, 4, 'test_view_t'::NAME, '2019-02-02'::TIMESTAMPTZ, '2019-02-03'::TIMESTAMPTZ, '2 days'::INTERVAL);

SELECT time_bucket, value FROM materialization ORDER BY 1;
SELECT * FROM _timescaledb_catalog.continuous_aggs_completed_threshold;
SELECT materialization_id, _timescaledb_internal.to_timestamp(watermark)
    FROM _timescaledb_catalog.continuous_aggs_completed_threshold
    WHERE materialization_id = 4;

-- test with a real continuous aggregate

\c :TEST_DBNAME :ROLE_SUPERUSER
TRUNCATE _timescaledb_catalog.continuous_aggs_completed_threshold;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

CREATE VIEW test_t_mat_view
    WITH ( timescaledb.continuous_agg = 'start')
    AS SELECT time_bucket('2 days', time), COUNT(data) as value
        FROM continuous_agg_test_t
        GROUP BY 1;

SELECT * FROM _timescaledb_catalog.continuous_agg;

SELECT run_continuous_agg_materialization(5, 6, 'ts_internal_test_t_mat_viewview', '2019-02-02'::TIMESTAMPTZ, '2019-02-03'::TIMESTAMPTZ, '2 days'::INTERVAL, input_view_schema => '_timescaledb_internal');

SELECT * FROM test_t_mat_view;

ALTER TABLE continuous_agg_test_t DROP COLUMN dummy;
TRUNCATE _timescaledb_internal.ts_internal_test_t_mat_viewtab;

SELECT run_continuous_agg_materialization(5, 6, 'ts_internal_test_t_mat_viewview', '2019-02-02'::TIMESTAMPTZ, '2019-02-03'::TIMESTAMPTZ, '2 days'::INTERVAL, input_view_schema => '_timescaledb_internal');

SELECT * FROM test_t_mat_view;


ALTER TABLE continuous_agg_test_t ADD COLUMN d2 int;
TRUNCATE _timescaledb_internal.ts_internal_test_t_mat_viewtab;

SELECT run_continuous_agg_materialization(5, 6, 'ts_internal_test_t_mat_viewview', '2019-02-02'::TIMESTAMPTZ, '2019-02-03'::TIMESTAMPTZ, '2 days'::INTERVAL, input_view_schema => '_timescaledb_internal');

SELECT * FROM test_t_mat_view;
