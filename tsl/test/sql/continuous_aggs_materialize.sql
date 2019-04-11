-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE OR REPLACE FUNCTION run_continuous_agg_materialization(
    hypertable REGCLASS,
    materialization_table REGCLASS,
    input_view NAME,
    lowest_modified_value ANYELEMENT,
    greatest_modified_value ANYELEMENT,
    bucket_width "any",
    input_view_schema NAME = 'public'
) RETURNS VOID AS :TSL_MODULE_PATHNAME, 'ts_run_continuous_agg_materialization' LANGUAGE C VOLATILE;

-- stop the continous aggregate background workers from interfering
SELECT _timescaledb_internal.stop_background_workers();
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

CREATE TABLE continuous_agg_test(time BIGINT, data BIGINT, dummy BOOLEAN);
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
SELECT run_continuous_agg_materialization('continuous_agg_test', 'materialization', 'test_view', 9, 12, 2);

SELECT * FROM materialization ORDER BY 1;
SELECT * FROM _timescaledb_catalog.continuous_aggs_completed_threshold;
SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;


-- materialize out of bounds is a nop
SELECT run_continuous_agg_materialization('continuous_agg_test', 'materialization', 'test_view', 16, 19, 2);

SELECT * FROM materialization ORDER BY 1;
SELECT * FROM _timescaledb_catalog.continuous_aggs_completed_threshold;
SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;

-- materialize the rest of the data
\c :TEST_DBNAME :ROLE_SUPERUSER
DELETE FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold WHERE hypertable_id = 1;
INSERT INTO _timescaledb_catalog.continuous_aggs_invalidation_threshold VALUES (1, 16);
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
SELECT run_continuous_agg_materialization('continuous_agg_test', 'materialization', 'test_view', 12, 17, 2);

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
SELECT run_continuous_agg_materialization('continuous_agg_test', 'materialization', 'test_view', 10, 15, 2);

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

SELECT run_continuous_agg_materialization('continuous_agg_test', 'materialization', 'test_view', 9, 9, 2);

SELECT * FROM materialization ORDER BY 1;
SELECT * FROM _timescaledb_catalog.continuous_aggs_completed_threshold;
SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;

-- fill in the remaining
SELECT run_continuous_agg_materialization('continuous_agg_test', 'materialization', 'test_view', 10, 12, 2);
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

SELECT run_continuous_agg_materialization('continuous_agg_test', 'materialization', 'test_view', 10, 12, 2);

SELECT * FROM materialization ORDER BY 1;
SELECT * FROM _timescaledb_catalog.continuous_aggs_completed_threshold;
SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;

-- test invalidations
SELECT run_continuous_agg_materialization('continuous_agg_test', 'materialization', 'test_view', :big_int_max-6, :big_int_max, 2);

SELECT * FROM materialization ORDER BY 1;
SELECT * FROM _timescaledb_catalog.continuous_aggs_completed_threshold;
SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;

SELECT run_continuous_agg_materialization('continuous_agg_test', 'materialization', 'test_view', :big_int_min, :big_int_max, 2);

SELECT * FROM materialization ORDER BY 1;
SELECT * FROM _timescaledb_catalog.continuous_aggs_completed_threshold;
SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;

TRUNCATE materialization;

-- test dropping columns
ALTER TABLE continuous_agg_test DROP COLUMN dummy;

SELECT run_continuous_agg_materialization('continuous_agg_test', 'materialization', 'test_view', 9, 13, 2);

SELECT * FROM materialization;

ALTER TABLE continuous_agg_test ADD COLUMN d2 int;
TRUNCATE materialization;

SELECT run_continuous_agg_materialization('continuous_agg_test', 'materialization', 'test_view', 9, 13, 2);

SELECT * FROM materialization;

TRUNCATE materialization;

-- name and view that needs quotes
CREATE VIEW "view with spaces"(time_bucket, value) AS
    SELECT time_bucket(2, time), COUNT(data) as value
    FROM continuous_agg_test
    GROUP BY 1;

CREATE TABLE "table with spaces"(time_bucket BIGINT, value BIGINT);
select create_hypertable('"table with spaces"'::REGCLASS, 'time_bucket', chunk_time_interval => 100);
SELECT run_continuous_agg_materialization('continuous_agg_test', '"table with spaces"', 'view with spaces', 9, 21, 2);
SELECT * FROM "view with spaces" ORDER BY 1;
SELECT * FROM materialization ORDER BY 1;
SELECT * FROM _timescaledb_catalog.continuous_aggs_completed_threshold;
SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;


DROP TABLE materialization;

----------------------------
----------------------------
----------------------------

-- test with a time type
SET SESSION timezone TO 'UTC';
SET SESSION datestyle TO 'ISO';

CREATE TABLE materialization(time_bucket TIMESTAMPTZ, value BIGINT);
select create_hypertable('materialization', 'time_bucket');

CREATE TABLE continuous_agg_test_t(time TIMESTAMPTZ, data int);
select create_hypertable('continuous_agg_test_t', 'time');

INSERT INTO continuous_agg_test_t VALUES
    ('2019-02-02 2:00 UTC', 1),
    ('2019-02-02 3:00 UTC', 1),
    ('2019-02-02 4:00 UTC', 1),
    ('2019-02-02 5:00 UTC', 1);

SELECT * FROM continuous_agg_test_t;


TRUNCATE materialization;

\c :TEST_DBNAME :ROLE_SUPERUSER
INSERT INTO _timescaledb_catalog.continuous_aggs_invalidation_threshold VALUES (5, _timescaledb_internal.time_to_internal('2019-02-02 4:00 UTC'::TIMESTAMPTZ));
TRUNCATE _timescaledb_catalog.continuous_aggs_completed_threshold;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
SET SESSION timezone TO 'UTC';
SET SESSION datestyle TO 'ISO';

CREATE VIEW test_view_t(time_bucket, value) AS
    SELECT time_bucket('2 hours', time) as time_bucket, COUNT(data) as value
    FROM continuous_agg_test_t
    GROUP BY 1;

SELECT run_continuous_agg_materialization('continuous_agg_test_t', 'materialization', 'test_view_t'::NAME, '2019-02-02 2:00 UTC'::TIMESTAMPTZ, '2019-02-02 3:00 UTC'::TIMESTAMPTZ, '2 hours'::INTERVAL);

SELECT time_bucket, value FROM materialization ORDER BY 1;
SELECT * FROM _timescaledb_catalog.continuous_aggs_completed_threshold;
SELECT materialization_id, _timescaledb_internal.to_timestamp(watermark)
    FROM _timescaledb_catalog.continuous_aggs_completed_threshold
    WHERE materialization_id = 4;


SELECT run_continuous_agg_materialization('continuous_agg_test_t', 'materialization', 'test_view_t'::NAME, '2019-02-02 4:00 UTC'::TIMESTAMPTZ, '2019-02-02 5:00 UTC'::TIMESTAMPTZ, '2 hours'::INTERVAL);

SELECT time_bucket, value FROM materialization ORDER BY 1;
SELECT * FROM _timescaledb_catalog.continuous_aggs_completed_threshold;
SELECT materialization_id, _timescaledb_internal.to_timestamp(watermark)
    FROM _timescaledb_catalog.continuous_aggs_completed_threshold
    WHERE materialization_id = 4;

\c :TEST_DBNAME :ROLE_SUPERUSER
DELETE FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold WHERE hypertable_id = 5;
INSERT INTO _timescaledb_catalog.continuous_aggs_invalidation_threshold VALUES (5, _timescaledb_internal.time_to_internal('2019-02-02 6:00 UTC'::TIMESTAMPTZ));
TRUNCATE _timescaledb_catalog.continuous_aggs_completed_threshold;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
SET SESSION timezone TO 'UTC';
SET SESSION datestyle TO 'ISO';

SELECT run_continuous_agg_materialization('continuous_agg_test_t', 'materialization', 'test_view_t'::NAME, '2019-02-02 4:00 UTC'::TIMESTAMPTZ, '2019-02-02 5:00 UTC'::TIMESTAMPTZ, '2 hours'::INTERVAL);

SELECT time_bucket, value FROM materialization ORDER BY 1;
SELECT * FROM _timescaledb_catalog.continuous_aggs_completed_threshold;
SELECT materialization_id, _timescaledb_internal.to_timestamp(watermark)
    FROM _timescaledb_catalog.continuous_aggs_completed_threshold
    WHERE materialization_id = 4;

----------------------------
----------------------------
----------------------------

-- test with a real continuous aggregate

\c :TEST_DBNAME :ROLE_SUPERUSER
TRUNCATE _timescaledb_catalog.continuous_aggs_completed_threshold;
TRUNCATE _timescaledb_catalog.continuous_aggs_invalidation_threshold;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
SET SESSION timezone TO 'UTC';
SET SESSION datestyle TO 'ISO';

SELECT * FROM continuous_agg_test_t;

CREATE VIEW test_t_mat_view
    WITH ( timescaledb.continuous)
    AS SELECT time_bucket('2 hours', time), COUNT(data) as value
        FROM continuous_agg_test_t
        GROUP BY 1;
--TODO this should be created as part of CREATE VIEW
SELECT id as raw_table_id FROM _timescaledb_catalog.hypertable WHERE table_name='continuous_agg_test_t' \gset
CREATE TRIGGER continuous_agg_insert_trigger
    AFTER INSERT ON continuous_agg_test_t
    FOR EACH ROW EXECUTE PROCEDURE _timescaledb_internal.continuous_agg_invalidation_trigger(:raw_table_id);

SELECT mat_hypertable_id, raw_hypertable_id, user_view_schema, user_view_name,
       partial_view_schema, partial_view_name,
       _timescaledb_internal.to_timestamp(bucket_width), _timescaledb_internal.to_interval(refresh_lag),
       job_id
    FROM _timescaledb_catalog.continuous_agg;
SELECT mat_hypertable_id FROM _timescaledb_catalog.continuous_agg \gset
\c :TEST_DBNAME :ROLE_SUPERUSER
UPDATE _timescaledb_catalog.continuous_agg
    SET refresh_lag=7200000000
    WHERE mat_hypertable_id=:mat_hypertable_id;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
SET SESSION timezone TO 'UTC';
SET SESSION datestyle TO 'ISO';

SELECT _timescaledb_internal.to_interval(refresh_lag)
    FROM _timescaledb_catalog.continuous_agg;

SELECT * FROM test_t_mat_view;
SELECT * FROM _timescaledb_internal.ts_internal_test_t_mat_viewtab ORDER BY 1;

SELECT * FROM _timescaledb_catalog.continuous_aggs_completed_threshold;
SELECT materialization_id, _timescaledb_internal.to_timestamp(watermark)
    FROM _timescaledb_catalog.continuous_aggs_completed_threshold
    WHERE materialization_id = :mat_hypertable_id;

REFRESH MATERIALIZED VIEW test_t_mat_view;
SELECT * FROM _timescaledb_internal.ts_internal_test_t_mat_viewtab ORDER BY 1;
SELECT * FROM test_t_mat_view ORDER BY 1;
SELECT * FROM _timescaledb_catalog.continuous_aggs_completed_threshold;
SELECT materialization_id, _timescaledb_internal.to_timestamp(watermark)
    FROM _timescaledb_catalog.continuous_aggs_completed_threshold
    WHERE materialization_id = :mat_hypertable_id;
SELECT hypertable_id, _timescaledb_internal.to_timestamp(watermark) FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;

REFRESH MATERIALIZED VIEW test_t_mat_view;
SELECT * FROM _timescaledb_internal.ts_internal_test_t_mat_viewtab ORDER BY 1;
SELECT * FROM test_t_mat_view ORDER BY 1;
SELECT * FROM _timescaledb_catalog.continuous_aggs_completed_threshold;
SELECT materialization_id, _timescaledb_internal.to_timestamp(watermark)
    FROM _timescaledb_catalog.continuous_aggs_completed_threshold
    WHERE materialization_id = :mat_hypertable_id;
SELECT hypertable_id, _timescaledb_internal.to_timestamp(watermark) FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;

-- increase the last timestamp, so we actually materialize
INSERT INTO continuous_agg_test_t VALUES ('2019-02-02 7:00 UTC', 1);
SELECT * FROM continuous_agg_test_t ORDER BY 1;

REFRESH MATERIALIZED VIEW test_t_mat_view;
SELECT * FROM test_t_mat_view ORDER BY 1;
SELECT materialization_id, _timescaledb_internal.to_timestamp(watermark)
    FROM _timescaledb_catalog.continuous_aggs_completed_threshold
    WHERE materialization_id = :mat_hypertable_id;
SELECT hypertable_id, _timescaledb_internal.to_timestamp(watermark) FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;

-- test invalidations
INSERT INTO continuous_agg_test_t VALUES
    ('2019-02-02 2:00 UTC', 1),
    ('2019-02-02 3:00 UTC', 1),
    ('2019-02-02 4:00 UTC', 1),
    ('2019-02-02 5:00 UTC', 1);

SELECT * FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;

REFRESH MATERIALIZED VIEW test_t_mat_view;
SELECT * FROM test_t_mat_view ORDER BY 1;
SELECT * FROM _timescaledb_catalog.continuous_aggs_completed_threshold;
SELECT materialization_id, _timescaledb_internal.to_timestamp(watermark)
    FROM _timescaledb_catalog.continuous_aggs_completed_threshold
    WHERE materialization_id = :mat_hypertable_id;
SELECT hypertable_id, _timescaledb_internal.to_timestamp(watermark) FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;

-- test extremes
CREATE TABLE continuous_agg_extreme(time BIGINT, data BIGINT);
SELECT create_hypertable('continuous_agg_extreme', 'time', chunk_time_interval=> 10);

-- TODO we should be able to use time_bucket(5, ...) (note lack of '), but that is currently not
--      recognized as a constant
CREATE VIEW extreme_view
    WITH ( timescaledb.continuous)
    AS SELECT time_bucket('1', time), SUM(data) as value
        FROM continuous_agg_extreme
        GROUP BY 1;
--TODO this should be created as part of CREATE VIEW
SELECT id as raw_table_id FROM _timescaledb_catalog.hypertable WHERE table_name='continuous_agg_extreme' \gset
CREATE TRIGGER continuous_agg_insert_trigger
    AFTER INSERT ON continuous_agg_extreme
    FOR EACH ROW EXECUTE PROCEDURE _timescaledb_internal.continuous_agg_invalidation_trigger(:raw_table_id);
SELECT mat_hypertable_id FROM _timescaledb_catalog.continuous_agg WHERE raw_hypertable_id=:raw_table_id \gset

-- EMPTY table should be a nop
REFRESH MATERIALIZED VIEW extreme_view;
SELECT * FROM extreme_view;
SELECT materialization_id, watermark
    FROM _timescaledb_catalog.continuous_aggs_completed_threshold
    WHERE materialization_id=:mat_hypertable_id;
SELECT hypertable_id, watermark
    FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
    WHERE hypertable_id=:raw_table_id;

-- less than a bucket above MIN should be a nop
INSERT INTO continuous_agg_extreme VALUES
    (:big_int_min,   1);

REFRESH MATERIALIZED VIEW extreme_view;
SELECT * FROM extreme_view;
SELECT materialization_id, watermark
    FROM _timescaledb_catalog.continuous_aggs_completed_threshold
    WHERE materialization_id=:mat_hypertable_id;
SELECT hypertable_id, watermark
    FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
    WHERE hypertable_id=:raw_table_id;

-- but we will be able to materialize it once we have enough values
INSERT INTO continuous_agg_extreme VALUES
    (:big_int_min+10, 11);

REFRESH MATERIALIZED VIEW extreme_view;
SELECT * FROM extreme_view;
SELECT materialization_id, watermark
    FROM _timescaledb_catalog.continuous_aggs_completed_threshold
    WHERE materialization_id=:mat_hypertable_id;
SELECT hypertable_id, watermark
    FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
    WHERE hypertable_id=:raw_table_id;

-- we don't materialize the max value, but attempting will materialize the entire
-- table up to the materialization limit
INSERT INTO continuous_agg_extreme VALUES
    (100,                     101),
    (:big_int_max-5,          201),
    (:big_int_max-4,          201),
    (:big_int_max-3,          201),
    (:big_int_max-2,          201),
    (:big_int_max-1,          201),
    (:big_int_max,   :big_int_max);

REFRESH MATERIALIZED VIEW extreme_view;
SELECT * FROM extreme_view ORDER BY 1;
SELECT materialization_id, watermark
    FROM _timescaledb_catalog.continuous_aggs_completed_threshold
    WHERE materialization_id=:mat_hypertable_id;
SELECT hypertable_id, watermark
    FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
    WHERE hypertable_id=:raw_table_id;

--cleanup of continuous agg views --
DROP view test_t_mat_view CASCADE;
DROP view extreme_view CASCADE;

-- negative lag test
CREATE TABLE continuous_agg_negative(time BIGINT, data BIGINT);
SELECT create_hypertable('continuous_agg_negative', 'time', chunk_time_interval=> 10);

CREATE VIEW negative_view_5
    WITH (timescaledb.continuous, timescaledb.refresh_lag='-2')
    AS SELECT time_bucket('5', time), COUNT(data) as value
        FROM continuous_agg_negative
        GROUP BY 1;

-- two chunks, 4 buckets
INSERT INTO continuous_agg_negative
    SELECT i, i FROM generate_series(0, 11) AS i;
REFRESH MATERIALIZED VIEW negative_view_5;
SELECT * FROM negative_view_5 ORDER BY 1;

-- inserting 12 and 13 will cause the next bucket to materialize
-- even though the time_bucket would require us INSERT 14
INSERT INTO continuous_agg_negative VALUES (12, 12), (13, 13);
REFRESH MATERIALIZED VIEW negative_view_5;
SELECT * FROM negative_view_5 ORDER BY 1;

-- bucket is finished as normal with the additional value
INSERT INTO continuous_agg_negative VALUES (14, 14);
REFRESH MATERIALIZED VIEW negative_view_5;
SELECT * FROM negative_view_5 ORDER BY 1;

-- we can handle values near max
INSERT INTO continuous_agg_negative VALUES (:big_int_max-3, 201);
REFRESH MATERIALIZED VIEW negative_view_5;
SELECT * FROM negative_view_5 ORDER BY 1;


-- even when the subrtraction would make a completion time greater than max
INSERT INTO continuous_agg_negative VALUES (:big_int_max-1, 201);
REFRESH MATERIALIZED VIEW negative_view_5;
SELECT * FROM negative_view_5 ORDER BY 1;

DROP TABLE continuous_agg_negative CASCADE;

-- max_interval_per_job tests
CREATE TABLE continuous_agg_max_mat(time BIGINT, data BIGINT);
SELECT create_hypertable('continuous_agg_max_mat', 'time', chunk_time_interval=> 10);

-- only allow two time_buckets per run
CREATE VIEW max_mat_view
    WITH (timescaledb.continuous, timescaledb.max_interval_per_job='4', timescaledb.refresh_lag='-2')
    AS SELECT time_bucket('2', time), COUNT(data) as value
        FROM continuous_agg_max_mat
        GROUP BY 1;

INSERT INTO continuous_agg_max_mat SELECT i, i FROM generate_series(0, 10) AS i;

-- first run create two materializations
REFRESH MATERIALIZED VIEW max_mat_view;
SELECT * FROM max_mat_view ORDER BY 1;

-- repeated runs will eventually materialize all the data
REFRESH MATERIALIZED VIEW max_mat_view;
SELECT * FROM max_mat_view ORDER BY 1;

REFRESH MATERIALIZED VIEW max_mat_view;
SELECT * FROM max_mat_view ORDER BY 1;

REFRESH MATERIALIZED VIEW max_mat_view;
SELECT * FROM max_mat_view ORDER BY 1;

-- time type
CREATE TABLE continuous_agg_max_mat_t(time TIMESTAMPTZ, data TIMESTAMPTZ);
SELECT create_hypertable('continuous_agg_max_mat_t', 'time');

-- only allow two time_buckets per run
CREATE VIEW max_mat_view_t
    WITH (timescaledb.continuous, timescaledb.max_interval_per_job='4 hours', timescaledb.refresh_lag='-2 hours')
    AS SELECT time_bucket('2 hours', time), COUNT(data) as value
        FROM continuous_agg_max_mat_t
        GROUP BY 1;

INSERT INTO continuous_agg_max_mat_t
    SELECT i, i FROM
        generate_series('2019-09-09 1:00'::TIMESTAMPTZ, '2019-09-09 10:00', '1 hour') AS i;

-- first run create two materializations
REFRESH MATERIALIZED VIEW max_mat_view_t;
SELECT * FROM max_mat_view_t ORDER BY 1;

-- repeated runs will eventually materialize all the data
REFRESH MATERIALIZED VIEW max_mat_view_t;
SELECT * FROM max_mat_view_t ORDER BY 1;

REFRESH MATERIALIZED VIEW max_mat_view_t;
SELECT * FROM max_mat_view_t ORDER BY 1;

REFRESH MATERIALIZED VIEW max_mat_view_t;
SELECT * FROM max_mat_view_t ORDER BY 1;
