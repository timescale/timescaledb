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

SET ROLE :ROLE_DEFAULT_PERM_USER;

CREATE TABLE continuous_agg_test(time BIGINT, data BIGINT, dummy BOOLEAN);
select create_hypertable('continuous_agg_test', 'time', chunk_time_interval=> 10);

-- simulated materialization table
CREATE TABLE materialization(time_bucket BIGINT, value BIGINT);
select create_hypertable('materialization', 'time_bucket', chunk_time_interval => 100);

SET ROLE :ROLE_SUPERUSER;
INSERT INTO _timescaledb_catalog.continuous_aggs_invalidation_threshold VALUES (1, 14);
SET ROLE :ROLE_DEFAULT_PERM_USER;

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
SET ROLE :ROLE_SUPERUSER;
DELETE FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold WHERE hypertable_id = 1;
INSERT INTO _timescaledb_catalog.continuous_aggs_invalidation_threshold VALUES (1, 16);
SET ROLE :ROLE_DEFAULT_PERM_USER;
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

SET ROLE :ROLE_SUPERUSER;
DELETE FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold WHERE hypertable_id = 1;
INSERT INTO _timescaledb_catalog.continuous_aggs_invalidation_threshold VALUES (1, 22);
SET ROLE :ROLE_DEFAULT_PERM_USER;

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

SET ROLE :ROLE_SUPERUSER;
DELETE FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold WHERE hypertable_id = 1;
INSERT INTO _timescaledb_catalog.continuous_aggs_invalidation_threshold VALUES (1, :big_int_max-1);
SET ROLE :ROLE_DEFAULT_PERM_USER;

INSERT INTO continuous_agg_test VALUES
    (:big_int_max - 4, 1), (:big_int_max - 3, 5), (:big_int_max - 2, 7), (:big_int_max - 1, 9), (:big_int_max, 11),
    (:big_int_min, 22), (:big_int_min + 1, 23);

SET client_min_messages TO error;
SELECT run_continuous_agg_materialization('continuous_agg_test', 'materialization', 'test_view', 10, 12, 2);
RESET client_min_messages;

SELECT * FROM materialization ORDER BY 1;
SELECT * FROM _timescaledb_catalog.continuous_aggs_completed_threshold;
SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;

-- test invalidations
SET client_min_messages TO error;
SELECT run_continuous_agg_materialization('continuous_agg_test', 'materialization', 'test_view', :big_int_max-6, :big_int_max, 2);
RESET client_min_messages;

SELECT * FROM materialization ORDER BY 1;
SELECT * FROM _timescaledb_catalog.continuous_aggs_completed_threshold;
SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;

SET client_min_messages TO error;
SELECT run_continuous_agg_materialization('continuous_agg_test', 'materialization', 'test_view', :big_int_min, :big_int_max, 2);
RESET client_min_messages;

SELECT * FROM materialization ORDER BY 1;
SELECT * FROM _timescaledb_catalog.continuous_aggs_completed_threshold;
SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;

TRUNCATE materialization;

-- test dropping columns
ALTER TABLE continuous_agg_test DROP COLUMN dummy;

SET client_min_messages TO error;
SELECT run_continuous_agg_materialization('continuous_agg_test', 'materialization', 'test_view', 9, 13, 2);
RESET client_min_messages;

SELECT * FROM materialization;

ALTER TABLE continuous_agg_test ADD COLUMN d2 int;
TRUNCATE materialization;

SET client_min_messages TO error;
SELECT run_continuous_agg_materialization('continuous_agg_test', 'materialization', 'test_view', 9, 13, 2);
RESET client_min_messages;

SELECT * FROM materialization;

TRUNCATE materialization;

-- name and view that needs quotes
CREATE VIEW "view with spaces"(time_bucket, value) AS
    SELECT time_bucket(2, time), COUNT(data) as value
    FROM continuous_agg_test
    GROUP BY 1;

CREATE TABLE "table with spaces"(time_bucket BIGINT, value BIGINT);
select create_hypertable('"table with spaces"'::REGCLASS, 'time_bucket', chunk_time_interval => 100);
SET client_min_messages TO error;
SELECT run_continuous_agg_materialization('continuous_agg_test', '"table with spaces"', 'view with spaces', 9, 21, 2);
RESET client_min_messages;
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

SET ROLE :ROLE_SUPERUSER;
INSERT INTO _timescaledb_catalog.continuous_aggs_invalidation_threshold VALUES (5, _timescaledb_internal.time_to_internal('2019-02-02 4:00 UTC'::TIMESTAMPTZ));
TRUNCATE _timescaledb_catalog.continuous_aggs_completed_threshold;
SET ROLE :ROLE_DEFAULT_PERM_USER;
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

SET ROLE :ROLE_SUPERUSER;
DELETE FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold WHERE hypertable_id = 5;
INSERT INTO _timescaledb_catalog.continuous_aggs_invalidation_threshold VALUES (5, _timescaledb_internal.time_to_internal('2019-02-02 6:00 UTC'::TIMESTAMPTZ));
TRUNCATE _timescaledb_catalog.continuous_aggs_completed_threshold;
SET ROLE :ROLE_DEFAULT_PERM_USER;
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

SET ROLE :ROLE_SUPERUSER;
TRUNCATE _timescaledb_catalog.continuous_aggs_completed_threshold;
TRUNCATE _timescaledb_catalog.continuous_aggs_invalidation_threshold;
SET ROLE :ROLE_DEFAULT_PERM_USER;
SET SESSION timezone TO 'UTC';
SET SESSION datestyle TO 'ISO';
SET client_min_messages TO LOG;

SELECT * FROM continuous_agg_test_t;

CREATE VIEW test_t_mat_view
    WITH (timescaledb.continuous, timescaledb.materialized_only=true)
    AS SELECT time_bucket('2 hours', time), COUNT(data) as value
        FROM continuous_agg_test_t
        GROUP BY 1;

SELECT mat_hypertable_id, raw_hypertable_id, user_view_schema, user_view_name,
       partial_view_schema, partial_view_name,
       _timescaledb_internal.to_timestamp(bucket_width), _timescaledb_internal.to_interval(refresh_lag),
       job_id
    FROM _timescaledb_catalog.continuous_agg;
SELECT mat_hypertable_id FROM _timescaledb_catalog.continuous_agg \gset
SET ROLE :ROLE_SUPERUSER;
UPDATE _timescaledb_catalog.continuous_agg
    SET refresh_lag=7200000000
    WHERE mat_hypertable_id=:mat_hypertable_id;
SET ROLE :ROLE_DEFAULT_PERM_USER;
SET SESSION timezone TO 'UTC';
SET SESSION datestyle TO 'ISO';

SELECT _timescaledb_internal.to_interval(refresh_lag)
    FROM _timescaledb_catalog.continuous_agg;

SELECT ca.raw_hypertable_id as "RAW_HYPERTABLE_ID",
       h.schema_name AS "MAT_SCHEMA_NAME",
       h.table_name AS "MAT_TABLE_NAME",
       partial_view_name as "PART_VIEW_NAME",
       partial_view_schema as "PART_VIEW_SCHEMA"
FROM _timescaledb_catalog.continuous_agg ca
INNER JOIN _timescaledb_catalog.hypertable h ON(h.id = ca.mat_hypertable_id)
WHERE user_view_name = 'test_t_mat_view'
\gset

SELECT * FROM test_t_mat_view;
SELECT * FROM :"MAT_SCHEMA_NAME".:"MAT_TABLE_NAME" ORDER BY 1;

SELECT * FROM _timescaledb_catalog.continuous_aggs_completed_threshold;
SELECT materialization_id, _timescaledb_internal.to_timestamp(watermark)
    FROM _timescaledb_catalog.continuous_aggs_completed_threshold
    WHERE materialization_id = :mat_hypertable_id;

SET timescaledb.current_timestamp_mock = '2019-02-02 5:00 UTC';

REFRESH MATERIALIZED VIEW test_t_mat_view;
SELECT * FROM :"MAT_SCHEMA_NAME".:"MAT_TABLE_NAME" ORDER BY 1;
SELECT * FROM test_t_mat_view ORDER BY 1;
SELECT * FROM _timescaledb_catalog.continuous_aggs_completed_threshold;
SELECT materialization_id, _timescaledb_internal.to_timestamp(watermark)
    FROM _timescaledb_catalog.continuous_aggs_completed_threshold
    WHERE materialization_id = :mat_hypertable_id;
SELECT hypertable_id, _timescaledb_internal.to_timestamp(watermark) FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;

REFRESH MATERIALIZED VIEW test_t_mat_view;
SELECT * FROM :"MAT_SCHEMA_NAME".:"MAT_TABLE_NAME" ORDER BY 1;
SELECT * FROM test_t_mat_view ORDER BY 1;
SELECT * FROM _timescaledb_catalog.continuous_aggs_completed_threshold;
SELECT materialization_id, _timescaledb_internal.to_timestamp(watermark)
    FROM _timescaledb_catalog.continuous_aggs_completed_threshold
    WHERE materialization_id = :mat_hypertable_id;
SELECT hypertable_id, _timescaledb_internal.to_timestamp(watermark) FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;

-- increase the current timestamp, so we actually materialize
SET timescaledb.current_timestamp_mock = '2019-02-02 7:00 UTC';
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

CREATE OR REPLACE FUNCTION integer_now_continuous_agg_extreme() returns BIGINT LANGUAGE SQL STABLE as $$ SELECT coalesce(max(time), BIGINT '-9223372036854775808') FROM continuous_agg_extreme $$;
SELECT set_integer_now_func('continuous_agg_extreme', 'integer_now_continuous_agg_extreme');

-- TODO we should be able to use time_bucket(5, ...) (note lack of '), but that is currently not
--      recognized as a constant
CREATE VIEW extreme_view
    WITH (timescaledb.continuous, timescaledb.materialized_only=true)
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

CREATE OR REPLACE FUNCTION integer_now_continuous_agg_negative() returns BIGINT LANGUAGE SQL STABLE as $$ SELECT coalesce(max(time), BIGINT '-9223372036854775808') FROM continuous_agg_negative $$;
SELECT set_integer_now_func('continuous_agg_negative', 'integer_now_continuous_agg_negative');

CREATE VIEW negative_view_5
    WITH (timescaledb.continuous, timescaledb.materialized_only=true, timescaledb.refresh_lag='-2')
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

--inserting a really large value does not work because of max_interval_per_job
INSERT INTO continuous_agg_negative VALUES (:big_int_max-3, 201);
REFRESH MATERIALIZED VIEW negative_view_5;
SELECT * FROM negative_view_5 ORDER BY 1;

DROP VIEW negative_view_5 CASCADE;
TRUNCATE continuous_agg_negative;

CREATE VIEW negative_view_5
            WITH (timescaledb.continuous, timescaledb.materialized_only=true, timescaledb.refresh_lag='-2')
AS SELECT time_bucket('5', time), COUNT(data) as value
   FROM continuous_agg_negative
   GROUP BY 1;

-- we can handle values near max
INSERT INTO continuous_agg_negative VALUES (:big_int_max-3, 201);
SET client_min_messages TO error;
REFRESH MATERIALIZED VIEW negative_view_5;
RESET client_min_messages;
SELECT * FROM negative_view_5 ORDER BY 1;


-- even when the subtraction would make a completion time greater than max
INSERT INTO continuous_agg_negative VALUES (:big_int_max-1, 201);
SET client_min_messages TO error;
REFRESH MATERIALIZED VIEW negative_view_5;
RESET client_min_messages;
SELECT * FROM negative_view_5 ORDER BY 1;

DROP TABLE continuous_agg_negative CASCADE;

-- max_interval_per_job tests
CREATE TABLE continuous_agg_max_mat(time BIGINT, data BIGINT);
SELECT create_hypertable('continuous_agg_max_mat', 'time', chunk_time_interval=> 10);

CREATE OR REPLACE FUNCTION integer_now_continuous_agg_max_mat() returns BIGINT LANGUAGE SQL STABLE as $$ SELECT coalesce(max(time), BIGINT '-9223372036854775808') FROM continuous_agg_max_mat $$;
SELECT set_integer_now_func('continuous_agg_max_mat', 'integer_now_continuous_agg_max_mat');

-- only allow two time_buckets per run
CREATE VIEW max_mat_view
    WITH (timescaledb.continuous, timescaledb.materialized_only=true, timescaledb.max_interval_per_job='4', timescaledb.refresh_lag='-2')
    AS SELECT time_bucket('2', time), COUNT(data) as value
        FROM continuous_agg_max_mat
        GROUP BY 1;

INSERT INTO continuous_agg_max_mat SELECT i, i FROM generate_series(0, 10) AS i;
SET client_min_messages TO LOG;

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

--one invalidation covered by max_interval refreshed right away
INSERT INTO continuous_agg_max_mat SELECT i, i FROM generate_series(0, 3) AS i;
REFRESH MATERIALIZED VIEW max_mat_view;
SELECT * FROM max_mat_view ORDER BY 1;
--nothing to do
REFRESH MATERIALIZED VIEW max_mat_view;

--one invalidation too big for max_interval will be split
INSERT INTO continuous_agg_max_mat SELECT i, i FROM generate_series(0, 6) AS i;
REFRESH MATERIALIZED VIEW max_mat_view;
REFRESH MATERIALIZED VIEW max_mat_view;
SELECT * FROM max_mat_view ORDER BY 1;
--nothing to do
REFRESH MATERIALIZED VIEW max_mat_view;

--many invalidation too big for max_interval will be split
INSERT INTO continuous_agg_max_mat SELECT i, i FROM generate_series(0, 1) AS i;
INSERT INTO continuous_agg_max_mat SELECT i, i FROM generate_series(2, 3) AS i;
INSERT INTO continuous_agg_max_mat SELECT i, i FROM generate_series(4, 5) AS i;
REFRESH MATERIALIZED VIEW max_mat_view;
REFRESH MATERIALIZED VIEW max_mat_view;
SELECT * FROM max_mat_view ORDER BY 1;
--nothing to do
REFRESH MATERIALIZED VIEW max_mat_view;


--one invalidation + new entries  requires two refreshes if the refresh budget taken up by invalidation
INSERT INTO continuous_agg_max_mat SELECT i, i FROM generate_series(0, 3) AS i;
INSERT INTO continuous_agg_max_mat SELECT i, i FROM generate_series(11, 12) AS i;
REFRESH MATERIALIZED VIEW max_mat_view;
REFRESH MATERIALIZED VIEW max_mat_view;
SELECT * FROM max_mat_view ORDER BY 1;
--nothing to do
REFRESH MATERIALIZED VIEW max_mat_view;

--one invalidation + new entries  done in one refresh if the refresh budget allows
INSERT INTO continuous_agg_max_mat SELECT i, i FROM generate_series(0, 1) AS i;
INSERT INTO continuous_agg_max_mat SELECT i, i FROM generate_series(14, 15) AS i;
REFRESH MATERIALIZED VIEW max_mat_view;
SELECT * FROM max_mat_view ORDER BY 1;
--nothing to do.
REFRESH MATERIALIZED VIEW max_mat_view;

-- time type
CREATE TABLE continuous_agg_max_mat_t(time TIMESTAMPTZ, data TIMESTAMPTZ);
SELECT create_hypertable('continuous_agg_max_mat_t', 'time');

-- only allow two time_buckets per run
CREATE VIEW max_mat_view_t
    WITH (timescaledb.continuous, timescaledb.materialized_only=true, timescaledb.max_interval_per_job='4 hours', timescaledb.refresh_lag='-2 hours')
    AS SELECT time_bucket('2 hours', time), COUNT(data) as value
        FROM continuous_agg_max_mat_t
        GROUP BY 1;

INSERT INTO continuous_agg_max_mat_t
    SELECT i, i FROM
        generate_series('2019-09-09 1:00'::TIMESTAMPTZ, '2019-09-09 10:00', '1 hour') AS i;

SET timescaledb.current_timestamp_mock = '2019-09-09 10:00 UTC';
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

-- regular timestamp
CREATE TABLE continuous_agg_max_mat_timestamp(time TIMESTAMP);
SELECT create_hypertable('continuous_agg_max_mat_timestamp', 'time');

CREATE VIEW max_mat_view_timestamp
    WITH (timescaledb.continuous, timescaledb.materialized_only=true, timescaledb.refresh_lag='-2 hours')
    AS SELECT time_bucket('2 hours', time)
        FROM continuous_agg_max_mat_timestamp
        GROUP BY 1;

INSERT INTO continuous_agg_max_mat_timestamp
    SELECT generate_series('2019-09-09 1:00'::TIMESTAMPTZ, '2019-09-09 10:00', '1 hour');

-- first materializes everything
REFRESH MATERIALIZED VIEW max_mat_view_timestamp;
SELECT * FROM max_mat_view_timestamp ORDER BY 1;

-- date
CREATE TABLE continuous_agg_max_mat_date(time DATE);
SELECT create_hypertable('continuous_agg_max_mat_date', 'time');

CREATE VIEW max_mat_view_date
    WITH (timescaledb.continuous, timescaledb.materialized_only=true, timescaledb.refresh_lag='-7 days')
    AS SELECT time_bucket('7 days', time)
        FROM continuous_agg_max_mat_date
        GROUP BY 1;

INSERT INTO continuous_agg_max_mat_date
    SELECT generate_series('2019-09-01'::DATE, '2019-09-010 10:00', '1 day');

SET timescaledb.current_timestamp_mock = '2019-09-09 01:00 UTC';
--SET timescaledb.current_timestamp_mock = '2019-09-09 01:01:01 UTC';
-- first materializes everything
REFRESH MATERIALIZED VIEW max_mat_view_date;
SELECT * FROM max_mat_view_date ORDER BY 1;

SELECT view_name, completed_threshold, invalidation_threshold, job_id, job_status, last_run_duration
    FROM timescaledb_information.continuous_aggregate_stats ORDER BY 1;

SELECT view_name, refresh_lag, max_interval_per_job
    FROM timescaledb_information.continuous_aggregates ORDER BY 1;

SET SESSION timezone TO 'EST';

SELECT view_name, completed_threshold, invalidation_threshold, job_id, job_status, last_run_duration
    FROM timescaledb_information.continuous_aggregate_stats ORDER BY 1;

SELECT view_name, refresh_lag, max_interval_per_job
    FROM timescaledb_information.continuous_aggregates ORDER BY 1;

-- test timezone is respected when materializing cagg with TIMESTAMP time column
RESET timescaledb.current_timestamp_mock;
RESET client_min_messages;
SET SESSION timezone TO 'GMT+5';

CREATE TABLE timezone_test(time timestamp NOT NULL);
SELECT table_name FROM create_hypertable('timezone_test','time');
INSERT INTO timezone_test VALUES (now() - '30m'::interval), (now()), (now() + '30m'::interval);

CREATE VIEW timezone_test_summary
    WITH (timescaledb.continuous,timescaledb.materialized_only=true)
    AS SELECT time_bucket('5m', time)
        FROM timezone_test
        GROUP BY 1;

REFRESH MATERIALIZED VIEW timezone_test_summary;

-- this must return 1 as only 1 row is in the materialization interval
SELECT count(*) FROM timezone_test_summary;
DROP TABLE timezone_test CASCADE;

-- repeat test with timezone with negative offset
SET SESSION timezone TO 'GMT-5';

CREATE TABLE timezone_test(time timestamp NOT NULL);
SELECT table_name FROM create_hypertable('timezone_test','time');
INSERT INTO timezone_test VALUES (now() - '30m'::interval), (now()), (now() + '30m'::interval);

CREATE VIEW timezone_test_summary
    WITH (timescaledb.continuous,timescaledb.materialized_only=true)
    AS SELECT time_bucket('5m', time)
        FROM timezone_test
        GROUP BY 1;

REFRESH MATERIALIZED VIEW timezone_test_summary;

-- this must return 1 as only 1 row is in the materialization interval
SELECT count(*) FROM timezone_test_summary;
DROP TABLE timezone_test CASCADE;

-- TESTS for integer based table to verify watermark limited by max value of time column and not by now
CREATE TABLE continuous_agg_int(time BIGINT, data BIGINT);
SELECT create_hypertable('continuous_agg_int', 'time', chunk_time_interval=> 10);

CREATE OR REPLACE FUNCTION integer_now_continuous_agg_max() returns BIGINT LANGUAGE SQL STABLE as $$ SELECT BIGINT '9223372036854775807' $$;
SELECT set_integer_now_func('continuous_agg_int', 'integer_now_continuous_agg_max');

CREATE VIEW continuous_agg_int_max 
    WITH (timescaledb.continuous, timescaledb.refresh_lag='0')
    AS SELECT time_bucket('10', time), COUNT(data) as value
        FROM continuous_agg_int
        GROUP BY 1;

INSERT INTO continuous_agg_int values (-10, 100), (1,100), (10, 100);
select chunk_table, ranges from chunk_relation_size('continuous_agg_int');
REFRESH MATERIALIZED VIEW continuous_agg_int_max;
REFRESH MATERIALIZED VIEW continuous_agg_int_max;
REFRESH MATERIALIZED VIEW continuous_agg_int_max;
select * from continuous_agg_int_max;
--watermark is 20
SELECT view_name, completed_threshold, invalidation_threshold
FROM timescaledb_information.continuous_aggregate_stats
where view_name::text like 'continuous_agg_int_max';

-- TEST that watermark is limited by max value from data and not by now() 
CREATE TABLE continuous_agg_ts_max_t(timecol TIMESTAMPTZ, data integer);
SELECT create_hypertable('continuous_agg_ts_max_t', 'timecol', chunk_time_interval=>'365 days'::interval);
CREATE VIEW continuous_agg_ts_max_view
    WITH (timescaledb.continuous, timescaledb.max_interval_per_job='365 days', timescaledb.refresh_lag='-2 hours')
    AS SELECT time_bucket('2 hours', timecol), COUNT(data) as value
        FROM continuous_agg_ts_max_t
        GROUP BY 1;

INSERT INTO continuous_agg_ts_max_t
    values ('1969-01-01 1:00'::timestamptz, 10);

REFRESH MATERIALIZED VIEW continuous_agg_ts_max_view;
SELECT view_name, completed_threshold, invalidation_threshold
FROM timescaledb_information.continuous_aggregate_stats
where view_name::text like 'continuous_agg_ts_max_view';

INSERT INTO continuous_agg_ts_max_t
    values ('1970-01-01 1:00'::timestamptz, 10);

select chunk_table, ranges from chunk_relation_size('continuous_agg_ts_max_t');

REFRESH MATERIALIZED VIEW continuous_agg_ts_max_view;
REFRESH MATERIALIZED VIEW continuous_agg_ts_max_view;
SELECT view_name, completed_threshold, invalidation_threshold
FROM timescaledb_information.continuous_aggregate_stats
where view_name::text like 'continuous_agg_ts_max_view';

-- no more new data to materialize, threshold should not change
REFRESH MATERIALIZED VIEW continuous_agg_ts_max_view;
SELECT view_name, completed_threshold, invalidation_threshold
FROM timescaledb_information.continuous_aggregate_stats
where view_name::text like 'continuous_agg_ts_max_view';
