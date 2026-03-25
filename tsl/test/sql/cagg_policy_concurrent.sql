-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test creation of multiple refresh policies
SET timezone TO PST8PDT;

SET timescaledb.current_timestamp_mock TO '2025-06-01 0:30:00+00';

SELECT setseed(1);

-- test interval checking with bigint
CREATE TABLE overlap_test_bigint (
    time BIGINT NOT NULL,
    a INTEGER,
    b INTEGER
);

SELECT create_hypertable('overlap_test_bigint', 'time', chunk_time_interval => 100);

CREATE OR REPLACE FUNCTION integer_now_overlap_test_bigint()
RETURNS BIGINT LANGUAGE SQL STABLE AS
$$ SELECT COALESCE(MAX(time), 0) FROM overlap_test_bigint $$;

SELECT set_integer_now_func('overlap_test_bigint', 'integer_now_overlap_test_bigint');

INSERT INTO overlap_test_bigint
SELECT i, (i % 5), random() * 100
FROM generate_series(1, 2000) i;

CREATE MATERIALIZED VIEW mat_m1(time, counta)
WITH (timescaledb.continuous, timescaledb.materialized_only=true)
AS
SELECT
    time_bucket(10, time) AS bucket,
    count(a),
    sum(b)
FROM overlap_test_bigint
GROUP BY 1
WITH NO DATA;

/* Test interval checking when multiple policies are created on the same cagg */
SELECT add_continuous_aggregate_policy('mat_m1', NULL, 1000::bigint, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', 1000::bigint, NULL, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');

/* Creating policies in either order should work */
SELECT add_continuous_aggregate_policy('mat_m1', 1000::bigint, NULL, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', NULL, 1000::bigint, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');

SELECT add_continuous_aggregate_policy('mat_m1', NULL, 3000::bigint, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', 2000::bigint, NULL, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');

/* Test non-null offsets on both sides too */
SELECT add_continuous_aggregate_policy('mat_m1', 2000::bigint, 1000::bigint, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', 4000::bigint, 3000::bigint,'12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', 3000::bigint, 2000::bigint, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');

/* Check overlap is detected correctly */
\set ON_ERROR_STOP 0
SELECT add_continuous_aggregate_policy('mat_m1', NULL, 1000::bigint, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', 2000::bigint, NULL, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');

SELECT add_continuous_aggregate_policy('mat_m1', 2000::bigint, NULL, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', NULL, 1000::bigint, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');

SELECT add_continuous_aggregate_policy('mat_m1', 5000::bigint, 1000::bigint, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', 4000::bigint, 2000::bigint, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');

SELECT add_continuous_aggregate_policy('mat_m1', 4000::bigint, 2000::bigint, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', 5000::bigint, 1000::bigint, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');

SELECT add_continuous_aggregate_policy('mat_m1', NULL, 2000::bigint, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', NULL, 1000::bigint, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');

SELECT add_continuous_aggregate_policy('mat_m1', 2000::bigint, NULL, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', 1000::bigint, NULL, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');
\set ON_ERROR_STOP 1

/* Check behaviour when exact policy is already defined */
\set ON_ERROR_STOP 0
/*if_not_exists=false*/
SELECT add_continuous_aggregate_policy('mat_m1', 4000::bigint, 2000::bigint, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', 2000::bigint, 1000::bigint, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', 2000::bigint, 1000::bigint, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');

SELECT add_continuous_aggregate_policy('mat_m1', NULL, NULL, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', NULL, NULL, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');

\set ON_ERROR_STOP 1

/*if_not_exists => true*/
SELECT add_continuous_aggregate_policy('mat_m1', 4000::bigint, 2000::bigint, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', 2000::bigint, 1000::bigint, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', 2000::bigint, 1000::bigint, '12 h'::interval, if_not_exists => true);
SELECT remove_continuous_aggregate_policy('mat_m1');

SELECT add_continuous_aggregate_policy('mat_m1', NULL, NULL, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', NULL, NULL, '12 h'::interval, if_not_exists => true);
SELECT remove_continuous_aggregate_policy('mat_m1');

/* Throw an error if there is an overlap even if if_not_exists => true */
SELECT add_continuous_aggregate_policy('mat_m1', 4000::bigint, 2000::bigint, '12 h'::interval);
\set ON_ERROR_STOP 0
SELECT add_continuous_aggregate_policy('mat_m1', 3000::bigint, 1000::bigint, '12 h'::interval, if_not_exists => true);
\set ON_ERROR_STOP 1
SELECT remove_continuous_aggregate_policy('mat_m1');

/* Test `alter_job` changing the config */
SELECT add_continuous_aggregate_policy('mat_m1', NULL, 3000::bigint, '12 h'::interval);
SELECT id AS job_id, config AS config FROM _timescaledb_catalog.bgw_job WHERE proc_name = 'policy_refresh_continuous_aggregate' \gset
SELECT add_continuous_aggregate_policy('mat_m1', 2000::bigint, NULL, '12 h'::interval);

/* Alter end offset but don't overlap */
SELECT jsonb_set(:'config', '{end_offset}', '2000') AS config \gset
SELECT * FROM alter_job(:job_id, config := :'config');

\set ON_ERROR_STOP 0

/* Alter end offset to overlap with another job*/
SELECT jsonb_set(:'config', '{end_offset}', '1000') AS config \gset
SELECT * FROM alter_job(:job_id, config := :'config');

/* Alter end offset to be null */
SELECT jsonb_set(:'config', '{end_offset}', 'null') AS config \gset
SELECT * FROM alter_job(:job_id, config := :'config');

/* Alter job to be identical to existing job */
SELECT jsonb_set(:'config', '{start_offset}', '2000') AS config \gset
SELECT * FROM alter_job(:job_id, config := :'config');
\set ON_ERROR_STOP 1

SELECT remove_continuous_aggregate_policy('mat_m1');

SELECT add_continuous_aggregate_policy('mat_m1', 2000::bigint, NULL, '12 h'::interval);
SELECT id AS job_id, config AS config FROM _timescaledb_catalog.bgw_job WHERE proc_name = 'policy_refresh_continuous_aggregate' \gset
SELECT add_continuous_aggregate_policy('mat_m1', NULL, 3000::bigint, '12 h'::interval);

/* Alter end offset to null but no overlap */
SELECT jsonb_set(:'config', '{end_offset}', 'null') AS config \gset
SELECT * FROM alter_job(:job_id, config := :'config');
SELECT remove_continuous_aggregate_policy('mat_m1');

/* Test that refresh is done correctly even though multiple policies exist */
/* We do this by creating two CAggs on the same hypertable */
/* One will have a single policy while the other will have two policies with adjacent offsets */

CREATE MATERIALIZED VIEW mat_m2(time, counta)
WITH (timescaledb.continuous, timescaledb.materialized_only=true)
AS
SELECT
    time_bucket(10, time) AS bucket,
    count(a),
    sum(b)
FROM overlap_test_bigint
GROUP BY 1
WITH NO DATA;

/* Create two policies on mat_m1 */
SELECT add_continuous_aggregate_policy('mat_m1', 5000::bigint, 3000::bigint, '12 h'::interval) AS agg_m1_job_1 \gset
SELECT add_continuous_aggregate_policy('mat_m1', 3000::bigint, 1000::bigint, '12 h'::interval) AS agg_m1_job_2 \gset

/* Create single policy on mat_m2 */
SELECT add_continuous_aggregate_policy('mat_m2', 5000::bigint, 1000::bigint, '12 h'::interval) AS agg_m2_job \gset

/* Cleanup any existing data */
TRUNCATE mat_m1;
TRUNCATE mat_m2;

/* Refresh both continuous aggs immediately */
CALL run_job(:agg_m1_job_1);
CALL run_job(:agg_m1_job_2);
CALL run_job(:agg_m2_job);

/* Compare both outputs */
SELECT count(*) AS exp_row_count from mat_m1 \gset
SELECT count(*) AS actual_row_count from (
SELECT * from mat_m1 UNION SELECT * from mat_m2) union_q \gset

/* Row counts should be the same */
SELECT :exp_row_count = :actual_row_count, :exp_row_count, :actual_row_count;

SELECT * from mat_m2 EXCEPT SELECT * from mat_m1;
SELECT * from mat_m1 EXCEPT SELECT * from mat_m2;

DROP MATERIALIZED VIEW mat_m1;
DROP MATERIALIZED VIEW mat_m2;

CREATE TABLE overlap_test_timestamptz (
    time timestamptz NOT NULL,
    a INTEGER,
    b INTEGER
);

SELECT create_hypertable('overlap_test_timestamptz', 'time', chunk_time_interval => '1 day'::interval);

INSERT INTO overlap_test_timestamptz
SELECT t, (i % 5), random() * 100
FROM
generate_series('2025-01-01T01:01:01+00', '2025-06-01T01:01:01+00', INTERVAL '1 days') t,
generate_series(1, 10) i;

CREATE MATERIALIZED VIEW mat_m1(time, counta)
WITH (timescaledb.continuous, timescaledb.materialized_only=true)
AS
SELECT
    time_bucket('1 day', time) AS bucket,
    count(a),
    sum(b)
FROM overlap_test_timestamptz
GROUP BY 1
WITH NO DATA;

/* Test interval checking when multiple policies are created on the same cagg */
SELECT add_continuous_aggregate_policy('mat_m1', NULL, '30 days'::interval, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', '29 days'::interval, NULL, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');

/* Creating policies in either order should work */
SELECT add_continuous_aggregate_policy('mat_m1', '30 days'::interval, NULL, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', NULL, '30 days'::interval, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');

SELECT add_continuous_aggregate_policy('mat_m1', NULL, '30 days'::interval, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', '15 days'::interval, NULL, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');

/* Test non-null offsets on both sides too */
SELECT add_continuous_aggregate_policy('mat_m1', '30 days'::interval, '20 days'::interval, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', '10 days'::interval, '5 days'::interval, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', '19 days'::interval, '11 days'::interval, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');

/* Check overlap is detected correctly */
\set ON_ERROR_STOP 0
SELECT add_continuous_aggregate_policy('mat_m1', NULL, '30 days'::interval, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', '45 days'::interval, NULL, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');

SELECT add_continuous_aggregate_policy('mat_m1', '45 days'::interval, NULL, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', NULL, '30 days'::interval, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');

SELECT add_continuous_aggregate_policy('mat_m1', '30 days'::interval, '10 days'::interval, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', '20 days'::interval, '15 days'::interval, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');

SELECT add_continuous_aggregate_policy('mat_m1', '20 days'::interval, '15 days'::interval, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', '30 days'::interval, '10 days'::interval, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');

SELECT add_continuous_aggregate_policy('mat_m1', NULL, '30 days'::interval, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', NULL, '20 days'::interval, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');

SELECT add_continuous_aggregate_policy('mat_m1', '30 days'::interval, NULL, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', '20 days'::interval, NULL, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');
\set ON_ERROR_STOP 1

/* Check behaviour when exact policy is already defined */
\set ON_ERROR_STOP 0
/*if_not_exists=false*/
SELECT add_continuous_aggregate_policy('mat_m1', '45 days'::interval, '30 days', '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', '31 days'::interval, '15 days', '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', '31 days'::interval, '15 days', '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');

SELECT add_continuous_aggregate_policy('mat_m1', NULL::interval, NULL::interval, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', NULL::interval, NULL::interval, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');

\set ON_ERROR_STOP 1

/*if_not_exists => true*/
\set ON_ERROR_STOP 0
SELECT add_continuous_aggregate_policy('mat_m1', '45 days', '30 days', '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', '30 days'::interval, '15 days'::interval, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', '30 days'::interval, '15 days'::interval, '12 h'::interval, if_not_exists => true);
SELECT remove_continuous_aggregate_policy('mat_m1');

SELECT add_continuous_aggregate_policy('mat_m1', NULL::interval, NULL::interval, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', NULL::interval, NULL::interval, '12 h'::interval, if_not_exists => true);
SELECT remove_continuous_aggregate_policy('mat_m1');
\set ON_ERROR_STOP 1

/* Throw an error if there is an overlap even if if_not_exists => true */
SELECT add_continuous_aggregate_policy('mat_m1', '30 days'::interval, '10 days'::interval, '12 h'::interval);
\set ON_ERROR_STOP 0
SELECT add_continuous_aggregate_policy('mat_m1', '15 days'::interval, NULL, '12 h'::interval);
\set ON_ERROR_STOP 1
SELECT remove_continuous_aggregate_policy('mat_m1');

/* Mixing different interval units should also work correctly*/
SELECT add_continuous_aggregate_policy('mat_m1', NULL, '1 month'::interval, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', '2 weeks'::interval, NULL, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');

SELECT add_continuous_aggregate_policy('mat_m1', '1 year'::interval, '2 months'::interval, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', '5 weeks'::interval, '-7 days'::interval, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');

SELECT add_continuous_aggregate_policy('mat_m1', '2 weeks'::interval, NULL, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', NULL, '1 month'::interval, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');

\set ON_ERROR_STOP 0
SELECT add_continuous_aggregate_policy('mat_m1', NULL, '2 weeks'::interval, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', '1 month'::interval, NULL, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');
\set ON_ERROR_STOP 1

/* Check overlap with negative offsets */
SELECT add_continuous_aggregate_policy('mat_m1', NULL, '2 weeks'::interval, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', '-1 month'::interval, NULL, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');

\set ON_ERROR_STOP 0
SELECT add_continuous_aggregate_policy('mat_m1', NULL, '-2 weeks'::interval, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', '1 month'::interval, NULL, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');
\set ON_ERROR_STOP 1

/* Test `alter_job` changing the config */
SELECT add_continuous_aggregate_policy('mat_m1', NULL, '2 months'::interval, '12 h'::interval);
SELECT id AS job_id, config AS config FROM _timescaledb_catalog.bgw_job WHERE proc_name = 'policy_refresh_continuous_aggregate' \gset
SELECT add_continuous_aggregate_policy('mat_m1', '2 weeks'::interval, NULL, '12 h'::interval);

/* Alter end offset but don't overlap */
SELECT jsonb_set(:'config', '{end_offset}', '"30 days"') AS config \gset
SELECT * FROM alter_job(:job_id, config := :'config');

\set ON_ERROR_STOP 0

/* Alter end offset to overlap with another job*/
SELECT jsonb_set(:'config', '{end_offset}', '"1 week"') AS config \gset
SELECT * FROM alter_job(:job_id, config := :'config');

/* Alter end offset to be null */
SELECT jsonb_set(:'config', '{end_offset}', 'null') AS config \gset
SELECT * FROM alter_job(:job_id, config := :'config');

/* Alter job to be identical to existing job */
SELECT jsonb_set(:'config', '{start_offset}', '"2 weeks"') AS config \gset
SELECT * FROM alter_job(:job_id, config := :'config');
\set ON_ERROR_STOP 1

SELECT remove_continuous_aggregate_policy('mat_m1');

SELECT add_continuous_aggregate_policy('mat_m1', '2 weeks'::interval, NULL, '12 h'::interval);
SELECT id AS job_id, config AS config FROM _timescaledb_catalog.bgw_job WHERE proc_name = 'policy_refresh_continuous_aggregate' \gset
SELECT add_continuous_aggregate_policy('mat_m1', NULL, '2 months'::interval, '12 h'::interval);

/* Alter end offset to null but no overlap */
SELECT jsonb_set(:'config', '{end_offset}', 'null') AS config \gset
SELECT * FROM alter_job(:job_id, config := :'config');
SELECT remove_continuous_aggregate_policy('mat_m1');

CREATE MATERIALIZED VIEW mat_m2(time, counta)
WITH (timescaledb.continuous, timescaledb.materialized_only=true)
AS
SELECT
    time_bucket('1 day', time) AS bucket,
    count(a),
    sum(b)
FROM overlap_test_timestamptz
GROUP BY 1
WITH NO DATA;

/* Create two policies on mat_m1 */
SELECT add_continuous_aggregate_policy('mat_m1', NULL, '30 days'::interval, '12 h'::interval) AS agg_m1_job_1 \gset
SELECT add_continuous_aggregate_policy('mat_m1', '30 days'::interval,  NULL, '12 h'::interval) AS agg_m1_job_2 \gset

/* Create single policy on mat_m2 */
SELECT add_continuous_aggregate_policy('mat_m2', NULL, NULL, '12 h'::interval) AS agg_m2_job \gset

/* Cleanup any existing data */
TRUNCATE mat_m1;
TRUNCATE mat_m2;

/* Refresh both continuous aggs immediately */
CALL run_job(:agg_m1_job_1);
CALL run_job(:agg_m1_job_2);
CALL run_job(:agg_m2_job);

/* Compare both outputs */
SELECT count(*) AS exp_row_count from mat_m1 \gset
SELECT count(*) AS actual_row_count from (
SELECT * from mat_m1 UNION SELECT * from mat_m2) AS union_q \gset

/* Row counts should be the same */
SELECT :exp_row_count = :actual_row_count, :exp_row_count, :actual_row_count;

SELECT * from mat_m2 EXCEPT SELECT * from mat_m1;
SELECT * from mat_m1 EXCEPT SELECT * from mat_m2;

DROP MATERIALIZED VIEW mat_m1;
DROP MATERIALIZED VIEW mat_m2;


/* Test with variable sized buckets */

CREATE TABLE overlap_test_timestamptz_var (
    time timestamptz NOT NULL,
    a INTEGER,
    b INTEGER
);

SELECT create_hypertable('overlap_test_timestamptz_var', 'time', chunk_time_interval => '1 month'::interval);

INSERT INTO overlap_test_timestamptz_var
SELECT t, (i % 5), random() * 100
FROM
generate_series('2024-01-01T01:01:01+00', '2025-06-01T01:01:01+00', INTERVAL '1 day') t,
generate_series(1, 10) i;

CREATE MATERIALIZED VIEW mat_m1(time, counta)
WITH (timescaledb.continuous, timescaledb.materialized_only=true)
AS
SELECT
    time_bucket('1 month', time) AS bucket,
    count(a),
    sum(b)
FROM overlap_test_timestamptz_var
GROUP BY 1
WITH NO DATA;

/* Test interval checking when multiple policies are created on the same cagg */
SELECT add_continuous_aggregate_policy('mat_m1', NULL, '3 months'::interval, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', '3 months'::interval, NULL, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');

/* Creating policies in either order should work */
SELECT add_continuous_aggregate_policy('mat_m1', '3 months'::interval, NULL, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', NULL, '3 months'::interval, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');

SELECT add_continuous_aggregate_policy('mat_m1', NULL, '3 months'::interval, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', '2 months'::interval, NULL, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');

/* Test non-null offsets on both sides too */
SELECT add_continuous_aggregate_policy('mat_m1', '8 months'::interval, '6 months'::interval, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', '6 months'::interval, '12 weeks'::interval, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', '12 weeks'::interval, '1 days'::interval, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');

/* Check overlap is detected correctly */
\set ON_ERROR_STOP 0
SELECT add_continuous_aggregate_policy('mat_m1', NULL, '2 months'::interval, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', '3 months'::interval, NULL, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');

SELECT add_continuous_aggregate_policy('mat_m1', '3 months'::interval, NULL, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', NULL, '2 months'::interval, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');

SELECT add_continuous_aggregate_policy('mat_m1', '6 months'::interval, '1 week'::interval, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', '4 months'::interval, '2 weeks'::interval, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');

SELECT add_continuous_aggregate_policy('mat_m1', '4 months'::interval, '2 weeks'::interval, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', '6 months'::interval, '1 week'::interval, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');

SELECT add_continuous_aggregate_policy('mat_m1', NULL, '30 days'::interval, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', NULL, '20 days'::interval, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');

SELECT add_continuous_aggregate_policy('mat_m1', '30 days'::interval, NULL, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', '20 days'::interval, NULL, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');
\set ON_ERROR_STOP 1

/* Check behaviour when exact policy is already defined */
\set ON_ERROR_STOP 0
/*if_not_exists=false*/
SELECT add_continuous_aggregate_policy('mat_m1', '1 year'::interval, '8 months', '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', '8 months'::interval, '2 weeks', '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', '8 months'::interval, '2 weeks', '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');

SELECT add_continuous_aggregate_policy('mat_m1', NULL::interval, NULL::interval, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', NULL::interval, NULL::interval, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');

\set ON_ERROR_STOP 1

/*if_not_exists => true*/
\set ON_ERROR_STOP 0
SELECT add_continuous_aggregate_policy('mat_m1', '1 year'::interval, '8 months', '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', '8 months'::interval, '2 weeks', '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', '8 months'::interval, '2 weeks', '12 h'::interval, if_not_exists => true);
SELECT remove_continuous_aggregate_policy('mat_m1');

SELECT add_continuous_aggregate_policy('mat_m1', NULL::interval, NULL::interval, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', NULL::interval, NULL::interval, '12 h'::interval, if_not_exists => true);
SELECT remove_continuous_aggregate_policy('mat_m1');

\set ON_ERROR_STOP 1

/* Mixing different interval units should also work correctly*/
SELECT add_continuous_aggregate_policy('mat_m1', NULL, '1 month'::interval, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', '2 weeks'::interval, NULL, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');

SELECT add_continuous_aggregate_policy('mat_m1', '1 year'::interval, '2 months'::interval, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', '8 weeks'::interval, '-7 days'::interval, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');

SELECT add_continuous_aggregate_policy('mat_m1', '2 weeks'::interval, NULL, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', NULL, '1 month'::interval, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');

\set ON_ERROR_STOP 0
SELECT add_continuous_aggregate_policy('mat_m1', NULL, '2 weeks'::interval, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', '1 month'::interval, NULL, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');
\set ON_ERROR_STOP 1

/* Check overlap with negative offsets */
SELECT add_continuous_aggregate_policy('mat_m1', NULL, '2 weeks'::interval, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', '-1 month'::interval, NULL, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');

\set ON_ERROR_STOP 0
SELECT add_continuous_aggregate_policy('mat_m1', NULL, '-2 weeks'::interval, '12 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', '1 month'::interval, NULL, '12 h'::interval);
SELECT remove_continuous_aggregate_policy('mat_m1');
\set ON_ERROR_STOP 1

/* Test `alter_job` changing the config */
SELECT add_continuous_aggregate_policy('mat_m1', NULL, '2 months'::interval, '12 h'::interval);
SELECT id AS job_id, config AS config FROM _timescaledb_catalog.bgw_job WHERE proc_name = 'policy_refresh_continuous_aggregate' \gset
SELECT add_continuous_aggregate_policy('mat_m1', '2 weeks'::interval, NULL, '12 h'::interval);

/* Alter end offset but don't overlap */
SELECT jsonb_set(:'config', '{end_offset}', '"30 days"') AS config \gset
SELECT * FROM alter_job(:job_id, config := :'config');

\set ON_ERROR_STOP 0

/* Alter end offset to overlap with another job*/
SELECT jsonb_set(:'config', '{end_offset}', '"1 week"') AS config \gset
SELECT * FROM alter_job(:job_id, config := :'config');

/* Alter end offset to be null */
SELECT jsonb_set(:'config', '{end_offset}', 'null') AS config \gset
SELECT * FROM alter_job(:job_id, config := :'config');

/* Alter job to be identical to existing job */
SELECT jsonb_set(:'config', '{start_offset}', '"2 weeks"') AS config \gset
SELECT * FROM alter_job(:job_id, config := :'config');
\set ON_ERROR_STOP 1

SELECT remove_continuous_aggregate_policy('mat_m1');

SELECT add_continuous_aggregate_policy('mat_m1', '2 weeks'::interval, NULL, '12 h'::interval);
SELECT id AS job_id, config AS config FROM _timescaledb_catalog.bgw_job WHERE proc_name = 'policy_refresh_continuous_aggregate' \gset
SELECT add_continuous_aggregate_policy('mat_m1', NULL, '2 months'::interval, '12 h'::interval);

/* Alter end offset to null but no overlap */
SELECT jsonb_set(:'config', '{end_offset}', 'null') AS config \gset
SELECT * FROM alter_job(:job_id, config := :'config');
SELECT remove_continuous_aggregate_policy('mat_m1');

CREATE MATERIALIZED VIEW mat_m2(time, counta)
WITH (timescaledb.continuous, timescaledb.materialized_only=true)
AS
SELECT
    time_bucket('1 month', time) AS bucket,
    count(a),
    sum(b)
FROM overlap_test_timestamptz_var
GROUP BY 1
WITH NO DATA;

/* Create two policies on mat_m1 */
SELECT add_continuous_aggregate_policy('mat_m1', NULL, '30 days'::interval, '12 h'::interval, buckets_per_batch => 0) AS agg_m1_job_1 \gset
SELECT add_continuous_aggregate_policy('mat_m1', '30 days'::interval,  NULL, '12 h'::interval, buckets_per_batch => 0) AS agg_m1_job_2 \gset

/* Create single policy on mat_m2 */
SELECT add_continuous_aggregate_policy('mat_m2', NULL, NULL, '12 h'::interval, buckets_per_batch => 0) AS agg_m2_job \gset

/* Cleanup any existing data */
TRUNCATE mat_m1;
TRUNCATE mat_m2;

/* Refresh both continuous aggs immediately */
CALL run_job(:agg_m1_job_1);
CALL run_job(:agg_m1_job_2);
CALL run_job(:agg_m2_job);

/* Compare both outputs */
SELECT count(*) AS exp_row_count from mat_m1 \gset
SELECT count(*) AS actual_row_count from (
SELECT * from mat_m1 UNION SELECT * from mat_m2) AS union_q \gset

/* Row counts should be the same */
SELECT :exp_row_count = :actual_row_count, :exp_row_count, :actual_row_count;

SELECT * from mat_m2 EXCEPT SELECT * from mat_m1;
SELECT * from mat_m1 EXCEPT SELECT * from mat_m2;

DROP MATERIALIZED VIEW mat_m1;
DROP MATERIALIZED VIEW mat_m2;

/* Concurrent policies aren't allowed on hierarchical continuous aggs */
CREATE MATERIALIZED VIEW mat_m1
WITH (timescaledb.continuous, timescaledb.materialized_only=true)
AS
SELECT
    time_bucket('1 day', time) AS bucket,
    count(a) AS counta,
    sum(b) AS sumb
FROM overlap_test_timestamptz
GROUP BY 1
WITH NO DATA;

CREATE MATERIALIZED VIEW mat_m1_rollup
WITH (timescaledb.continuous, timescaledb.materialized_only=true)
AS
SELECT
    time_bucket('1 month', bucket) AS bucket,
    sum(counta) AS counta,
    sum(sumb) AS sumb
FROM mat_m1
GROUP BY 1
WITH NO DATA;

CREATE MATERIALIZED VIEW mat_m1_rollup2
WITH (timescaledb.continuous, timescaledb.materialized_only=true)
AS
SELECT
    time_bucket('1 month', bucket) AS bucket,
    sum(counta) AS counta,
    sum(sumb) AS sumb
FROM mat_m1
GROUP BY 1
WITH NO DATA;

SELECT add_continuous_aggregate_policy('mat_m1_rollup', NULL, '30 days'::interval, '12 h'::interval) AS "JOB_ID" \gset
-- alter_job should not be blocked
SELECT alter_job(:JOB_ID, next_start => '2000-01-01'::timestamptz);
\set ON_ERROR_STOP 0
-- Multiple policies on hierarchical cagg should not be allowed
SELECT add_continuous_aggregate_policy('mat_m1_rollup', '29 days'::interval, NULL, '12 h'::interval);
\set ON_ERROR_STOP 1
-- Adding the exact same policy with if_not_exists should succeed (not error)
SELECT add_continuous_aggregate_policy('mat_m1_rollup', NULL, '30 days'::interval, '12 h'::interval, if_not_exists => true);
-- different hierarchical caggs should be allowed to have their own policies
SELECT add_continuous_aggregate_policy('mat_m1_rollup2', NULL, '30 days'::interval, '12 h'::interval) AS "JOB_ID2" \gset
SELECT alter_job(:JOB_ID2, next_start => '2000-01-01'::timestamptz);

TRUNCATE mat_m1;
TRUNCATE mat_m1_rollup;
DROP MATERIALIZED VIEW mat_m1_rollup2;

/*
 * Test that concurrent policies on hierarchical CAggs propagate invalidations above correctly
 */

SET timezone TO 'UTC';

/* Create two policies on mat_m1 */
SELECT add_continuous_aggregate_policy('mat_m1', NULL, '30 days'::interval, '12 h'::interval, buckets_per_batch => 0) AS agg_m1_job_1 \gset
SELECT add_continuous_aggregate_policy('mat_m1', '30 days'::interval,  NULL, '12 h'::interval, buckets_per_batch => 0) AS agg_m1_job_2 \gset

SELECT remove_continuous_aggregate_policy('mat_m1_rollup');
SELECT add_continuous_aggregate_policy('mat_m1_rollup', NULL, NULL, '12 h'::interval) AS m1_rollup_job \gset

/* Refresh both continuous aggs */
CALL run_job(:agg_m1_job_1);
CALL run_job(:agg_m1_job_2);
CALL run_job(:m1_rollup_job);

/* Insert new data to generate invalidations */
INSERT INTO overlap_test_timestamptz
SELECT t, (i % 5), random() * 100
FROM
generate_series('2024-01-01T01:01:01+00', '2025-06-01T01:01:01+00', INTERVAL '1 day') t,
generate_series(1, 10) i;

SELECT
    ht.table_name AS hypertable_name,
    count(*),
    _timescaledb_functions.to_timestamp(min(hil.lowest_modified_value)) AS min_lowest_modified,
    _timescaledb_functions.to_timestamp(max(hil.greatest_modified_value)) AS max_greatest_modified
FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log hil
JOIN _timescaledb_catalog.hypertable ht ON ht.id = hil.hypertable_id
GROUP BY ht.schema_name, ht.table_name;

/* Run both L1 policies */
CALL run_job(:agg_m1_job_1);
CALL run_job(:agg_m1_job_2);

SELECT
    ht.table_name AS hypertable_name,
    count(*),
    _timescaledb_functions.to_timestamp(min(hil.lowest_modified_value)) AS min_lowest_modified,
    _timescaledb_functions.to_timestamp(max(hil.greatest_modified_value)) AS max_greatest_modified
FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log hil
JOIN _timescaledb_catalog.hypertable ht ON ht.id = hil.hypertable_id
GROUP BY ht.schema_name, ht.table_name;

/* Run L2 policy */
CALL run_job(:m1_rollup_job);

/* L2 must be consistent with L1 after both policies run */
SELECT r.bucket,
       (r.counta = l.reagg_counta) AS counta_match,
       (r.sumb = l.reagg_sumb) AS sumb_match
FROM mat_m1_rollup r
JOIN (
    SELECT time_bucket('1 month', bucket) AS month,
           sum(counta) AS reagg_counta,
           sum(sumb) AS reagg_sumb
    FROM mat_m1 GROUP BY 1
) l ON l.month = r.bucket
ORDER BY 1;

/* Restore timezone */
SET timezone TO PST8PDT;

DROP MATERIALIZED VIEW mat_m1_rollup;
DROP MATERIALIZED VIEW mat_m1;

-- Test: Variable-width bucket correctness with extend_last_bucket
-- mat_m1: two adjacent policies split at '6 months'
--   - Policy 1: [NULL, '6 months') -> will extend_last_bucket
--   - Policy 2: ['6 months', NULL)
-- compare with results from mat_m2 for correctness

CREATE MATERIALIZED VIEW mat_varwidth_m1
WITH (timescaledb.continuous, timescaledb.materialized_only=true)
AS
SELECT
    time_bucket('1 month', time) AS bucket,
    count(a),
    sum(b)
FROM overlap_test_timestamptz_var
GROUP BY 1
WITH NO DATA;

CREATE MATERIALIZED VIEW mat_varwidth_m2
WITH (timescaledb.continuous, timescaledb.materialized_only=true)
AS
SELECT
    time_bucket('1 month', time) AS bucket,
    count(a),
    sum(b)
FROM overlap_test_timestamptz_var
GROUP BY 1
WITH NO DATA;

SET timezone TO 'UTC';
SELECT time_bucket('1 month' , time) FROM overlap_test_timestamptz_var
GROUP BY 1
ORDER BY 1;

/* Create two adjacent policies on mat_varwidth_m1 */
SELECT add_continuous_aggregate_policy('mat_varwidth_m1', NULL, '6 months'::interval, '12 h'::interval, buckets_per_batch => 0) AS varwidth_m1_job_1 \gset
SELECT add_continuous_aggregate_policy('mat_varwidth_m1', '6 months'::interval, NULL, '12 h'::interval, buckets_per_batch => 0) AS varwidth_m1_job_2 \gset

/* Create single policy on mat_varwidth_m2 */
SELECT add_continuous_aggregate_policy('mat_varwidth_m2', NULL, NULL, '12 h'::interval, buckets_per_batch => 0) AS varwidth_m2_job \gset

/* Set mock time so policy windows are deterministic */
/* Expect varwidth_m1_job_1 will refresh all the way upto Jan 1 */
SET timescaledb.current_timestamp_mock TO '2025-06-01 05:30:00+00';

/* Refresh both continuous aggs immediately */
CALL run_job(:varwidth_m1_job_1);
SELECT * FROM mat_varwidth_m1 ORDER BY 1;
-- data for dec 01 2024 should be identical for the cagg query and the raw query below
SELECT
    time_bucket('1 month', time) AS bucket,
    count(a),
    sum(b)
FROM overlap_test_timestamptz_var
WHERE time_bucket('1 month', time)  >= '2024-12-01 00:00:00+00' and time_bucket('1 month', time) < '2025-01-01 00:00:00+00'
GROUP BY 1;

CALL run_job(:varwidth_m1_job_2);
CALL run_job(:varwidth_m2_job);

--verify that all the data has been materialized now
SELECT min(bucket), max(bucket) FROM mat_varwidth_m1;

/* Compare both outputs */
SELECT * from mat_varwidth_m2 EXCEPT SELECT * from mat_varwidth_m1;
SELECT * from mat_varwidth_m1 EXCEPT SELECT * from mat_varwidth_m2;


DROP MATERIALIZED VIEW mat_varwidth_m1;
DROP MATERIALIZED VIEW mat_varwidth_m2;

/* Test 3: Three concurrent policies — middle policy also extends correctly
 *
 * With 3 adjacent policies, both the first and middle should have
 * extend_last_bucket = true, and only the last should not.
 *
 *   - Policy 1: ['7 days', '3 days')
 *   - Policy 2: ['3 days', '1 day')
 *   - Policy 3: ['1 day',  '1 hour')
 */

CREATE TABLE test_3pol_timestamptz (
    time timestamptz NOT NULL,
    a INTEGER,
    b INTEGER
);

SELECT create_hypertable('test_3pol_timestamptz', 'time', chunk_time_interval => '1 day'::interval);

INSERT INTO test_3pol_timestamptz
SELECT t, 1, (random() * 100)::int
FROM
generate_series('2025-05-20T11:05:00+00', '2025-05-27T12:05:00+00', INTERVAL '1 hour') t;

CREATE MATERIALIZED VIEW mat_3pol_m1
WITH (timescaledb.continuous, timescaledb.materialized_only=true)
AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    count(a),
    sum(b)
FROM test_3pol_timestamptz
GROUP BY 1
WITH NO DATA;

CREATE MATERIALIZED VIEW mat_3pol_m2
WITH (timescaledb.continuous, timescaledb.materialized_only=true)
AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    count(a),
    sum(b)
FROM test_3pol_timestamptz
GROUP BY 1
WITH NO DATA;

/* Three adjacent policies on mat_3pol_m1 */
SELECT add_continuous_aggregate_policy('mat_3pol_m1', '7 days'::interval, '3 days'::interval, '12 h'::interval, buckets_per_batch => 0) AS pol3_m1_job_1 \gset
SELECT add_continuous_aggregate_policy('mat_3pol_m1', '3 days'::interval, '1 day'::interval, '12 h'::interval, buckets_per_batch => 0) AS pol3_m1_job_2 \gset
SELECT add_continuous_aggregate_policy('mat_3pol_m1', '1 day'::interval, '1 hour'::interval, '12 h'::interval, buckets_per_batch => 0) AS pol3_m1_job_3 \gset

/* Single policy on mat_3pol_m2 */
SELECT add_continuous_aggregate_policy('mat_3pol_m2', '7 days'::interval, '1 hour'::interval, '12 h'::interval, buckets_per_batch => 0) AS pol3_m2_job \gset

/* Run all jobs */
SET timescaledb.current_timestamp_mock TO '2025-05-27 12:30:00+00';

-- Policy 1 (not last): extend_last_bucket applies.
-- Raw window [May 20 12:30, May 24 12:30) inscribes to [May 20 13:00, May 24 12:00),
-- then extension pushes end to May 24 13:00). So Max bucket = May 24 12:00.
SELECT ts_now_mock(),
 ts_now_mock() - '7 days'::interval, ts_now_mock() - '3 days'::interval;
CALL run_job(:pol3_m1_job_1);
SELECT min(bucket), max(bucket) FROM mat_3pol_m1;

-- Policy 2 (not last): extend_last_bucket applies.
-- Raw window [May 24 12:30, May 26 12:30) inscribes to [May 24 13:00, May 26 12:00),
-- then extension pushes end to May 26 13:00). Max bucket = May 26 12:00.
SELECT ts_now_mock(),
 ts_now_mock() - '3 days'::interval, ts_now_mock() - '1 day'::interval;
CALL run_job(:pol3_m1_job_2);
SELECT min(bucket), max(bucket) FROM mat_3pol_m1;

-- Policy 3 (last): no extension.
-- Raw window [May 26 12:30, May 27 11:30) inscribes to [May 26 13:00, May 27 11:00).
-- End is exclusive, so last complete bucket is [10:00, 11:00). Max bucket = May 27 10:00.
SELECT ts_now_mock(),
 ts_now_mock() - '1 day'::interval, ts_now_mock() - '1 hour'::interval;
CALL run_job(:pol3_m1_job_3);
SELECT min(bucket), max(bucket) FROM mat_3pol_m1;


CALL run_job(:pol3_m2_job);

/* Output should be the same */
SELECT * from mat_3pol_m2 EXCEPT SELECT * from mat_3pol_m1;
SELECT * from mat_3pol_m1 EXCEPT SELECT * from mat_3pol_m2;

DROP MATERIALIZED VIEW mat_3pol_m1;
DROP MATERIALIZED VIEW mat_3pol_m2;
DROP TABLE test_3pol_timestamptz CASCADE;

--restore time zone settings
SET timezone TO PST8PDT;
