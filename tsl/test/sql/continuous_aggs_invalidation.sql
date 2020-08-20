-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Disable background workers since we are testing manual refresh
\c :TEST_DBNAME :ROLE_SUPERUSER
SELECT _timescaledb_internal.stop_background_workers();
SET ROLE :ROLE_DEFAULT_PERM_USER;
SET datestyle TO 'ISO, YMD';
SET timezone TO 'UTC';

CREATE TABLE conditions (time int NOT NULL, device int, temp float);
SELECT create_hypertable('conditions', 'time', chunk_time_interval => 10);

CREATE TABLE measurements (time int NOT NULL, device int, temp float);
SELECT create_hypertable('measurements', 'time', chunk_time_interval => 10);

CREATE OR REPLACE FUNCTION cond_now()
RETURNS int LANGUAGE SQL STABLE AS
$$
    SELECT coalesce(max(time), 0)
    FROM conditions
$$;

CREATE OR REPLACE FUNCTION measure_now()
RETURNS int LANGUAGE SQL STABLE AS
$$
    SELECT coalesce(max(time), 0)
    FROM measurements
$$;

SELECT set_integer_now_func('conditions', 'cond_now');
SELECT set_integer_now_func('measurements', 'measure_now');

INSERT INTO conditions
SELECT t, ceil(abs(timestamp_hash(to_timestamp(t)::timestamp))%4)::int,
       abs(timestamp_hash(to_timestamp(t)::timestamp))%40
FROM generate_series(1, 100, 1) t;

INSERT INTO measurements
SELECT * FROM conditions;

-- Show the most recent data
SELECT * FROM conditions
ORDER BY time DESC, device
LIMIT 10;

-- Create two continuous aggregates on the same hypertable to test
-- that invalidations are handled correctly across both of them.
CREATE MATERIALIZED VIEW cond_10
WITH (timescaledb.continuous,
      timescaledb.materialized_only=true)
AS
SELECT time_bucket(10, time) AS day, device, avg(temp) AS avg_temp
FROM conditions
GROUP BY 1,2;

CREATE MATERIALIZED VIEW cond_20
WITH (timescaledb.continuous,
      timescaledb.materialized_only=true)
AS
SELECT time_bucket(20, time) AS day, device, avg(temp) AS avg_temp
FROM conditions
GROUP BY 1,2;

CREATE MATERIALIZED VIEW measure_10
WITH (timescaledb.continuous,
      timescaledb.materialized_only=true)
AS
SELECT time_bucket(10, time) AS day, device, avg(temp) AS avg_temp
FROM measurements
GROUP BY 1,2;

-- There should be three continuous aggregates, two on one hypertable
-- and one on the other:
SELECT mat_hypertable_id, raw_hypertable_id, user_view_name
FROM _timescaledb_catalog.continuous_agg;

-- The continuous aggregates should be empty
SELECT * FROM cond_10
ORDER BY day DESC, device;

SELECT * FROM cond_20
ORDER BY day DESC, device;

SELECT * FROM measure_10
ORDER BY day DESC, device;

-- Must refresh to move the invalidation threshold, or no
-- invalidations will be generated. Initially, there is no threshold
-- set:
SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
ORDER BY 1,2;

-- Now refresh up to 50, and the threshold should be updated accordingly:
CALL refresh_continuous_aggregate('cond_10', 1, 50);
SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
ORDER BY 1,2;

-- Refreshing below the threshold does not move it:
CALL refresh_continuous_aggregate('cond_10', 20, 49);
SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
ORDER BY 1,2;

-- Refreshing measure_10 moves the threshold only for the other hypertable:
CALL refresh_continuous_aggregate('measure_10', 1, 30);
SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
ORDER BY 1,2;

-- Refresh on the second continuous aggregate, cond_20, on the first
-- hypertable moves the same threshold as when refreshing cond_10:
CALL refresh_continuous_aggregate('cond_20', 60, 100);
SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
ORDER BY 1,2;

-- There should be no hypertable invalidations initially:
SELECT hypertable_id AS hyper_id,
       lowest_modified_value AS start,
       greatest_modified_value AS end
       FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
       ORDER BY 1,2,3;

SELECT materialization_id AS cagg_id,
       lowest_modified_value AS start,
       greatest_modified_value AS end
       FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
       ORDER BY 1,2,3;

-- Create invalidations across different ranges. Some of these should
-- be deleted and others cut in different ways when a refresh is
-- run. Note that the refresh window is inclusive in the start of the
-- window but exclusive at the end.

-- Entries that should be left unmodified:
INSERT INTO conditions VALUES (10, 4, 23.7);
INSERT INTO conditions VALUES (10, 5, 23.8), (19, 3, 23.6);
INSERT INTO conditions VALUES (60, 3, 23.7), (70, 4, 23.7);

-- Should see some invaliations in the hypertable invalidation log:
SELECT hypertable_id AS hyper_id,
       lowest_modified_value AS start,
       greatest_modified_value AS end
       FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
       ORDER BY 1,2,3;

-- Generate some invalidations for the other hypertable
INSERT INTO measurements VALUES (20, 4, 23.7);
INSERT INTO measurements VALUES (30, 5, 23.8), (80, 3, 23.6);

-- Should now see invalidations for both hypertables
SELECT hypertable_id AS hyper_id,
       lowest_modified_value AS start,
       greatest_modified_value AS end
       FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
       ORDER BY 1,2,3;

-- First refresh a window where we don't have any invalidations. This
-- allows us to see only the copying of the invalidations to the per
-- cagg log without additional processing.
CALL refresh_continuous_aggregate('cond_10', 20, 60);
-- Invalidation threshold remains at 100:
SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
ORDER BY 1,2;

-- Invalidations should be moved from the hypertable invalidation log
-- to the continuous aggregate log, but only for the hypertable that
-- the refreshed aggregate belongs to:
SELECT hypertable_id AS hyper_id,
       lowest_modified_value AS start,
       greatest_modified_value AS end
       FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
       ORDER BY 1,2,3;

SELECT materialization_id AS cagg_id,
       lowest_modified_value AS start,
       greatest_modified_value AS end
       FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
       ORDER BY 1,2,3;

-- Now add more invalidations to test a refresh that overlaps with them.
-- Entries that should be deleted:
INSERT INTO conditions VALUES (30, 1, 23.4), (59, 1, 23.4);
INSERT INTO conditions VALUES (20, 1, 23.4), (30, 1, 23.4);
-- Entries that should be cut to the right, leaving an invalidation to
-- the left of the refresh window:
INSERT INTO conditions VALUES (1, 4, 23.7), (25, 1, 23.4);
INSERT INTO conditions VALUES (19, 4, 23.7), (59, 1, 23.4);
-- Entries that should be cut to the left and right, leaving two
-- invalidation entries on each side of the refresh window:
INSERT INTO conditions VALUES (2, 2, 23.5), (60, 1, 23.4);
INSERT INTO conditions VALUES (3, 2, 23.5), (80, 1, 23.4);
-- Entries that should be cut to the left, leaving an invalidation to
-- the right of the refresh window:
INSERT INTO conditions VALUES (60, 3, 23.6), (90, 3, 23.6);
INSERT INTO conditions VALUES (20, 5, 23.8), (100, 3, 23.6);

-- New invalidations in the hypertable invalidation log:
SELECT hypertable_id AS hyper_id,
       lowest_modified_value AS start,
       greatest_modified_value AS end
       FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
       ORDER BY 1,2,3;

-- But nothing has yet changed in the cagg invalidation log:
SELECT materialization_id AS cagg_id,
       lowest_modified_value AS start,
       greatest_modified_value AS end
       FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
       ORDER BY 1,2,3;

-- Refresh to process invalidations for daily temperature:
CALL refresh_continuous_aggregate('cond_10', 20, 60);

-- Invalidations should be moved from the hypertable invalidation log
-- to the continuous aggregate log.
SELECT hypertable_id AS hyper_id,
       lowest_modified_value AS start,
       greatest_modified_value AS end
       FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
       ORDER BY 1,2,3;

-- Only the cond_10 cagg should have its entries cut:
SELECT materialization_id AS cagg_id,
       lowest_modified_value AS start,
       greatest_modified_value AS end
       FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
       ORDER BY 1,2,3;

-- Refresh also cond_20:
CALL refresh_continuous_aggregate('cond_20', 20, 60);

-- The cond_20 cagg should also have its entries cut:
SELECT materialization_id AS cagg_id,
       lowest_modified_value AS start,
       greatest_modified_value AS end
       FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
       ORDER BY 1,2,3;

-- Refresh cond_10 to completely remove an invalidation:
CALL refresh_continuous_aggregate('cond_10', 1, 20);

-- The 1-19 invalidation should be deleted:
SELECT materialization_id AS cagg_id,
       lowest_modified_value AS start,
       greatest_modified_value AS end
       FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
       ORDER BY 1,2,3;
