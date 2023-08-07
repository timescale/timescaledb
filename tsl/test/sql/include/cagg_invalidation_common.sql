-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Disable background workers since we are testing manual refresh
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER
SELECT _timescaledb_internal.stop_background_workers();
SET ROLE :ROLE_DEFAULT_PERM_USER;
SET datestyle TO 'ISO, YMD';
SET timezone TO 'UTC';

CREATE TABLE conditions (time bigint NOT NULL, device int, temp float);
\if :IS_DISTRIBUTED
SELECT create_distributed_hypertable('conditions', 'time', chunk_time_interval => 10, replication_factor => 2);
\else
SELECT create_hypertable('conditions', 'time', chunk_time_interval => 10);
\endif

CREATE TABLE measurements (time int NOT NULL, device int, temp float);
\if :IS_DISTRIBUTED
SELECT create_distributed_hypertable('measurements', 'time', chunk_time_interval => 10, replication_factor => 2);
\else
SELECT create_hypertable('measurements', 'time', chunk_time_interval => 10);
\endif

CREATE OR REPLACE FUNCTION bigint_now()
RETURNS bigint LANGUAGE SQL STABLE AS
$$
    SELECT coalesce(max(time), 0)
    FROM conditions
$$;
\if :IS_DISTRIBUTED
CALL distributed_exec($DIST$
CREATE OR REPLACE FUNCTION bigint_now()
RETURNS bigint LANGUAGE SQL STABLE AS
$$
    SELECT coalesce(max(time), 0)
    FROM conditions
$$;
$DIST$);
\endif

CREATE OR REPLACE FUNCTION int_now()
RETURNS int LANGUAGE SQL STABLE AS
$$
    SELECT coalesce(max(time), 0)
    FROM measurements
$$;
\if :IS_DISTRIBUTED
CALL distributed_exec($DIST$
CREATE OR REPLACE FUNCTION int_now()
RETURNS int LANGUAGE SQL STABLE AS
$$
    SELECT coalesce(max(time), 0)
    FROM measurements
$$;
$DIST$);
\endif

SELECT set_integer_now_func('conditions', 'bigint_now');
SELECT set_integer_now_func('measurements', 'int_now');

INSERT INTO conditions
SELECT t, ceil(abs(timestamp_hash(to_timestamp(t)::timestamp))%4)::int,
       abs(timestamp_hash(to_timestamp(t)::timestamp))%40
FROM generate_series(1, 100, 1) t;

CREATE TABLE temp AS
SELECT * FROM conditions;
INSERT INTO measurements
SELECT * FROM temp;

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
SELECT time_bucket(BIGINT '10', time) AS bucket, device, avg(temp) AS avg_temp
FROM conditions
GROUP BY 1,2 WITH NO DATA;

CREATE MATERIALIZED VIEW cond_20
WITH (timescaledb.continuous,
      timescaledb.materialized_only=true)
AS
SELECT time_bucket(BIGINT '20', time) AS bucket, device, avg(temp) AS avg_temp
FROM conditions
GROUP BY 1,2 WITH NO DATA;

CREATE MATERIALIZED VIEW measure_10
WITH (timescaledb.continuous,
      timescaledb.materialized_only=true)
AS
SELECT time_bucket(10, time) AS bucket, device, avg(temp) AS avg_temp
FROM measurements
GROUP BY 1,2 WITH NO DATA;

-- There should be three continuous aggregates, two on one hypertable
-- and one on the other:
SELECT mat_hypertable_id, raw_hypertable_id, user_view_name
FROM _timescaledb_catalog.continuous_agg;

-- The continuous aggregates should be empty
SELECT * FROM cond_10
ORDER BY 1 DESC, 2;

SELECT * FROM cond_20
ORDER BY 1 DESC, 2;

SELECT * FROM measure_10
ORDER BY 1 DESC, 2;

\if :IS_DISTRIBUTED
CREATE OR REPLACE FUNCTION get_hyper_invals() RETURNS TABLE(
      "hyper_id" INT,
      "start" BIGINT,
      "end" BIGINT
)
LANGUAGE SQL VOLATILE AS
$$
SELECT DISTINCT table_record[1]::TEXT::INT, table_record[2]::TEXT::BIGINT, table_record[3]::TEXT::BIGINT FROM test.remote_exec_get_result_strings(NULL, $DIST$
      SELECT hypertable_id,
            lowest_modified_value,
            greatest_modified_value
            FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
$DIST$)
ORDER BY 1,2,3
$$;

CREATE OR REPLACE FUNCTION get_cagg_invals() RETURNS TABLE(
      "cagg_id" INT,
      "start" BIGINT,
      "end" BIGINT
)
LANGUAGE SQL VOLATILE AS
$$
SELECT DISTINCT table_record[1]::TEXT::INT, table_record[2]::TEXT::BIGINT, table_record[3]::TEXT::BIGINT FROM test.remote_exec_get_result_strings(NULL, $DIST$
      SELECT materialization_id AS cagg_id,
            lowest_modified_value AS start,
            greatest_modified_value AS end
            FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
$DIST$)
ORDER BY 1,2,3
$$;
\else
CREATE OR REPLACE FUNCTION get_hyper_invals() RETURNS  TABLE (
      "hyper_id" INT,
      "start" BIGINT,
      "end" BIGINT
)
LANGUAGE SQL VOLATILE AS
$$
SELECT hypertable_id,
       lowest_modified_value,
       greatest_modified_value
       FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
       ORDER BY 1,2,3
$$;

CREATE OR REPLACE FUNCTION get_cagg_invals() RETURNS TABLE (
      "cagg_id" INT,
      "start" BIGINT,
      "end" BIGINT
)
LANGUAGE SQL VOLATILE AS
$$
SELECT materialization_id,
       lowest_modified_value,
       greatest_modified_value
       FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
       ORDER BY 1,2,3
$$;
\endif

CREATE VIEW hyper_invals AS SELECT * FROM get_hyper_invals();
CREATE VIEW cagg_invals AS SELECT * FROM get_cagg_invals();

-- Must refresh to move the invalidation threshold, or no
-- invalidations will be generated. Initially, threshold is the
-- MIN of the time dimension data type:
SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
ORDER BY 1,2;

-- There should be only "infinite" invalidations in the cagg
-- invalidation log:
SELECT * FROM cagg_invals;

-- Now refresh up to 50 without the first bucket, and the threshold should be updated accordingly:
CALL refresh_continuous_aggregate('cond_10', 1, 50);
SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
ORDER BY 1,2;

-- Invalidations should be cleared inside the refresh window:
SELECT * FROM cagg_invals;

-- Refresh up to 50 from the beginning
CALL refresh_continuous_aggregate('cond_10', 0, 50);
SELECT * FROM cagg_invals;

-- Refreshing below the threshold does not move it:
CALL refresh_continuous_aggregate('cond_10', 20, 49);
SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
ORDER BY 1,2;

-- Nothing changes with invalidations either since the region was
-- already refreshed and no new invalidations have been generated:
SELECT * FROM cagg_invals;

-- Refreshing measure_10 moves the threshold only for the other hypertable:
CALL refresh_continuous_aggregate('measure_10', 0, 30);
SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
ORDER BY 1,2;
SELECT * FROM cagg_invals;

-- Refresh on the second continuous aggregate, cond_20, on the first
-- hypertable moves the same threshold as when refreshing cond_10:
CALL refresh_continuous_aggregate('cond_20', 60, 100);
SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
ORDER BY 1,2;
SELECT * FROM cagg_invals;

-- There should be no hypertable invalidations initially:
SELECT * FROM hyper_invals;
SELECT * FROM cagg_invals;

-- Create invalidations across different ranges. Some of these should
-- be deleted and others cut in different ways when a refresh is
-- run. Note that the refresh window is inclusive in the start of the
-- window but exclusive at the end.

-- Entries that should be left unmodified:
INSERT INTO conditions VALUES (10, 4, 23.7);
INSERT INTO conditions VALUES (10, 5, 23.8), (19, 3, 23.6);
INSERT INTO conditions VALUES (60, 3, 23.7), (70, 4, 23.7);

-- Should see some invaliations in the hypertable invalidation log:
SELECT * FROM hyper_invals;

-- Generate some invalidations for the other hypertable
INSERT INTO measurements VALUES (20, 4, 23.7);
INSERT INTO measurements VALUES (30, 5, 23.8), (80, 3, 23.6);

-- Should now see invalidations for both hypertables
SELECT * FROM hyper_invals;

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
SELECT * FROM hyper_invals;
SELECT * FROM cagg_invals;

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
SELECT * FROM hyper_invals;

-- But nothing has yet changed in the cagg invalidation log:
SELECT * FROM cagg_invals;

-- Refresh to process invalidations for daily temperature:
CALL refresh_continuous_aggregate('cond_10', 20, 60);

-- Invalidations should be moved from the hypertable invalidation log
-- to the continuous aggregate log.
SELECT * FROM hyper_invals;

-- Only the cond_10 cagg should have its entries cut:
SELECT * FROM cagg_invals;

-- Refresh also cond_20:
CALL refresh_continuous_aggregate('cond_20', 20, 60);

-- The cond_20 cagg should also have its entries cut:
SELECT * FROM cagg_invals;

-- Refresh cond_10 to completely remove an invalidation:
CALL refresh_continuous_aggregate('cond_10', 0, 20);

-- The 1-19 invalidation should be deleted:
SELECT * FROM cagg_invals;

-- Clear everything between 0 and 100 to make way for new
-- invalidations
CALL refresh_continuous_aggregate('cond_10', 0, 100);

-- Test refreshing with non-overlapping invalidations
INSERT INTO conditions VALUES (20, 1, 23.4), (25, 1, 23.4);
INSERT INTO conditions VALUES (30, 1, 23.4), (46, 1, 23.4);

CALL refresh_continuous_aggregate('cond_10', 1, 40);

SELECT * FROM cagg_invals;

-- Refresh whithout cutting (in area where there are no
-- invalidations). Merging of overlapping entries should still happen:
INSERT INTO conditions VALUES (15, 1, 23.4), (42, 1, 23.4);

CALL refresh_continuous_aggregate('cond_10', 90, 100);

SELECT * FROM cagg_invals;

-- Test max refresh window
CALL refresh_continuous_aggregate('cond_10', NULL, NULL);

SELECT * FROM cagg_invals;
SELECT * FROM hyper_invals;

-- Pick the first chunk of conditions to TRUNCATE
SELECT show_chunks AS chunk_to_truncate
FROM show_chunks('conditions')
ORDER BY 1
LIMIT 1 \gset

-- Show the data before truncating one of the chunks
SELECT * FROM :chunk_to_truncate
ORDER BY 1;

-- Truncate one chunk
\if :IS_DISTRIBUTED
-- There is no TRUNCATE implementation for FOREIGN tables yet
\set ON_ERROR_STOP 0
\endif
TRUNCATE TABLE :chunk_to_truncate;
\if :IS_DISTRIBUTED
\set ON_ERROR_STOP 1
\endif

-- Should see new invalidation entries for conditions for the non-distributed case
SELECT * FROM hyper_invals;

-- TRUNCATE the hypertable to invalidate all its continuous aggregates
TRUNCATE conditions;

-- Now empty
SELECT * FROM conditions;

-- Should see an infinite invalidation entry for conditions
SELECT * FROM hyper_invals;

-- Aggregates still hold data
SELECT * FROM cond_10
ORDER BY 1,2
LIMIT 5;

SELECT * FROM cond_20
ORDER BY 1,2
LIMIT 5;

CALL refresh_continuous_aggregate('cond_10', NULL, NULL);
CALL refresh_continuous_aggregate('cond_20', NULL, NULL);

-- Both should now be empty after refresh
SELECT * FROM cond_10
ORDER BY 1,2;

SELECT * FROM cond_20
ORDER BY 1,2;

-- Insert new data again and refresh
INSERT INTO conditions VALUES
       (1, 1, 23.4), (4, 3, 14.3), (5, 1, 13.6),
       (6, 2, 17.9), (12, 1, 18.3), (19, 3, 28.2),
       (10, 3, 22.3), (11, 2, 34.9), (15, 2, 45.6),
       (21, 1, 15.3), (22, 2, 12.3), (29, 3, 16.3);

CALL refresh_continuous_aggregate('cond_10', NULL, NULL);
CALL refresh_continuous_aggregate('cond_20', NULL, NULL);

-- Should now hold data again
SELECT * FROM cond_10
ORDER BY 1,2;

SELECT * FROM cond_20
ORDER BY 1,2;

-- Truncate one of the aggregates, but first test that we block
-- TRUNCATE ONLY
\set ON_ERROR_STOP 0
TRUNCATE ONLY cond_20;
\set ON_ERROR_STOP 1
TRUNCATE cond_20;

-- Should now be empty
SELECT * FROM cond_20
ORDER BY 1,2;

-- Other aggregate is not affected
SELECT * FROM cond_10
ORDER BY 1,2;

-- Refresh again to bring data back
CALL refresh_continuous_aggregate('cond_20', NULL, NULL);

-- The aggregate should be populated again
SELECT * FROM cond_20
ORDER BY 1,2;

-------------------------------------------------------
-- Test corner cases against a minimal bucket aggregate
-------------------------------------------------------

-- First, clear the table and aggregate
TRUNCATE conditions;
SELECT * FROM conditions;

CALL refresh_continuous_aggregate('cond_10', NULL, NULL);

SELECT * FROM cond_10
ORDER BY 1,2;

CREATE MATERIALIZED VIEW cond_1
WITH (timescaledb.continuous,
      timescaledb.materialized_only=true)
AS
SELECT time_bucket(BIGINT '1', time) AS bucket, device, avg(temp) AS avg_temp
FROM conditions
GROUP BY 1,2 WITH NO DATA;

SELECT mat_hypertable_id AS cond_1_id
FROM _timescaledb_catalog.continuous_agg
WHERE user_view_name = 'cond_1' \gset

-- Test manual invalidation error
\if :IS_DISTRIBUTED
\else
\set ON_ERROR_STOP 0
SELECT _timescaledb_internal.invalidation_cagg_log_add_entry(:cond_1_id, 1, 0);
\set ON_ERROR_STOP 1
\endif

-- Test invalidations with bucket size 1
INSERT INTO conditions VALUES (0, 1, 1.0);

SELECT * FROM hyper_invals;

-- Refreshing around the bucket should not update the aggregate
CALL refresh_continuous_aggregate('cond_1', -1, 0);
SELECT * FROM cond_1
ORDER BY 1,2;
CALL refresh_continuous_aggregate('cond_1', 1, 2);
SELECT * FROM cond_1
ORDER BY 1,2;

-- Refresh only the invalidated bucket
CALL refresh_continuous_aggregate('cond_1', 0, 1);

SELECT * FROM cagg_invals
WHERE cagg_id = :cond_1_id;

SELECT * FROM cond_1
ORDER BY 1,2;

-- Refresh 1 extra bucket on the left
INSERT INTO conditions VALUES (0, 1, 2.0);
CALL refresh_continuous_aggregate('cond_1', -1, 1);
SELECT * FROM cond_1
ORDER BY 1,2;

-- Refresh 1 extra bucket on the right
INSERT INTO conditions VALUES (0, 1, 3.0);
CALL refresh_continuous_aggregate('cond_1', 0, 2);
SELECT * FROM cond_1
ORDER BY 1,2;

-- Refresh 1 extra bucket on each side
INSERT INTO conditions VALUES (0, 1, 4.0);
CALL refresh_continuous_aggregate('cond_1', -1, 2);
SELECT * FROM cond_1
ORDER BY 1,2;

-- Clear to reset aggregate
TRUNCATE conditions;
CALL refresh_continuous_aggregate('cond_1', NULL, NULL);

-- Test invalidation of size 2
INSERT INTO conditions VALUES (0, 1, 1.0), (1, 1, 2.0);

-- Refresh one bucket at a time
CALL refresh_continuous_aggregate('cond_1', 0, 1);
SELECT * FROM cond_1
ORDER BY 1,2;

CALL refresh_continuous_aggregate('cond_1', 1, 2);
SELECT * FROM cond_1
ORDER BY 1,2;

-- Repeat the same thing but refresh the whole invalidation at once
TRUNCATE conditions;
CALL refresh_continuous_aggregate('cond_1', NULL, NULL);

INSERT INTO conditions VALUES (0, 1, 1.0), (1, 1, 2.0);
CALL refresh_continuous_aggregate('cond_1', 0, 2);
SELECT * FROM cond_1
ORDER BY 1,2;

-- Test invalidation of size 3
TRUNCATE conditions;
CALL refresh_continuous_aggregate('cond_1', NULL, NULL);

INSERT INTO conditions VALUES (0, 1, 1.0), (1, 1, 2.0), (2, 1, 3.0);

-- Invalidation extends beyond the refresh window on both ends
CALL refresh_continuous_aggregate('cond_1', 1, 2);
SELECT * FROM cond_1
ORDER BY 1,2;

-- Should leave one invalidation on each side of the refresh window
SELECT * FROM cagg_invals
WHERE cagg_id = :cond_1_id;

-- Refresh the two remaining invalidations
CALL refresh_continuous_aggregate('cond_1', 0, 1);
SELECT * FROM cond_1
ORDER BY 1,2;

CALL refresh_continuous_aggregate('cond_1', 2, 3);
SELECT * FROM cond_1
ORDER BY 1,2;

-- Clear and repeat but instead refresh the whole range in one go. The
-- result should be the same as the three partial refreshes. Use
-- DELETE instead of TRUNCATE to clear this time.
DELETE FROM conditions;
CALL refresh_continuous_aggregate('cond_1', NULL, NULL);
INSERT INTO conditions VALUES (0, 1, 1.0), (1, 1, 2.0), (2, 1, 3.0);

CALL refresh_continuous_aggregate('cond_1', 0, 3);
SELECT * FROM cond_1
ORDER BY 1,2;

----------------------------------------------
-- Test that invalidation threshold is capped
----------------------------------------------
CREATE table threshold_test (time int, value int);
\if :IS_DISTRIBUTED
SELECT create_distributed_hypertable('threshold_test', 'time', chunk_time_interval => 4, replication_factor => 2);
\else
SELECT create_hypertable('threshold_test', 'time', chunk_time_interval => 4);
\endif
SELECT set_integer_now_func('threshold_test', 'int_now');

CREATE MATERIALIZED VIEW thresh_2
WITH (timescaledb.continuous,
      timescaledb.materialized_only=true)
AS
SELECT time_bucket(2, time) AS bucket, max(value) AS max
FROM threshold_test
GROUP BY 1 WITH NO DATA;

SELECT raw_hypertable_id AS thresh_hyper_id, mat_hypertable_id AS thresh_cagg_id
FROM _timescaledb_catalog.continuous_agg
WHERE user_view_name = 'thresh_2' \gset

-- There's no invalidation threshold initially
SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
WHERE hypertable_id = :thresh_hyper_id
ORDER BY 1,2;

-- Test manual invalidation error
\if :IS_DISTRIBUTED
\else
\set ON_ERROR_STOP 0
SELECT _timescaledb_internal.invalidation_hyper_log_add_entry(:thresh_hyper_id, 1, 0);
\set ON_ERROR_STOP 1
\endif

-- Test that threshold is initilized to min value when there's no data
-- and we specify an infinite end. Note that the min value may differ
-- depending on time type.
CALL refresh_continuous_aggregate('thresh_2', 0, NULL);

SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
WHERE hypertable_id = :thresh_hyper_id
ORDER BY 1,2;

INSERT INTO threshold_test
SELECT v, v FROM generate_series(1, 10) v;

CALL refresh_continuous_aggregate('thresh_2', 0, 5);

-- Threshold should move to end of the last refreshed bucket, which is
-- the last bucket fully included in the window, i.e., the window
-- shrinks to end of previous bucket.
SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
WHERE hypertable_id = :thresh_hyper_id
ORDER BY 1,2;

-- Refresh where both the start and end of the window is above the
-- max data value
CALL refresh_continuous_aggregate('thresh_2', 14, NULL);

SELECT watermark AS thresh_hyper_id_watermark
FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
WHERE hypertable_id = :thresh_hyper_id \gset

-- Refresh where we start from the current watermark to infinity
CALL refresh_continuous_aggregate('thresh_2', :thresh_hyper_id_watermark, NULL);

-- Now refresh with max end of the window to test that the
-- invalidation threshold is capped at the last bucket of data
CALL refresh_continuous_aggregate('thresh_2', 0, NULL);

SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
WHERE hypertable_id = :thresh_hyper_id
ORDER BY 1,2;

-- Should not have processed invalidations beyond the invalidation
-- threshold.
SELECT * FROM cagg_invals
WHERE cagg_id = :thresh_cagg_id;

-- Check that things are properly materialized
SELECT * FROM thresh_2
ORDER BY 1;

-- Delete the last data
SELECT show_chunks AS chunk_to_drop
FROM show_chunks('threshold_test')
ORDER BY 1 DESC
LIMIT 1 \gset

DELETE FROM threshold_test
WHERE time > 6;

-- The last data in the hypertable is gone
SELECT time_bucket(2, time) AS bucket, max(value) AS max
FROM threshold_test
GROUP BY 1
ORDER BY 1;

-- The aggregate still holds data
SELECT * FROM thresh_2
ORDER BY 1;

-- Refresh the aggregate to bring it up-to-date
CALL refresh_continuous_aggregate('thresh_2', 0, NULL);

-- Data also gone from the aggregate
SELECT * FROM thresh_2
ORDER BY 1;

-- The invalidation threshold remains the same
SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
WHERE hypertable_id = :thresh_hyper_id
ORDER BY 1,2;

-- Insert new data beyond the invalidation threshold to move it
-- forward
INSERT INTO threshold_test
SELECT v, v FROM generate_series(7, 15) v;

CALL refresh_continuous_aggregate('thresh_2', 0, NULL);

-- Aggregate now updated to reflect newly aggregated data
SELECT * FROM thresh_2
ORDER BY 1;

-- The invalidation threshold should have moved forward to the end of
-- the new data
SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
WHERE hypertable_id = :thresh_hyper_id
ORDER BY 1,2;

-- The aggregate remains invalid beyond the invalidation threshold
SELECT * FROM cagg_invals
WHERE cagg_id = :thresh_cagg_id;

----------------------------------------------------------------------
-- Test that dropping a chunk invalidates the dropped region. First
-- create another chunk so that we have two chunks. One of the chunks
-- will be dropped.
---------------------------------------------------------------------
INSERT INTO conditions VALUES (10, 1, 10.0);

-- Chunks currently associated with the hypertable
SELECT show_chunks AS chunk_to_drop
FROM show_chunks('conditions');

-- Pick the first one to drop
SELECT show_chunks AS chunk_to_drop
FROM show_chunks('conditions')
ORDER BY 1
LIMIT 1 \gset

-- Show the data before dropping one of the chunks
SELECT * FROM conditions
ORDER BY 1,2;

-- Drop one chunk
\if :IS_DISTRIBUTED
CALL distributed_exec(format('DROP TABLE IF EXISTS %s', :'chunk_to_drop'));
DROP FOREIGN TABLE :chunk_to_drop;
\else
DROP TABLE :chunk_to_drop;
\endif

-- The chunk's data no longer exists in the hypertable
SELECT * FROM conditions
ORDER BY 1,2;

-- Aggregate still remains in continuous aggregate, however
SELECT * FROM cond_1
ORDER BY 1,2;

-- Refresh the continuous aggregate to make the dropped data be
-- reflected in the aggregate
CALL refresh_continuous_aggregate('cond_1', NULL, NULL);

-- Aggregate now up-to-date with the source hypertable
SELECT * FROM cond_1
ORDER BY 1,2;

-- Test that adjacent invalidations are merged
INSERT INTO conditions VALUES(1, 1, 1.0), (2, 1, 2.0);
INSERT INTO conditions VALUES(3, 1, 1.0);
INSERT INTO conditions VALUES(4, 1, 1.0);
INSERT INTO conditions VALUES(6, 1, 1.0);

CALL refresh_continuous_aggregate('cond_1', 10, NULL);
SELECT * FROM cagg_invals
WHERE cagg_id = :cond_1_id;

---------------------------------------------------------------------
-- Test that single timestamp invalidations are expanded to buckets,
-- and adjacent buckets merged. This merging cannot cross Data-Node
-- chunk boundaries for the distributed hypertable case.
---------------------------------------------------------------------
-- First clear invalidations in a range:
CALL refresh_continuous_aggregate('cond_10', -20, 60);

-- The following three should be merged to one range 0-29
INSERT INTO conditions VALUES (5, 1, 1.0);
INSERT INTO conditions VALUES (15, 1, 1.0);
INSERT INTO conditions VALUES (25, 1, 1.0);
-- The last one should not merge with the others
INSERT INTO conditions VALUES (40, 1, 1.0);

-- Refresh to process invalidations, but outside the range of
-- invalidations we inserted so that we don't clear them.
CALL refresh_continuous_aggregate('cond_10', 50, 60);

SELECT mat_hypertable_id AS cond_10_id
FROM _timescaledb_catalog.continuous_agg
WHERE user_view_name = 'cond_10' \gset

SELECT * FROM cagg_invals
WHERE cagg_id = :cond_10_id;

-- should trigger two individual refreshes
CALL refresh_continuous_aggregate('cond_10', 0, 200);

-- Allow at most 5 individual invalidations per refreshe
SET timescaledb.materializations_per_refresh_window=5;

-- Insert into every second bucket
INSERT INTO conditions VALUES (20, 1, 1.0);
INSERT INTO conditions VALUES (40, 1, 1.0);
INSERT INTO conditions VALUES (60, 1, 1.0);
INSERT INTO conditions VALUES (80, 1, 1.0);
INSERT INTO conditions VALUES (100, 1, 1.0);
INSERT INTO conditions VALUES (120, 1, 1.0);
INSERT INTO conditions VALUES (140, 1, 1.0);

CALL refresh_continuous_aggregate('cond_10', 0, 200);

\set VERBOSITY default
-- Test acceptable values for materializations per refresh
SET timescaledb.materializations_per_refresh_window=' 5 ';
INSERT INTO conditions VALUES (140, 1, 1.0);
CALL refresh_continuous_aggregate('cond_10', 0, 200);
-- Large value will be treated as LONG_MAX
SET timescaledb.materializations_per_refresh_window=342239897234023842394249234766923492347;
INSERT INTO conditions VALUES (140, 1, 1.0);
CALL refresh_continuous_aggregate('cond_10', 0, 200);

-- Test bad values for materializations per refresh
SET timescaledb.materializations_per_refresh_window='foo';
INSERT INTO conditions VALUES (140, 1, 1.0);
CALL refresh_continuous_aggregate('cond_10', 0, 200);
SET timescaledb.materializations_per_refresh_window='2bar';
INSERT INTO conditions VALUES (140, 1, 1.0);
CALL refresh_continuous_aggregate('cond_10', 0, 200);

SET timescaledb.materializations_per_refresh_window='-';
INSERT INTO conditions VALUES (140, 1, 1.0);
CALL refresh_continuous_aggregate('cond_10', 0, 200);
\set VERBOSITY terse
