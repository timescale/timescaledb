-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Tests for add_continuous_aggregate_column function

SET ROLE :ROLE_DEFAULT_PERM_USER;

-- Create test hypertable
CREATE TABLE test_ht (
    time TIMESTAMPTZ NOT NULL,
    device_id INTEGER NOT NULL,
    value DOUBLE PRECISION,
    extra TEXT,
    location TEXT
);

SELECT create_hypertable('test_ht', 'time');

-- Insert some test data
INSERT INTO test_ht VALUES
    ('2020-01-01 00:00:00', 1, 10.0, 'extra1', 'loc1'),
    ('2020-01-01 01:00:00', 1, 20.0, 'extra2', 'loc1'),
    ('2020-01-01 02:00:00', 2, 30.0, 'extra3', 'loc2'),
    ('2020-01-02 00:00:00', 1, 15.0, 'extra4', 'loc1'),
    ('2020-01-02 01:00:00', 2, 25.0, 'extra5', 'loc2');

-- Create a simple continuous aggregate (materialized-only mode)
CREATE MATERIALIZED VIEW test_cagg_mat_only
    WITH (timescaledb.continuous, timescaledb.materialized_only=true)
AS SELECT
    time_bucket('1 day', time) AS bucket,
    device_id,
    avg(value) AS avg_value
FROM test_ht
GROUP BY 1, 2
WITH NO DATA;

-- Create a real-time continuous aggregate
CREATE MATERIALIZED VIEW test_cagg_realtime
    WITH (timescaledb.continuous, timescaledb.materialized_only=false)
AS SELECT
    time_bucket('1 day', time) AS bucket,
    device_id,
    avg(value) AS avg_value
FROM test_ht
GROUP BY 1, 2
WITH NO DATA;

-- Show initial state
SELECT user_view_name, partial_view_name, direct_view_name
FROM _timescaledb_catalog.continuous_agg
WHERE user_view_name IN ('test_cagg_mat_only', 'test_cagg_realtime')
ORDER BY user_view_name;

-- Test 1: Add column to materialized-only CA
SELECT add_continuous_aggregate_column('test_cagg_mat_only', 'extra');

-- Verify the column was added
\d+ test_cagg_mat_only

-- Test 2: Add column to real-time CA
SELECT add_continuous_aggregate_column('test_cagg_realtime', 'location');

-- Verify the column was added
\d+ test_cagg_realtime

-- Test 3: Error - column doesn't exist
\set ON_ERROR_STOP 0
SELECT add_continuous_aggregate_column('test_cagg_mat_only', 'nonexistent_column');
\set ON_ERROR_STOP 1

-- Test 4: Error - column already exists in CA
\set ON_ERROR_STOP 0
SELECT add_continuous_aggregate_column('test_cagg_mat_only', 'device_id');
\set ON_ERROR_STOP 1

-- Test 5: Error - column already added
\set ON_ERROR_STOP 0
SELECT add_continuous_aggregate_column('test_cagg_mat_only', 'extra');
\set ON_ERROR_STOP 1

-- Test 6: if_not_exists=true should not error
SELECT add_continuous_aggregate_column('test_cagg_mat_only', 'extra', true);
SELECT add_continuous_aggregate_column('test_cagg_mat_only', 'device_id', true);

-- Test 7: Refresh and verify data
CALL refresh_continuous_aggregate('test_cagg_mat_only', NULL, NULL);
CALL refresh_continuous_aggregate('test_cagg_realtime', NULL, NULL);

-- Check data in materialized-only CA
SELECT * FROM test_cagg_mat_only ORDER BY bucket, device_id;

-- Check data in real-time CA
SELECT * FROM test_cagg_realtime ORDER BY bucket, device_id;

-- Test 8: Add another column and refresh again
SELECT add_continuous_aggregate_column('test_cagg_mat_only', 'location');

-- Insert new data
INSERT INTO test_ht VALUES
    ('2020-01-03 00:00:00', 1, 35.0, 'extra6', 'loc3'),
    ('2020-01-03 01:00:00', 2, 45.0, 'extra7', 'loc3');

-- Refresh
CALL refresh_continuous_aggregate('test_cagg_mat_only', NULL, NULL);

-- New data should have the location column populated
SELECT * FROM test_cagg_mat_only ORDER BY bucket, device_id;

-- Test 9: Error - not a continuous aggregate
CREATE TABLE regular_table (id INT, data TEXT);
\set ON_ERROR_STOP 0
SELECT add_continuous_aggregate_column('regular_table', 'data');
\set ON_ERROR_STOP 1
DROP TABLE regular_table;

-- Cleanup
DROP MATERIALIZED VIEW test_cagg_mat_only;
DROP MATERIALIZED VIEW test_cagg_realtime;
DROP TABLE test_ht;
