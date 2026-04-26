-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test backfill tracking for continuous aggregates.
-- Uses current_timestamp_mock to control "now" for deterministic results.

\c :TEST_DBNAME :ROLE_SUPERUSER
SET timescaledb.current_timestamp_mock = '2024-01-15 00:00:00+00';

-- Convenience view: hypertable-level invalidation log with human-readable
-- timestamps and the owning hypertable's table_name. Used by the tests below
-- to assert that backfill inserts do not emit coarse hyper-log entries.
CREATE VIEW hyper_invalidation_log_view AS
SELECT ht.table_name,
       _timescaledb_functions.to_timestamp(il.lowest_modified_value) AT TIME ZONE 'UTC' AS start,
       _timescaledb_functions.to_timestamp(il.greatest_modified_value) AT TIME ZONE 'UTC' AS "end"
FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log il
JOIN _timescaledb_catalog.hypertable ht ON ht.id = il.hypertable_id;

-- Convenience view: per-device backfill tracker with the owning hypertable's
-- table_name and UTC-formatted timestamps.
CREATE VIEW backfill_tracker_view AS
SELECT ht.table_name,
       bt.device_value,
       _timescaledb_functions.to_timestamp(bt.lowest_modified_value) AT TIME ZONE 'UTC' AS start,
       _timescaledb_functions.to_timestamp(bt.greatest_modified_value) AT TIME ZONE 'UTC' AS "end"
FROM _timescaledb_catalog.continuous_aggs_backfill_tracker bt
JOIN _timescaledb_catalog.hypertable ht ON ht.id = bt.hypertable_id;

-- Setup: hypertable with 6-hour chunk interval
CREATE TABLE metrics(
    ts TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    sensor_id UUID,
    temp INTEGER
);

SELECT create_hypertable('metrics', 'ts', chunk_time_interval => interval '6 hours');

-- Seed a current-time row so WITH DATA below has something to materialize
-- and inserts a real continuous_aggs_invalidation_threshold row.
INSERT INTO metrics VALUES ('2024-01-15 00:00:00+00', 99, '11111111-1111-1111-1111-111111111111', 0);

-- Create a cagg
CREATE MATERIALIZED VIEW metrics_hourly
WITH (timescaledb.continuous) AS
SELECT device_id, time_bucket('1 hour', ts) AS bucket, avg(temp) AS avg_temp
FROM metrics
GROUP BY device_id, bucket
WITH DATA;

-- Set tenant column
ALTER MATERIALIZED VIEW metrics_hourly SET (timescaledb.tenant_column = 'device_id');

-- Verify catalog entry
SELECT tenant_column_name
FROM _timescaledb_catalog.continuous_agg
WHERE user_view_name = 'metrics_hourly';

-- Test 1: Current inserts produce no tracker rows
-- "Now" is 2024-01-15 00:00, so data within last day is current
INSERT INTO metrics VALUES
    ('2024-01-14 20:00:00+00', 1, '11111111-1111-1111-1111-111111111111', 25),
    ('2024-01-14 21:00:00+00', 2, '11111111-1111-1111-1111-111111111111', 30),
    ('2024-01-14 22:00:00+00', 1, '11111111-1111-1111-1111-111111111111', 28);

SELECT * FROM backfill_tracker_view ORDER BY device_value;
-- Invariant: new hyper-log entries for 'metrics'
SELECT * FROM hyper_invalidation_log_view WHERE table_name = 'metrics' ORDER BY start;

-- Test 2: Backfill inserts produce tracker rows
-- Data from 7 days ago is definitely below the watermark (now - max(6h, 1day))
INSERT INTO metrics VALUES
    ('2024-01-08 10:00:00+00', 1, '11111111-1111-1111-1111-111111111111', 20),
    ('2024-01-08 11:00:00+00', 2, '11111111-1111-1111-1111-111111111111', 22);

SELECT * FROM backfill_tracker_view WHERE device_value = '1' or device_value = '2'
ORDER BY device_value;

-- Verify we got rows for both devices
SELECT count(*) AS tracker_count FROM backfill_tracker_view;
-- Invariant: tracker captured the backfill,  no new hyper-log entries
SELECT * FROM hyper_invalidation_log_view WHERE table_name = 'metrics' ORDER BY start;

-- Test 3: Min/max tracking across multiple inserts in same transaction
BEGIN;
INSERT INTO metrics VALUES
    ('2024-01-05 08:00:00+00', 3, '11111111-1111-1111-1111-111111111111', 15),
    ('2024-01-07 16:00:00+00', 3, '11111111-1111-1111-1111-111111111111', 18),
    ('2024-01-06 12:00:00+00', 3, '11111111-1111-1111-1111-111111111111', 17);
COMMIT;

-- Device 3 should have min from Jan 5 and max from Jan 7
SELECT * FROM backfill_tracker_view WHERE device_value = '3';
-- Invariant: tracker captured the backfill, no new entries
SELECT * FROM hyper_invalidation_log_view WHERE table_name = 'metrics' ORDER BY start;

-- Test 4: COPY path
\copy metrics FROM STDIN WITH (FORMAT csv)
2024-01-03 10:00:00+00,4,,40
2024-01-03 14:00:00+00,4,,42
\.

SELECT * FROM backfill_tracker_view WHERE device_value = '4';
-- Invariant: tracker captured the backfill, no new entries
SELECT * FROM hyper_invalidation_log_view WHERE table_name = 'metrics' ORDER BY start;

-- Test 5: No tracking when tenant_column not set
CREATE TABLE metrics_no_tenant(
    ts TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    sensor_id UUID,
    temp INTEGER
);

SELECT create_hypertable('metrics_no_tenant', 'ts', chunk_time_interval => interval '6 hours');


CREATE MATERIALIZED VIEW metrics_no_tenant_hourly
WITH (timescaledb.continuous) AS
SELECT device_id, time_bucket('1 hour', ts) AS bucket, avg(temp) AS avg_temp
FROM metrics_no_tenant
GROUP BY device_id, bucket
WITH DATA;

-- No tenant_column set — insert backfill data
INSERT INTO metrics_no_tenant VALUES
    ('2024-01-08 10:00:00+00', 100, '11111111-1111-1111-1111-111111111111', 50);

-- Should have no tracker rows for metrics_no_tenant's hypertable
-- (only rows from metrics inserts above)
SELECT count(*) AS tracker_count_for_no_tenant
FROM backfill_tracker_view
WHERE table_name = 'metrics_no_tenant';
-- No refresh has run so it should still be empty today.
SELECT * FROM hyper_invalidation_log_view WHERE table_name = 'metrics_no_tenant' ORDER BY start;

-- Test 6: Sibling cagg with same tenant column succeeds
CREATE MATERIALIZED VIEW metrics_daily
WITH (timescaledb.continuous) AS
SELECT device_id, time_bucket('1 day', ts) AS bucket, avg(temp) AS avg_temp
FROM metrics
GROUP BY device_id, bucket
WITH DATA;

ALTER MATERIALIZED VIEW metrics_daily SET (timescaledb.tenant_column = 'device_id');

SELECT user_view_name, tenant_column_name
FROM _timescaledb_catalog.continuous_agg
WHERE user_view_name IN ('metrics_hourly', 'metrics_daily')
ORDER BY user_view_name;

-- Test 7:  insert has mix of backfill and live data. should see entries in
-- tracker and inv. log table
--clean up backfill_tracker first
TRUNCATE TABLE _timescaledb_catalog.continuous_aggs_backfill_tracker;
CALL refresh_continuous_aggregate('metrics_hourly', '2024-01-10 00:00+00', '2024-01-15 00:00+00');
INSERT INTO metrics VALUES ('2024-01-02 23:00:00+00', 7, '11111111-1111-1111-1111-111111111111', 0),
                           ('2024-01-14 23:00:00+00', 7, '11111111-1111-1111-1111-111111111111', 0);
-- should contain the entry for 2024-01-02
SELECT * FROM backfill_tracker_view ORDER BY device_value;
-- should contain the entry for 2024-01-14 23:00
SELECT * FROM hyper_invalidation_log_view WHERE table_name = 'metrics' ORDER BY start;

-- Cleanup
DROP VIEW hyper_invalidation_log_view;
DROP VIEW backfill_tracker_view;
DROP TABLE metrics CASCADE;
DROP TABLE metrics_no_tenant CASCADE;
