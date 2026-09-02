-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Tests for late arrival tracking window. Verify tracked entries match config.

-- TEST 0: listing the tracker map before anything here is tracked.  Nothing in
-- this database has granular refresh enabled yet, so the map holds no entry for
-- it and the listing comes back empty
\c :TEST_DBNAME :ROLE_SUPERUSER
SELECT count(*) AS entries_for_this_database
FROM _timescaledb_functions.tenant_tracking_map()
WHERE database_id = (SELECT oid FROM pg_database WHERE datname = current_database());

SELECT count(*) >= 0 AS listing_the_whole_map_succeeds
FROM _timescaledb_functions.tenant_tracking_map();

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

SET timezone TO 'UTC';

-- Anchor the start offset to a fixed date safely before all the fixed 2020
-- fixture dates below, computed relative to today so the window keeps
-- covering them regardless of when this test actually runs.
SELECT (CURRENT_DATE - DATE '2019-01-01') AS granular_refresh_lookback_days,
       (CURRENT_DATE - DATE '2019-01-01')::text || ' days' AS granular_refresh_lookback \gset

CREATE TABLE conditions(time timestamptz NOT NULL, sensor_id text, value float);
SELECT create_hypertable('conditions', 'time');
ALTER TABLE conditions SET (
    timescaledb.granular_refresh_column = 'sensor_id',
    timescaledb.granular_refresh_start_offset = :'granular_refresh_lookback',
    timescaledb.granular_refresh_end_offset = '1 day'
);

CREATE MATERIALIZED VIEW cond_daily
  WITH (timescaledb.continuous) AS
  SELECT time_bucket('1 day', time) AS bucket, sensor_id, avg(value)
  FROM conditions
  GROUP BY bucket, sensor_id
  WITH NO DATA;
ALTER MATERIALIZED VIEW cond_daily SET (timescaledb.enable_granular_refresh = true);

-- TEST 1: the tracking window is computed and pinned on the tracker.
--
-- The first INSERT creates the tracker and seeds its window.  The window
-- width is deterministic (granular_refresh_start_offset -
-- granular_refresh_end_offset); exact bounds depend on now(), so we assert
-- only the width and that the window is non-empty.
INSERT INTO conditions VALUES ('2020-01-01 00:00+00', 'sensor_a', 1);

SELECT late_threshold_end - late_threshold_start =
         (EXTRACT(EPOCH FROM (:granular_refresh_lookback_days - 1) * INTERVAL '1 day') * 1000000)::int8
           AS window_width_matches,
       late_threshold_start < late_threshold_end                     AS window_nonempty
FROM _timescaledb_functions.hypertable_get_tenant_tracking_info('conditions');

DROP MATERIALIZED VIEW cond_daily;
DROP TABLE conditions;

-- TEST 1b: integer-time hypertables (smallint/int/bigint) use the integer_now()
-- watermark for the window, with the configured offsets interpreted as plain
-- integers.  metrics_now() is a constant 1000, and offsets are 900/100, so the
-- seeded window is exactly [100, 900).
CREATE TABLE metrics(t bigint NOT NULL, sensor_id text, value float);
SELECT create_hypertable('metrics', 't', chunk_time_interval => 100);
CREATE OR REPLACE FUNCTION metrics_now() RETURNS bigint LANGUAGE SQL STABLE AS
  $$ SELECT 1000::bigint $$;
SELECT set_integer_now_func('metrics', 'metrics_now');
ALTER TABLE metrics SET (
    timescaledb.granular_refresh_column = 'sensor_id',
    timescaledb.granular_refresh_start_offset = 900,
    timescaledb.granular_refresh_end_offset = 100
);

CREATE MATERIALIZED VIEW metrics_by_bucket
  WITH (timescaledb.continuous) AS
  SELECT time_bucket(100, t) AS bucket, sensor_id, avg(value)
  FROM metrics
  GROUP BY bucket, sensor_id
  WITH NO DATA;
ALTER MATERIALIZED VIEW metrics_by_bucket SET (timescaledb.enable_granular_refresh = true);

INSERT INTO metrics VALUES (100, 'sensor_a', 1);

SELECT late_threshold_start AS window_start,
       late_threshold_end   AS window_end
FROM _timescaledb_functions.hypertable_get_tenant_tracking_info('metrics');

DROP MATERIALIZED VIEW metrics_by_bucket;
DROP TABLE metrics;

-- TEST 2: only late-arriving data (inside the window) is tracked; recent data
-- (newer than now - 1 day) is gated out at the commit drain, so its tenant
-- never enters the tracker.
CREATE TABLE conditions(time timestamptz NOT NULL, sensor_id text, value float);
SELECT create_hypertable('conditions', 'time');
ALTER TABLE conditions SET (
    timescaledb.granular_refresh_column = 'sensor_id',
    timescaledb.granular_refresh_start_offset = :'granular_refresh_lookback',
    timescaledb.granular_refresh_end_offset = '1 day'
);

CREATE MATERIALIZED VIEW cond_daily
  WITH (timescaledb.continuous) AS
  SELECT time_bucket('1 day', time) AS bucket, sensor_id, avg(value)
  FROM conditions
  GROUP BY bucket, sensor_id
  WITH NO DATA;
ALTER MATERIALIZED VIEW cond_daily SET (timescaledb.enable_granular_refresh = true);

-- sensor_a / sensor_b: old (2020) => inside the late window => tracked.
-- sensor_recent: at now() => outside the window => NOT tracked.
INSERT INTO conditions VALUES
  ('2020-01-01 00:00+00', 'sensor_a', 1),
  ('2020-01-02 00:00+00', 'sensor_b', 2);
INSERT INTO conditions VALUES (now(), 'sensor_recent', 3);

-- Refresh only the recent (2025+) region: it has work to do (so the flush
-- runs and drains the tracker), but the 2020 tracking rows do not overlap the
-- refresh window, so they are persisted and NOT consumed -- letting us observe
-- exactly which tenants were tracked.
CALL refresh_continuous_aggregate('cond_daily', '2025-01-01 00:00+00', NULL);

-- Expect sensor_a and sensor_b (late), never sensor_recent (gated out).
SELECT tenant_id
FROM _timescaledb_catalog.continuous_aggs_tenant_tracking
WHERE hypertable_id = (
    SELECT raw_hypertable_id FROM _timescaledb_catalog.continuous_agg
    WHERE user_view_name = 'cond_daily')
  AND tenant_id IS NOT NULL
ORDER BY tenant_id;

DROP MATERIALIZED VIEW cond_daily;
DROP TABLE conditions;

-- TEST 3: the tracker map lists the hypertables present in the per-tenant
-- invalidation tracker.  The map is process-global, so restrict it to this
-- database; and trackers are never freed, so the hypertables dropped above are
-- still in the map by id -- joining the catalog leaves just the live ones.
CREATE TABLE conditions(time timestamptz NOT NULL, sensor_id text, value float);
SELECT create_hypertable('conditions', 'time');
ALTER TABLE conditions SET (
    timescaledb.granular_refresh_column = 'sensor_id',
    timescaledb.granular_refresh_start_offset = :'granular_refresh_lookback',
    timescaledb.granular_refresh_end_offset = '1 day'
);

CREATE MATERIALIZED VIEW cond_daily
  WITH (timescaledb.continuous) AS
  SELECT time_bucket('1 day', time) AS bucket, sensor_id, avg(value)
  FROM conditions
  GROUP BY bucket, sensor_id
  WITH NO DATA;
ALTER MATERIALIZED VIEW cond_daily SET (timescaledb.enable_granular_refresh = true);

-- The map spans databases, so listing it is superuser-only.
\set ON_ERROR_STOP 0
SELECT * FROM _timescaledb_functions.tenant_tracking_map();
\set ON_ERROR_STOP 1

\c :TEST_DBNAME :ROLE_SUPERUSER
-- No tracker for this hypertable until the first INSERT commits.
SELECT count(*) AS entries_before
FROM _timescaledb_functions.tenant_tracking_map() m
JOIN _timescaledb_catalog.hypertable h ON h.id = m.hypertable_id
WHERE m.database_id = (SELECT oid FROM pg_database WHERE datname = current_database());

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
INSERT INTO conditions VALUES ('2020-01-01 00:00+00', 'sensor_a', 1);

\c :TEST_DBNAME :ROLE_SUPERUSER
-- Now 'conditions' is in the map, with a tracker behind it.
SELECT h.table_name, m.is_tracked
FROM _timescaledb_functions.tenant_tracking_map() m
JOIN _timescaledb_catalog.hypertable h ON h.id = m.hypertable_id
WHERE m.database_id = (SELECT oid FROM pg_database WHERE datname = current_database())
ORDER BY h.table_name;

-- Entries for hypertables dropped earlier in this test outlive them.
SELECT count(*) > 0 AS dropped_hypertables_still_listed
FROM _timescaledb_functions.tenant_tracking_map() m
WHERE m.database_id = (SELECT oid FROM pg_database WHERE datname = current_database())
  AND NOT EXISTS (SELECT 1 FROM _timescaledb_catalog.hypertable h WHERE h.id = m.hypertable_id);

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
DROP MATERIALIZED VIEW cond_daily;
DROP TABLE conditions;

-- TEST 4: per-hypertable tracker state is restricted to the hypertable owner.
CREATE TABLE conditions(time timestamptz NOT NULL, sensor_id text, value float);
SELECT create_hypertable('conditions', 'time');

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2
\set ON_ERROR_STOP 0
SELECT seq_num FROM _timescaledb_functions.hypertable_get_tenant_tracking_info('conditions');
\set ON_ERROR_STOP 1

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
DROP TABLE conditions;

-- TEST 5: A DML transaction touching multiple tenant_ids can end up producing
-- an invalidation that spans across the granular refresh window, while
-- the trackings of a subset of the tenant_ids can fall completely outside
-- and the rest inside the window. As such, the invalidation should be splitted
-- into 2: one invalidation outside the window with seqnum = 0,
-- and one insidde with the current seqnum.
-- When refresh happens later, the data outside the window is refresh fully,
-- while those inside gets a granular refresh. There should be no missing
-- or stale data in the cagg after the refresh.
-- current_time is mocked so that now() is fixed,
-- so the window opens at W = 2025-01-07 12:00, with one tenant
-- on each side of it:
--
--        t1 = 10:00             W = 12:00          t2 = 14:00
--   ---------|-----------------[|-----------------|---------->
--        tenant 'x'         window opens      tenant 'y'
--   \______________ one chunk, one transaction _______________/
SET timezone = 'UTC';
SET timescaledb.current_timestamp_mock = '2025-01-10 12:00:00+00';

CREATE TABLE metrics(time timestamptz NOT NULL, tenant text NOT NULL, value float);

SELECT create_hypertable('metrics', 'time', chunk_time_interval => INTERVAL '30 days');
ALTER TABLE metrics SET (
    timescaledb.granular_refresh_column = 'tenant',
    timescaledb.granular_refresh_start_offset = '3 days',
    timescaledb.granular_refresh_end_offset = '1 hour'
);

INSERT INTO metrics VALUES ('2025-01-07 10:00:00+00', 'x', 10),
                           ('2025-01-07 14:00:00+00', 'y', 20);

CREATE MATERIALIZED VIEW metrics_hourly
  WITH (timescaledb.continuous) AS
  SELECT time_bucket('1 hour', time) AS bucket, tenant, avg(value)
  FROM metrics
  GROUP BY bucket, tenant
  WITH NO DATA;

--Initial refresh to set invalidation threshold forward.
CALL refresh_continuous_aggregate('metrics_hourly', '2020-01-01', '2025-01-10 12:00:00+00');
ALTER MATERIALIZED VIEW metrics_hourly SET (timescaledb.enable_granular_refresh = true);

--Check current cagg content
SELECT tenant, bucket, avg FROM metrics_hourly ORDER BY tenant, bucket;

-- Insert values for both x and y, where y's time falls into the the granular threshold window
-- but x's time is outside to the left of the window
BEGIN;
INSERT INTO metrics VALUES ('2025-01-07 10:00:00+00', 'x', 30);
INSERT INTO metrics VALUES ('2025-01-07 14:00:00+00', 'y', 40);
COMMIT;

-- Invalidation log show 2 invalidation entries for the insert transaction,
-- one outside the window with seqnum 0, one inside with seqnum 1
SELECT _timescaledb_functions.to_timestamp(lowest_modified_value)   AS lowest,
       _timescaledb_functions.to_timestamp(greatest_modified_value) AS greatest,
       seqnum
FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
WHERE hypertable_id = (
    SELECT raw_hypertable_id FROM _timescaledb_catalog.continuous_agg
    WHERE user_view_name = 'metrics_hourly')
ORDER BY lowest_modified_value;

--Refresh the range that covers the newly inserted rows. This also flush the corresponding
--trackings.
CALL refresh_continuous_aggregate('metrics_hourly', '2020-01-01', '2025-01-10 12:00:00+00');

-- Check the content of the trackings, we can see
-- 'x' lies wholly below the granular window, so its tracking was not written
SELECT tenant_id, seqnum
FROM _timescaledb_catalog.continuous_aggs_tenant_tracking
WHERE hypertable_id = (
    SELECT raw_hypertable_id FROM _timescaledb_catalog.continuous_agg
    WHERE user_view_name = 'metrics_hourly')
  AND tenant_id IS NOT NULL
ORDER BY seqnum, tenant_id;

-- Query the raw data
SELECT tenant, time_bucket('1 hour', time) AS bucket, avg(value)
FROM metrics
GROUP BY 1, 2
ORDER BY 1, 2;

--check the cagg content, should match the raw data query above
SELECT tenant, bucket, avg FROM metrics_hourly ORDER BY tenant, bucket;

-- Tenant trackings straddling the boundary are not cut like invalidations.
-- That works because a refresh is lead by the invalidation range.
-- In the test below, 'z' writes on both sides of the late arriving window in
-- one transaction, so it overlaps the window and is tracked as a single
-- tracking of [09:00,16:00].
-- Its invalidation is splitted into 2: one outside the window with segnum null,
-- one inside with nonzero seqnum. As such, the data outside the window will
-- be refresh with a full refresh, while the one inside the window has granular
-- refresh.

BEGIN;
INSERT INTO metrics VALUES ('2025-01-07 09:00:00+00', 'z', 10);
INSERT INTO metrics VALUES ('2025-01-07 16:00:00+00', 'z', 20);
COMMIT;

SELECT _timescaledb_functions.to_timestamp(lowest_modified_value)   AS lowest,
       _timescaledb_functions.to_timestamp(greatest_modified_value) AS greatest,
       seqnum
FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
WHERE hypertable_id = (
    SELECT raw_hypertable_id FROM _timescaledb_catalog.continuous_agg
    WHERE user_view_name =  'metrics_hourly')
ORDER BY lowest_modified_value;

CALL refresh_continuous_aggregate('metrics_hourly', '2020-01-01', '2025-01-10 12:00:00+00');

-- The tracking row spans the window's left boundary, not splitted like the invalidation
SELECT tenant_id, seqnum,
       _timescaledb_functions.to_timestamp(min_timestamp) AS min_ts,
       _timescaledb_functions.to_timestamp(max_timestamp) AS max_ts
FROM _timescaledb_catalog.continuous_aggs_tenant_tracking
WHERE hypertable_id = (
    SELECT raw_hypertable_id FROM _timescaledb_catalog.continuous_agg
    WHERE user_view_name = 'metrics_hourly')
  AND tenant_id = 'z';

--cagg content should match the corresponding query from the raw table
SELECT tenant, bucket, avg FROM metrics_hourly WHERE tenant = 'z' ORDER BY bucket;
SELECT tenant, time_bucket('1 hour', time) AS bucket, avg(value)
FROM metrics
WHERE tenant = 'z'
GROUP BY 1, 2
ORDER BY 2;

-----------------------------------------------------------------------
-- Also test the invalidation split at the window's right edge,
-- WE = now() - 1 hour = 2025-01-10 11:00.

-- This first insert add more data to the right, so a refresh after that
-- move invalidation threshold further right, pass the region we want
-- to test (otherwise we won't observe invalidations being written)

INSERT INTO metrics VALUES ('2025-01-10 10:00:00+00', 'p', 10),
                           ('2025-01-10 11:35:00+00', 'q', 20);
CALL refresh_continuous_aggregate('metrics_hourly', '2020-01-01', '2025-01-10 12:00:00+00');

SELECT tenant, bucket, avg FROM metrics_hourly WHERE tenant IN ('p','q') ORDER BY tenant;

-- insert data for p and q, with q falling to the right of the late window
BEGIN;
INSERT INTO metrics VALUES ('2025-01-10 10:00:00+00', 'p', 30);
INSERT INTO metrics VALUES ('2025-01-10 11:30:00+00', 'q', 40);
COMMIT;

-- Split at the right edge: the part below keeps the seqnum, the part above is
-- untracked.
SELECT _timescaledb_functions.to_timestamp(lowest_modified_value)   AS lowest,
       _timescaledb_functions.to_timestamp(greatest_modified_value) AS greatest,
       seqnum
FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
WHERE hypertable_id = (
    SELECT raw_hypertable_id FROM _timescaledb_catalog.continuous_agg
    WHERE user_view_name = 'metrics_hourly')
ORDER BY lowest_modified_value;

CALL refresh_continuous_aggregate('metrics_hourly', '2020-01-01', '2025-01-10 12:00:00+00');

--cagg content should match the corresponding query from the raw table
SELECT tenant, bucket, avg FROM metrics_hourly WHERE tenant IN ('p','q') ORDER BY tenant;
SELECT tenant, time_bucket('1 hour', time) AS bucket, avg(value)
FROM metrics
WHERE tenant IN ('p','q')
GROUP BY 1, 2
ORDER BY 1;

-----------------------------------------------------------------------
-- Every split above lands on a bucket boundary, because the mocked now() puts W
-- on the hour and the buckets are hourly. Now mock a different time so
-- the boundary falls mid-bucket. Both halves of the split then expand to 
-- bucket boundary when they are moved to the materialization log, and overlap
-- each other by one bucket. That bucket is refreshed twice,
-- once fully and once tenant-scoped. The result is still correct, though
-- the tenant-scoped refresh on that bucket is wasted.
--
CREATE MATERIALIZED VIEW metrics_daily
  WITH (timescaledb.continuous) AS
  SELECT time_bucket('1 day', time) AS bucket, tenant, avg(value)
  FROM metrics
  GROUP BY bucket, tenant
  WITH NO DATA;

-- Moving now() half an hour puts W at 2025-01-07 12:30, inside the 12:00 bucket.
-- A writer gates on the window stored on its generation, which is only replaced
-- when a flush activates the next one, so the insert and refresh below are what
-- install the moved window.
SET timescaledb.current_timestamp_mock = '2025-01-10 12:30:00+00';
--refresh to move invalidation threshold forward
CALL refresh_continuous_aggregate('metrics_hourly', '2020-01-01', '2025-01-30 12:00:00+00');
--print the bucket that contain now()
SELECT '2025-01-10 12:30:00+00'::timestamptz - INTERVAL '3 days' AS window_start,
       time_bucket('1 hour', '2025-01-10 12:30:00+00'::timestamptz - INTERVAL '3 days')
         AS containing_bucket;

SELECT tenant, bucket, avg FROM metrics_hourly WHERE tenant IN ('m','n') ORDER BY tenant;

-- m sits below W and n above it
BEGIN;
INSERT INTO metrics VALUES ('2025-01-07 11:10:00+00', 'm', 30);
INSERT INTO metrics VALUES ('2025-01-07 12:50:00+00', 'n', 40);
COMMIT;

-- Raw ranges, split at 12:30, i.e. in the middle of a bucket.
SELECT _timescaledb_functions.to_timestamp(lowest_modified_value)   AS lowest,
       _timescaledb_functions.to_timestamp(greatest_modified_value) AS greatest,
       seqnum
FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
WHERE hypertable_id = (
    SELECT raw_hypertable_id FROM _timescaledb_catalog.continuous_agg
    WHERE user_view_name = 'metrics_hourly')
ORDER BY lowest_modified_value;

-- Refreshing the other cagg to move invalidations from hypertable inval. log to 
-- cagg invalidation log, expanding both halves to bucket
-- boundaries in metrics_hourly's log without consuming them.
CALL refresh_continuous_aggregate('metrics_daily', '2020-01-01', '2025-01-10 12:30:00+00');

-- The two halves now overlap at the 12:00 bucket, one untracked and one carrying
-- the seqnum: that bucket gets a full pass and a tenant-scoped pass.
SELECT CASE WHEN lowest_modified_value <= _timescaledb_functions.get_internal_time_min('timestamptz'::regtype)
            THEN '-infinity'::timestamptz
            ELSE _timescaledb_functions.to_timestamp(lowest_modified_value) END AS lowest,
       CASE WHEN greatest_modified_value >= _timescaledb_functions.get_internal_time_max('timestamptz'::regtype)
            THEN 'infinity'::timestamptz
            ELSE _timescaledb_functions.to_timestamp(greatest_modified_value) END AS greatest,
       seqnum
FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
WHERE materialization_id = (
    SELECT mat_hypertable_id FROM _timescaledb_catalog.continuous_agg
    WHERE user_view_name = 'metrics_hourly')
ORDER BY lowest_modified_value, seqnum;

CALL refresh_continuous_aggregate('metrics_hourly', '2020-01-01', '2025-01-10 12:30:00+00');

-- m is recomputed by the full pass over the bucket, n by the tenant-scoped pass
-- over the same bucket. Both should match the raw data.
SELECT tenant, bucket, avg FROM metrics_hourly WHERE tenant IN ('m','n') ORDER BY tenant;
SELECT tenant, time_bucket('1 hour', time) AS bucket, avg(value)
FROM metrics
WHERE tenant IN ('m','n')
GROUP BY 1, 2
ORDER BY 1;

-----------------------------------------------------------------------
-- One transaction reaching past both edges at once: rows before the window
-- start, inside it, and at or after the window end. They share a chunk, so the
-- whole thing is one invalidation spanning the window. The invalidation is
-- splitted at both boundary, and three log rows come out.
-- With W = 2025-01-07 12:30 and WE = 2025-01-10 11:30:
--
--   'before' 01-07 11:00   W    'inside' 01-08 12:00    WE   'after' 01-10 11:45
--   ---------|------------[|---------|------------------|)---------|---------->
--
-- Only 'inside' overlaps the window, so it is the only tenant recorded; the two
-- untracked pieces are what bring 'before' and 'after' back up to date.

SELECT tenant, bucket, avg FROM metrics_hourly
WHERE tenant IN ('before','inside','after') ORDER BY tenant;

BEGIN;
INSERT INTO metrics VALUES ('2025-01-07 11:00:00+00', 'before', 3);
INSERT INTO metrics VALUES ('2025-01-08 12:00:00+00', 'inside', 4);
INSERT INTO metrics VALUES ('2025-01-10 11:45:00+00', 'after',  5);
COMMIT;

-- Three rows: untracked below W, the seqnum in the middle, untracked from WE up.
SELECT _timescaledb_functions.to_timestamp(lowest_modified_value)   AS lowest,
       _timescaledb_functions.to_timestamp(greatest_modified_value) AS greatest,
       seqnum
FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
WHERE hypertable_id = (
    SELECT raw_hypertable_id FROM _timescaledb_catalog.continuous_agg
    WHERE user_view_name = 'metrics_hourly')
ORDER BY lowest_modified_value;

CALL refresh_continuous_aggregate('metrics_hourly', '2020-01-01', '2025-01-10 12:30:00+00');

-- 'before' and 'after' are outside the window, so neither was tracked.
SELECT tenant_id, seqnum
FROM _timescaledb_catalog.continuous_aggs_tenant_tracking
WHERE hypertable_id = (
    SELECT raw_hypertable_id FROM _timescaledb_catalog.continuous_agg
    WHERE user_view_name = 'metrics_hourly')
  AND tenant_id IN ('before','inside','after')
ORDER BY seqnum, tenant_id;

--cagg content should match the corresponding query from the raw table
SELECT tenant, bucket, avg FROM metrics_hourly
WHERE tenant IN ('before','inside','after') ORDER BY tenant;
SELECT tenant, time_bucket('1 hour', time) AS bucket, avg(value)
FROM metrics
WHERE tenant IN ('before','inside','after')
GROUP BY 1, 2
ORDER BY 1;

DROP MATERIALIZED VIEW metrics_daily;
DROP MATERIALIZED VIEW metrics_hourly;
DROP TABLE metrics;
RESET timezone;
