-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Reclamation of tenant-tracking rows.
--
-- Each refresh flushes one generation, so its rows are written stamped with that
-- generation's seqnum.  A generation becomes collectable once both hold:
--   (a) no invalidation carries its seqnum any more -- it has been materialized;
--   (b) it is not among the newest TRACKING_CLEANUP_HEADROOM generations, which
--       are held back so an invalidation still in flight has time to land.
-- The rounds below walk a generation from written, through consumed, to
-- reclaimed.  Tenants are tracked only for late-arriving data (older than
-- now - 1 day), so everything uses fixed 2020 timestamps.

SET timezone TO 'UTC';
SET timescaledb.current_timestamp_mock = '2021-01-10 00:00:00+00';

CREATE TABLE conditions(time timestamptz NOT NULL, sensor_id text, value float);
SELECT create_hypertable('conditions', 'time');
ALTER TABLE conditions SET (
    timescaledb.granular_refresh_column = 'sensor_id',
    timescaledb.granular_refresh_start_offset = '2 years',
    timescaledb.granular_refresh_end_offset = '1 day'
);

CREATE MATERIALIZED VIEW cond_daily
  WITH (timescaledb.continuous) AS
  SELECT time_bucket('1 day', time) AS bucket, sensor_id, avg(value)
  FROM conditions
  GROUP BY bucket, sensor_id
  WITH NO DATA;
ALTER MATERIALIZED VIEW cond_daily SET (timescaledb.enable_granular_refresh = true);

CREATE VIEW tracking AS
SELECT tenant_id,
       _timescaledb_functions.to_timestamp(min_timestamp) AS min_ts,
       seqnum
FROM _timescaledb_catalog.continuous_aggs_tenant_tracking
WHERE hypertable_id = (SELECT raw_hypertable_id
                       FROM _timescaledb_catalog.continuous_agg
                       WHERE user_view_name = 'cond_daily');

-- Which seqnums the cagg's log still references, i.e. what keeps a generation
-- alive.  A generation absent here and old enough is collectable.
CREATE VIEW live_seqnums AS
SELECT DISTINCT seqnum
FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
WHERE materialization_id = (SELECT mat_hypertable_id
                            FROM _timescaledb_catalog.continuous_agg
                            WHERE user_view_name = 'cond_daily')
  AND seqnum > 0;


-- Priming: move the invalidation threshold above the 2020 fixture data so the
-- later inserts are late-arriving and produce narrow per-bucket invalidations.
INSERT INTO conditions VALUES ('2020-06-01 00:00+00', 'prime', 9);
CALL refresh_continuous_aggregate('cond_daily', '2019-01-01', '2021-01-01');

\echo -- after priming
SELECT * FROM tracking ORDER BY seqnum, tenant_id;
SELECT * FROM live_seqnums ORDER BY seqnum;

-- A second hypertable, with its own tracker and so its own seqnum sequence.
-- It is given a dead generation 1 and a live 2, then left alone: every round
-- below refreshes cond_daily only, and none of them may touch these rows.
CREATE TABLE conditions2(time timestamptz NOT NULL, sensor_id text, value float);
SELECT create_hypertable('conditions2', 'time');
ALTER TABLE conditions2 SET (
    timescaledb.granular_refresh_column = 'sensor_id',
    timescaledb.granular_refresh_start_offset = '2 years',
    timescaledb.granular_refresh_end_offset = '1 day'
);
CREATE MATERIALIZED VIEW cond2_daily
  WITH (timescaledb.continuous) AS
  SELECT time_bucket('1 day', time) AS bucket, sensor_id, avg(value)
  FROM conditions2
  GROUP BY bucket, sensor_id
  WITH NO DATA;
ALTER MATERIALIZED VIEW cond2_daily SET (timescaledb.enable_granular_refresh = true);

CREATE VIEW tracking2 AS
SELECT tenant_id, seqnum
FROM _timescaledb_catalog.continuous_aggs_tenant_tracking
WHERE hypertable_id = (SELECT raw_hypertable_id
                       FROM _timescaledb_catalog.continuous_agg
                       WHERE user_view_name = 'cond2_daily');

INSERT INTO conditions2 VALUES ('2020-06-01 00:00+00', 'prime2', 9);
CALL refresh_continuous_aggregate('cond2_daily', '2019-01-01', '2021-01-01');
INSERT INTO conditions2 VALUES ('2020-01-02 00:00+00', 'c1', 1);
CALL refresh_continuous_aggregate('cond2_daily', '2020-01-01', '2020-01-10');
SELECT * FROM tracking2 ORDER BY seqnum, tenant_id;

-- Reclamation shows up as generations disappearing from the tracking table.
-- A generation is reclaimed once a later refresh has both consumed it and
-- flushed past it by more than the headroom. As headroom = 1, the dead trackings
-- start to be collected at round 2.
INSERT INTO conditions VALUES ('2020-01-02 00:00+00', 's1', 1);
-- round 1
CALL refresh_continuous_aggregate('cond_daily', '2020-01-01', '2020-01-10');
SELECT * FROM tracking ORDER BY seqnum, tenant_id;
SELECT * FROM live_seqnums ORDER BY seqnum;

INSERT INTO conditions VALUES ('2020-01-03 00:00+00', 's2', 2);
-- round 2
CALL refresh_continuous_aggregate('cond_daily', '2020-01-01', '2020-01-10');
SELECT * FROM tracking ORDER BY seqnum, tenant_id;

INSERT INTO conditions VALUES ('2020-01-04 00:00+00', 's3', 3);
-- round 3
CALL refresh_continuous_aggregate('cond_daily', '2020-01-01', '2020-01-10');
SELECT * FROM tracking ORDER BY seqnum, tenant_id;

INSERT INTO conditions VALUES ('2020-01-05 00:00+00', 's4', 4);
-- round 4
CALL refresh_continuous_aggregate('cond_daily', '2020-01-01', '2020-01-10');
SELECT * FROM tracking ORDER BY seqnum, tenant_id;
SELECT * FROM live_seqnums ORDER BY seqnum;

-- ============================================================================
-- generations that stay live, and a collection of several at once.
-- ============================================================================

INSERT INTO conditions VALUES ('2020-01-02 00:00+00', 'a1', 1);
--max seqnum (in the tracking table) = 5 when this refresh runs gc
-- dead trackings with seqnum <=4 are collected by the gc.
-- Note that refresh later flushes trackings so max seqnum after refresh is 6.
CALL refresh_continuous_aggregate('cond_daily', '2020-01-01', '2020-01-10');
SELECT * FROM tracking ORDER BY seqnum, tenant_id;

INSERT INTO conditions VALUES ('2020-02-05 00:00+00', 'fA', 10);   -- outside the Jan window
--max seqnum (in the tracking table) = 6 when this refresh runs gc,
--seqnum 5 collected
CALL refresh_continuous_aggregate('cond_daily', '2020-01-01', '2020-01-10');
SELECT * FROM tracking ORDER BY seqnum, tenant_id;

--max = 7, 6 collected
INSERT INTO conditions VALUES ('2020-01-03 00:00+00', 'a2', 2);
CALL refresh_continuous_aggregate('cond_daily', '2020-01-01', '2020-01-10');
SELECT * FROM tracking ORDER BY seqnum, tenant_id;

--max = 8, 7 is NOT collected because it's still live.
INSERT INTO conditions VALUES ('2020-02-06 00:00+00', 'fB', 11);   -- outside the Jan window
CALL refresh_continuous_aggregate('cond_daily', '2020-01-01', '2020-01-10');
SELECT * FROM tracking ORDER BY seqnum, tenant_id;

--max = 9, 7 is NOT collected because it's still live.
INSERT INTO conditions VALUES ('2020-01-04 00:00+00', 'a3', 3);
--Refresh the Jan window, which consumes the Jan invalidations and leaves the Feb ones live.
CALL refresh_continuous_aggregate('cond_daily', '2020-01-01', '2020-01-10');

SELECT * FROM tracking ORDER BY seqnum, tenant_id;
-- live seqnums: the Feb invalidations, as we have only refresh Jan so far.
SELECT * FROM live_seqnums ORDER BY seqnum;

-- Retire the Feb invalidations.Seqnum 7 and 9 are dead after the refresh but won't be
-- collected until the next refresh.
-- This also pump in-memory seqnum to 12 without inserting any trackings with seqnum 11,
-- As there is no new data between this and the previous refresh.
CALL refresh_continuous_aggregate('cond_daily', '2020-01-01', '2020-02-10');

SELECT * FROM tracking ORDER BY seqnum, tenant_id;
SELECT * FROM live_seqnums ORDER BY seqnum;

--Max seqnum = 10. Although the in-memory seqnum is 12, the max seqnum in the tracking table
-- is 10 because there was no tracking with seqnum 11.
--7 and 9 are collected
INSERT INTO conditions VALUES ('2020-01-05 00:00+00', 'a4', 4);
CALL refresh_continuous_aggregate('cond_daily', '2020-01-01', '2020-01-10');

SELECT * FROM tracking ORDER BY seqnum, tenant_id;
SELECT * FROM live_seqnums ORDER BY seqnum;

-- correctness after all of it
SELECT sensor_id, bucket, avg FROM cond_daily ORDER BY sensor_id, bucket;
SELECT sensor_id, time_bucket('1 day', time) AS bucket, avg(value)
FROM conditions GROUP BY sensor_id, bucket ORDER BY sensor_id, bucket;

-- conditions2 is untouched by all of the above, generation 1 included, even
-- though it is just as dead as the generations cond_daily's refreshes reclaimed.
SELECT * FROM tracking2 ORDER BY seqnum, tenant_id;

-- Its own refresh does reclaim it, so what held it was the hypertable scoping.
INSERT INTO conditions2 VALUES ('2020-01-03 00:00+00', 'c2', 2);
CALL refresh_continuous_aggregate('cond2_daily', '2020-01-01', '2020-01-10');
SELECT * FROM tracking2 ORDER BY seqnum, tenant_id;

DROP MATERIALIZED VIEW cond_daily;
DROP TABLE conditions;
DROP MATERIALIZED VIEW cond2_daily;
DROP TABLE conditions2;
