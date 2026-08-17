-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- A writer that never reaches end_batch still gives up its generation pin.
--
-- The tracker pins a generation for the whole pre-commit drain: begin_batch
-- increments num_writers, end_batch decrements it, and a flush refuses to drain
-- a generation until that count reaches zero.  A writer that fails in between
-- would otherwise leave the count up, and every later flush of that generation
-- would wait on a writer that is gone.  The abort handler releases the pin the
-- backend still holds, so the refresh below completes and the tenant recorded
-- into that generation is flushed.

SET timezone TO 'UTC';

SELECT (CURRENT_DATE - DATE '2019-01-01')::text || ' days' AS granular_refresh_lookback \gset

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

-- A normal write and refresh first, so the tracker exists and flushes cleanly.
INSERT INTO conditions VALUES ('2020-01-02 00:00+00', 'a', 1);
CALL refresh_continuous_aggregate('cond_daily', '2020-01-01', '2020-01-10');
SELECT count(*) AS tracking_rows
FROM _timescaledb_catalog.continuous_aggs_tenant_tracking
WHERE hypertable_id = (SELECT raw_hypertable_id FROM _timescaledb_catalog.continuous_agg
                       WHERE user_view_name = 'cond_daily');

-- A tenant recorded into the generation the failing writer is about to pin, so
-- the end of this test can show it was still flushed correctly.
INSERT INTO conditions VALUES ('2020-01-04 00:00+00', 'c', 3);

-- Fail a writer between begin_batch and end_batch.  The INSERT aborting is
-- expected; what matters is the pin it leaves behind.
SELECT debug_waitpoint_enable('tenant_tracker_fail_in_pin');
\set ON_ERROR_STOP 0
INSERT INTO conditions VALUES ('2020-01-03 00:00+00', 'b', 2);
\set ON_ERROR_STOP 1
SELECT debug_waitpoint_release('tenant_tracker_fail_in_pin');

-- The refresh flips the generation and drains the pinned one, which it can only
-- do because the abort released the pin.  The timeout is a ceiling so that a
-- regression fails here instead of hanging the suite; it is not the assertion.
SET statement_timeout = '60s';
CALL refresh_continuous_aggregate('cond_daily', '2020-01-01', '2020-01-10');
RESET statement_timeout;

-- The generation was drained, so the tenant written before the failing writer
-- is persisted with its seqnum, and the aborted writer's tenant is absent.
SELECT tenant_id, seqnum
FROM _timescaledb_catalog.continuous_aggs_tenant_tracking
WHERE hypertable_id = (SELECT raw_hypertable_id FROM _timescaledb_catalog.continuous_agg
                       WHERE user_view_name = 'cond_daily')
ORDER BY seqnum, tenant_id;

SELECT sensor_id, bucket, avg FROM cond_daily ORDER BY sensor_id, bucket;

DROP MATERIALIZED VIEW cond_daily;
DROP TABLE conditions;
