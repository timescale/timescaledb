-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- A writer that never reaches end_batch leaves its generation pinned.
--
-- The tracker pins a generation for the whole pre-commit drain: begin_batch
-- increments num_writers, end_batch decrements it, and a flush refuses to drain
-- a generation until that count reaches zero.  Nothing unwinds the pin when the
-- writer fails in between: there is no PG_FINALLY around it, and the abort-time
-- cleanup frees only this backend's local hash tables.  The count stays up, and
-- every later flush of that generation waits on a writer that no longer exists.

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

-- Fail a writer between begin_batch and end_batch.  The INSERT aborting is
-- expected; what matters is the pin it leaves behind.
SELECT debug_waitpoint_enable('tenant_tracker_fail_in_pin');
\set ON_ERROR_STOP 0
INSERT INTO conditions VALUES ('2020-01-03 00:00+00', 'b', 2);
\set ON_ERROR_STOP 1
SELECT debug_waitpoint_release('tenant_tracker_fail_in_pin');

-- The refresh flips the generation and then drains the pinned one, so it cannot
-- finish.  The statement timeout is the only thing that ends it.
SET statement_timeout = '10s';
\set ON_ERROR_STOP 0
CALL refresh_continuous_aggregate('cond_daily', '2020-01-01', '2020-01-10');
\set ON_ERROR_STOP 1
RESET statement_timeout;

DROP MATERIALIZED VIEW cond_daily;
DROP TABLE conditions;
