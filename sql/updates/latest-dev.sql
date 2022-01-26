-- Get rid of chunk_id from materialization hypertables
DROP FUNCTION IF EXISTS timescaledb_experimental.refresh_continuous_aggregate(REGCLASS, REGCLASS);

-- Metadata to store information about the current version of the CAGG
-- 0 - Current and deprecated version
-- 1 - Faster version finalizing data (available only on single-node)
-- 2 - New version removing re-aggregation (needed for multi-node)
ALTER TABLE _timescaledb_catalog.continuous_agg
  ADD COLUMN version INTEGER;

UPDATE _timescaledb_catalog.continuous_agg SET version = 0;

ALTER TABLE _timescaledb_catalog.continuous_agg
  ALTER COLUMN version SET NOT NULL,
  ALTER COLUMN version SET DEFAULT 1;
