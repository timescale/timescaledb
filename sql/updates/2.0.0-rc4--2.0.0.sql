ALTER TABLE _timescaledb_catalog.hypertable
  DROP CONSTRAINT hypertable_replication_factor_check,
  ADD CONSTRAINT hypertable_replication_factor_check CHECK (replication_factor > 0 OR replication_factor = -1);
