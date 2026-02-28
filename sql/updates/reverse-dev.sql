
-- Re-add self-referential foreign keys
ALTER TABLE _timescaledb_catalog.hypertable ADD CONSTRAINT hypertable_compressed_hypertable_id_fkey FOREIGN KEY (compressed_hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id);
ALTER TABLE _timescaledb_catalog.chunk ADD CONSTRAINT chunk_compressed_chunk_id_fkey FOREIGN KEY (compressed_chunk_id) REFERENCES _timescaledb_catalog.chunk (id);

DROP FUNCTION IF EXISTS _timescaledb_functions.bloom1_contains_any_hashes(_timescaledb_internal.bloom1, bigint[]);
DROP FUNCTION IF EXISTS _timescaledb_functions.bloom1_hash(anyelement);
-- Drop continuous_aggs_jobs_refresh_ranges table
DROP TABLE IF EXISTS _timescaledb_catalog.continuous_aggs_jobs_refresh_ranges;

