
-- Re-add self-referential foreign keys
ALTER TABLE _timescaledb_catalog.hypertable ADD CONSTRAINT hypertable_compressed_hypertable_id_fkey FOREIGN KEY (compressed_hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id);
ALTER TABLE _timescaledb_catalog.chunk ADD CONSTRAINT chunk_compressed_chunk_id_fkey FOREIGN KEY (compressed_chunk_id) REFERENCES _timescaledb_catalog.chunk (id);

DROP FUNCTION IF EXISTS _timescaledb_functions.bloom1_contains_any_hashes(_timescaledb_internal.bloom1, bigint[]);
DROP FUNCTION IF EXISTS _timescaledb_functions.bloom1_hash(anyelement);

-- Drop BIGINT-returning version so the downgrade script can recreate the
-- INTEGER-returning version of compressed_data_column_size.
DROP FUNCTION IF EXISTS _timescaledb_functions.compressed_data_column_size(_timescaledb_internal.compressed_data, ANYELEMENT);

DROP FUNCTION IF EXISTS _timescaledb_functions.decompress_batch(record);
