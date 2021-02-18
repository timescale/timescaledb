-- set compressed_chunk_id to NULL for dropped chunks
UPDATE _timescaledb_catalog.chunk SET compressed_chunk_id = NULL WHERE dropped = true AND compressed_chunk_id IS NOT NULL;
