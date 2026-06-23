
DROP FUNCTION IF EXISTS _timescaledb_functions.estimate_uncompressed_size(regclass);

--
-- BEGIN chunk.compressed_chunk_id no longer used
--

ALTER TABLE _timescaledb_catalog.compression_chunk_size DROP CONSTRAINT IF EXISTS compression_chunk_size_compressed_chunk_id_fkey;
DROP INDEX IF EXISTS _timescaledb_catalog.chunk_compressed_chunk_id_idx;

UPDATE _timescaledb_catalog.chunk SET compressed_chunk_id = NULL WHERE compressed_chunk_id IS NOT NULL;
UPDATE _timescaledb_catalog.compression_chunk_size SET compressed_chunk_id = 0 WHERE compressed_chunk_id <> 0;

DELETE FROM _timescaledb_catalog.chunk
WHERE hypertable_id IN (
  SELECT compressed_hypertable_id
  FROM _timescaledb_catalog.hypertable
  WHERE compressed_hypertable_id IS NOT NULL
);

--
-- END chunk.compressed_chunk_id no longer used
--

