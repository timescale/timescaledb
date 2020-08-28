
-- set compressed_chunk_id to NULL for dropped chunks
UPDATE _timescaledb_catalog.chunk SET compressed_chunk_id = NULL WHERE dropped = true AND compressed_chunk_id IS NOT NULL;

-- DIFF: 946684800000000
UPDATE
  _timescaledb_catalog.dimension_slice ds
SET range_start = ds.range_start - 946684800000000,
  range_end = ds.range_end - 946684800000000
FROM _timescaledb_catalog.dimension d
WHERE d.id = ds.dimension_id
  AND d.column_type IN ('timestamp', 'timestamptz', 'date');

