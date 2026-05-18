-- Inherited CHECK constraints on OSM chunks are no longer tracked in
-- _timescaledb_catalog.chunk_constraint. PostgreSQL inheritance handles
-- propagation, so the rows are redundant. Remove any rows left over from
-- earlier versions.
DELETE FROM _timescaledb_catalog.chunk_constraint cc
USING _timescaledb_catalog.chunk c
WHERE cc.chunk_id = c.id
  AND c.osm_chunk
  AND cc.dimension_slice_id IS NULL
  AND cc.hypertable_constraint_name IS NOT NULL
  AND cc.constraint_name = cc.hypertable_constraint_name;

