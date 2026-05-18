-- Restore tracking rows for inherited CHECK constraints on OSM chunks.
-- Earlier versions expected one chunk_constraint row per CHECK constraint
-- inherited from the hypertable on each foreign-table chunk.
INSERT INTO _timescaledb_catalog.chunk_constraint
    (chunk_id, dimension_slice_id, constraint_name, hypertable_constraint_name)
SELECT c.id, NULL, con.conname, con.conname
FROM _timescaledb_catalog.chunk c
JOIN _timescaledb_catalog.hypertable ht ON ht.id = c.hypertable_id
JOIN pg_class ht_class
    ON ht_class.relname = ht.table_name
JOIN pg_namespace ht_ns
    ON ht_ns.oid = ht_class.relnamespace
    AND ht_ns.nspname = ht.schema_name
JOIN pg_constraint con
    ON con.conrelid = ht_class.oid
    AND con.contype = 'c'
WHERE c.osm_chunk
ON CONFLICT DO NOTHING;

