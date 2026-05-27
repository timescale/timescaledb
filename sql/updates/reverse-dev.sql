-- Restore chunk_constraint rows for CHECK constraints on OSM chunks.
INSERT INTO _timescaledb_catalog.chunk_constraint
    (chunk_id, dimension_slice_id, constraint_name, hypertable_constraint_name)
SELECT c.id, NULL, con.conname, con.conname
FROM _timescaledb_catalog.chunk c
JOIN _timescaledb_catalog.hypertable ht ON ht.id = c.hypertable_id
JOIN pg_constraint con
    ON con.conrelid = pg_catalog.format('%I.%I', ht.schema_name, ht.table_name)::regclass
    AND con.contype = 'c'
WHERE c.osm_chunk
ON CONFLICT DO NOTHING;

-- Restore chunk_constraint rows for outbound FKs by matching chunk-side
-- FKs to their hypertable-side counterpart by name.
INSERT INTO _timescaledb_catalog.chunk_constraint
    (chunk_id, dimension_slice_id, constraint_name, hypertable_constraint_name)
SELECT c.id, NULL, child.conname, parent.conname
FROM _timescaledb_catalog.chunk c
JOIN _timescaledb_catalog.hypertable ht ON ht.id = c.hypertable_id
JOIN pg_constraint parent
    ON parent.conrelid = pg_catalog.format('%I.%I', ht.schema_name, ht.table_name)::regclass
    AND parent.contype = 'f'
JOIN pg_constraint child
    ON child.conrelid = pg_catalog.format('%I.%I', c.schema_name, c.table_name)::regclass
    AND child.contype = 'f'
    AND child.conname = parent.conname
ON CONFLICT DO NOTHING;

-- Restore chunk_constraint rows for unique/PK/exclusion/trigger constraints.
-- The chunk-side conname is the deterministic "<chunk_id>_<parent>" form
INSERT INTO _timescaledb_catalog.chunk_constraint
    (chunk_id, dimension_slice_id, constraint_name, hypertable_constraint_name)
SELECT c.id, NULL, child.conname, parent.conname
FROM _timescaledb_catalog.chunk c
JOIN _timescaledb_catalog.hypertable ht ON ht.id = c.hypertable_id
JOIN pg_constraint parent
    ON parent.conrelid = pg_catalog.format('%I.%I', ht.schema_name, ht.table_name)::regclass
    AND parent.contype IN ('u', 'p', 'x', 't')
JOIN pg_constraint child
    ON child.conrelid = pg_catalog.format('%I.%I', c.schema_name, c.table_name)::regclass
    AND child.conname = format('%s_%s', c.id, parent.conname)
ON CONFLICT DO NOTHING;

ALTER TABLE _timescaledb_catalog.hypertable RESET (user_catalog_table);
ALTER TABLE _timescaledb_catalog.chunk RESET (user_catalog_table);

DROP FUNCTION IF EXISTS _timescaledb_functions.decompress_batch(record);
