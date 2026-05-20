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

-- Restore chunk_constraint rows for outbound FKs, derived from the
-- DEPENDENCY_AUTO edge that replaced them.
INSERT INTO _timescaledb_catalog.chunk_constraint
    (chunk_id, dimension_slice_id, constraint_name, hypertable_constraint_name)
SELECT c.id, NULL, child.conname, parent.conname
FROM _timescaledb_catalog.chunk c
JOIN _timescaledb_catalog.hypertable ht ON ht.id = c.hypertable_id
JOIN pg_constraint child
    ON child.conrelid = pg_catalog.format('%I.%I', c.schema_name, c.table_name)::regclass
    AND child.contype = 'f'
JOIN pg_depend d
    ON d.classid = 'pg_constraint'::regclass
    AND d.objid = child.oid
    AND d.refclassid = 'pg_constraint'::regclass
    AND d.deptype = 'a'
JOIN pg_constraint parent
    ON parent.oid = d.refobjid
    AND parent.conrelid = pg_catalog.format('%I.%I', ht.schema_name, ht.table_name)::regclass
    AND parent.contype = 'f'
ON CONFLICT DO NOTHING;

DELETE FROM pg_catalog.pg_depend d
USING pg_constraint child,
      pg_constraint parent,
      _timescaledb_catalog.chunk c,
      _timescaledb_catalog.hypertable ht
WHERE d.classid = 'pg_constraint'::regclass
  AND d.objid = child.oid
  AND d.refclassid = 'pg_constraint'::regclass
  AND d.refobjid = parent.oid
  AND d.deptype = 'a'
  AND child.contype = 'f'
  AND parent.contype = 'f'
  AND ht.id = c.hypertable_id
  AND child.conrelid = pg_catalog.format('%I.%I', c.schema_name, c.table_name)::regclass
  AND parent.conrelid = pg_catalog.format('%I.%I', ht.schema_name, ht.table_name)::regclass;

