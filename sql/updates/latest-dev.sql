-- Drop obsolete chunk_constraint rows for inherited CHECK constraints on
-- OSM chunks; PG inheritance handles propagation now.
DELETE FROM _timescaledb_catalog.chunk_constraint cc
USING _timescaledb_catalog.chunk c
WHERE cc.chunk_id = c.id
  AND c.osm_chunk
  AND cc.dimension_slice_id IS NULL
  AND cc.hypertable_constraint_name IS NOT NULL
  AND cc.constraint_name = cc.hypertable_constraint_name;

-- Rename legacy <chunk_id>_<seq>_<parent> chunk-side FKs to the parent name
-- so the event-trigger hooks can locate them by name on DROP/RENAME.
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT pg_catalog.format('%I.%I', c.schema_name, c.table_name) AS chunk_table,
               cc.constraint_name AS old_name,
               cc.hypertable_constraint_name AS new_name
        FROM _timescaledb_catalog.chunk_constraint cc
        JOIN _timescaledb_catalog.chunk c ON c.id = cc.chunk_id
        JOIN _timescaledb_catalog.hypertable ht ON ht.id = c.hypertable_id
        JOIN pg_constraint parent
            ON parent.conrelid = pg_catalog.format('%I.%I', ht.schema_name, ht.table_name)::regclass
            AND parent.conname = cc.hypertable_constraint_name
            AND parent.contype = 'f'
        WHERE cc.dimension_slice_id IS NULL
          AND cc.hypertable_constraint_name IS NOT NULL
          AND cc.constraint_name <> cc.hypertable_constraint_name
    LOOP
        EXECUTE pg_catalog.format('ALTER TABLE %s RENAME CONSTRAINT %I TO %I',
                                  r.chunk_table, r.old_name, r.new_name);
    END LOOP;
END
$$;

-- Drop the obsolete chunk_constraint rows for outbound FKs; the event-trigger
-- hooks locate chunk-side FKs by name and no longer need this catalog.
DELETE FROM _timescaledb_catalog.chunk_constraint cc
USING _timescaledb_catalog.chunk c,
      _timescaledb_catalog.hypertable ht,
      pg_constraint parent
WHERE cc.chunk_id = c.id
  AND ht.id = c.hypertable_id
  AND parent.conrelid = pg_catalog.format('%I.%I', ht.schema_name, ht.table_name)::regclass
  AND parent.conname = cc.hypertable_constraint_name
  AND parent.contype = 'f'
  AND cc.dimension_slice_id IS NULL
  AND cc.hypertable_constraint_name IS NOT NULL;

