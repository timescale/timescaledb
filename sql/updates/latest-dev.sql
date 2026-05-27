-- Drop obsolete chunk_constraint rows for inherited CHECK constraints on
-- OSM chunks; PG inheritance handles propagation now.
DELETE FROM _timescaledb_catalog.chunk_constraint cc
USING _timescaledb_catalog.chunk c
WHERE cc.chunk_id = c.id
  AND c.osm_chunk
  AND cc.dimension_slice_id IS NULL
  AND cc.hypertable_constraint_name IS NOT NULL
  AND cc.constraint_name = cc.hypertable_constraint_name;

-- Rename legacy chunk-side constraints to the names the new code recomputes:
-- FKs use the parent's name; unique/PK/exclusion/trigger use the deterministic
-- "<chunk_id>_<parent>" form
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT pg_catalog.format('%I.%I', c.schema_name, c.table_name) AS chunk_table,
               cc.constraint_name AS old_name,
               CASE WHEN parent.contype = 'f' THEN cc.hypertable_constraint_name
                    ELSE format('%s_%s', c.id, cc.hypertable_constraint_name)
               END AS new_name
        FROM _timescaledb_catalog.chunk_constraint cc
        JOIN _timescaledb_catalog.chunk c ON c.id = cc.chunk_id
        JOIN _timescaledb_catalog.hypertable ht ON ht.id = c.hypertable_id
        JOIN pg_constraint parent
            ON parent.conrelid = pg_catalog.format('%I.%I', ht.schema_name, ht.table_name)::regclass
            AND parent.conname = cc.hypertable_constraint_name
            AND parent.contype IN ('f', 'u', 'p', 'x', 't')
        WHERE cc.dimension_slice_id IS NULL
          AND cc.hypertable_constraint_name IS NOT NULL
    LOOP
        IF r.old_name <> r.new_name THEN
            EXECUTE pg_catalog.format('ALTER TABLE %s RENAME CONSTRAINT %I TO %I',
                                      r.chunk_table, r.old_name, r.new_name);
        END IF;
    END LOOP;
END
$$;

-- Remove the chunk_constraint rows that mirrored non-dimensional constraints.
-- FK chunk-side constraints are now located by name through the event-trigger
-- hooks; unique/PK/exclusion/trigger constraints through the deterministic
-- "<chunk_id>_<parent>" name pattern.
DELETE FROM _timescaledb_catalog.chunk_constraint
WHERE dimension_slice_id IS NULL
  AND hypertable_constraint_name IS NOT NULL;

ALTER TABLE _timescaledb_catalog.hypertable SET (user_catalog_table = true);
ALTER TABLE _timescaledb_catalog.chunk SET (user_catalog_table = true);

CREATE OR REPLACE FUNCTION _timescaledb_functions.decompress_batch(record)
   RETURNS SETOF record
   AS '@MODULE_PATHNAME@', 'ts_update_placeholder'
   LANGUAGE C STRICT;

