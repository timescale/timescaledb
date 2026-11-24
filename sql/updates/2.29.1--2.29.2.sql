--
-- BEGIN repair mismatched dimensional CHECK constraints
--

-- Chunk merges before 2.28 could leave a dimensional CHECK whose constraint_<id>
-- name no longer matches the chunk's current slice. Since 2.28 the CHECK is found
-- by that name alone, so such a stale CHECK is either the current slice's
-- constraint under an old name, or a leftover duplicate once a later merge added
-- the correctly named one. Rename each stale CHECK to its current slice, or drop
-- it when that CHECK already exists.
DO $$
DECLARE
  r RECORD;
  orphan_slice_id int;
BEGIN
  -- Every dimensional CHECK named constraint_<id> where <id> is not a current
  -- slice of the chunk.
  FOR r IN
    SELECT c.relid AS chunk_table,
           pc.conrelid, pc.conname AS stale_name, c.id AS chunk_id,
           (SELECT a.attname
              FROM pg_catalog.pg_attribute a
              WHERE a.attrelid = pc.conrelid
                AND a.attnum = pc.conkey[1]) AS stale_column
    FROM _timescaledb_catalog.chunk c
    JOIN pg_catalog.pg_constraint pc
      ON pc.conrelid = c.relid
     AND pc.contype = 'c'
     AND pc.conname ~ '^constraint_[0-9]+$'
    WHERE
      NOT EXISTS (
        SELECT 1 FROM _timescaledb_catalog.dimension_slice ds
        WHERE ds.chunk_id = c.id
          AND ds.id = substring(pc.conname FROM '^constraint_([0-9]+)$')::int)
  LOOP
    SELECT ds.id INTO orphan_slice_id
    FROM _timescaledb_catalog.dimension_slice ds
    JOIN _timescaledb_catalog.dimension d ON d.id = ds.dimension_id
    WHERE ds.chunk_id = r.chunk_id
      AND d.column_name = r.stale_column
      AND NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint pc
        WHERE pc.conrelid = r.conrelid
          AND pc.conname = pg_catalog.format('constraint_%s', ds.id)::name)
    LIMIT 1;

    IF orphan_slice_id IS NULL THEN
      EXECUTE pg_catalog.format('ALTER TABLE %s DROP CONSTRAINT %I',
                                r.chunk_table, r.stale_name);
    ELSE
      EXECUTE pg_catalog.format('ALTER TABLE %s RENAME CONSTRAINT %I TO %I',
                                r.chunk_table, r.stale_name,
                                pg_catalog.format('constraint_%s', orphan_slice_id));
    END IF;
  END LOOP;
END
$$;

--
-- END repair mismatched dimensional CHECK constraints
--
