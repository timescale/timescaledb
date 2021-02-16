-- Recreate missing dimension slices that might be missing. If the
-- dimension slice table is broken and there are dimension slices
-- missing from the table, we will repair it by:
--
--    1. Finding all chunk constraints that have missing dimension
--       slices and extract the constraint expression from the
--       associated constraint.
--
--    2. Parse the constraint expression and extract the column name,
--       and upper and lower range values as text or, if it is a
--       partition constraint, pick the existing constraint (either
--       uppper or lower end of range) and make the other end open.
--
--    3. Use the column type to construct the range values (UNIX
--       microseconds) from these strings.
INSERT INTO _timescaledb_catalog.dimension_slice
WITH
   -- All dimension slices that are mentioned in the chunk_constraint
   -- table but are missing from the dimension_slice table.
   missing_slices AS (
      SELECT hypertable_id,
             chunk_id,
             dimension_slice_id,
             constraint_name,
             attname AS column_name,
             pg_get_expr(conbin, conrelid) AS constraint_expr
      FROM _timescaledb_catalog.chunk_constraint cc
      JOIN _timescaledb_catalog.chunk ch ON cc.chunk_id = ch.id
      JOIN pg_constraint ON conname = constraint_name
      JOIN pg_namespace ns ON connamespace = ns.oid AND ns.nspname = ch.schema_name
      JOIN pg_attribute ON attnum = conkey[1] AND attrelid = conrelid
      WHERE
         dimension_slice_id NOT IN (SELECT id FROM _timescaledb_catalog.dimension_slice)
   ),

  -- Unparsed range start and end for each dimension slice id that
  -- is missing.
   unparsed_missing_slices AS (
      SELECT di.id AS dimension_id,
             dimension_slice_id,
             constraint_name,
             column_type,
             column_name,
             (SELECT SUBSTRING(constraint_expr, $$>=\s*'?([\w\d\s:+-]+)'?$$)) AS range_start,
             (SELECT SUBSTRING(constraint_expr, $$<\s*'?([\w\d\s:+-]+)'?$$)) AS range_end
        FROM missing_slices JOIN _timescaledb_catalog.dimension di USING (hypertable_id, column_name)
   )
SELECT DISTINCT
       dimension_slice_id,
       dimension_id,
       CASE
       WHEN column_type = 'timestamptz'::regtype THEN
            EXTRACT(EPOCH FROM range_start::timestamptz)::bigint * 1000000
       WHEN column_type = 'timestamp'::regtype THEN
            EXTRACT(EPOCH FROM range_start::timestamp)::bigint * 1000000
       WHEN column_type = 'date'::regtype THEN
            EXTRACT(EPOCH FROM range_start::date)::bigint * 1000000
       ELSE
            CASE
            WHEN range_start IS NULL
            THEN (-9223372036854775808)::bigint
            ELSE range_start::bigint
            END
       END AS range_start,
       CASE
       WHEN column_type = 'timestamptz'::regtype THEN
            EXTRACT(EPOCH FROM range_end::timestamptz)::bigint * 1000000
       WHEN column_type = 'timestamp'::regtype THEN
            EXTRACT(EPOCH FROM range_end::timestamp)::bigint * 1000000
       WHEN column_type = 'date'::regtype THEN
            EXTRACT(EPOCH FROM range_end::date)::bigint * 1000000
       ELSE
            CASE WHEN range_end IS NULL
            THEN 9223372036854775807::bigint
            ELSE range_end::bigint
            END
       END AS range_end
  FROM unparsed_missing_slices;

-- set compressed_chunk_id to NULL for dropped chunks
UPDATE _timescaledb_catalog.chunk SET compressed_chunk_id = NULL WHERE dropped = true AND compressed_chunk_id IS NOT NULL;
