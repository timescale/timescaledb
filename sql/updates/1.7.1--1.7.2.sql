-- Recreate missing dimension slices that might be missing due to a bug
-- that is fixed in this release. If the dimension slice table is broken and there are dimension
-- slices missing from the table, we will repair it by:
--    1. Finding all chunk constraints that have missing dimension
--       slices and extract the constraint expression from the associated
--       constraint.
--    2. Parse the constraint expression and extract the column name,
--       and upper and lower range values as text.
--    3. Use the column type to construct the range values (UNIX
--       microseconds) from these values.
CREATE OR REPLACE FUNCTION _timescaledb_internal.time_to_internal(time_val ANYELEMENT)
RETURNS BIGINT AS '@MODULE_PATHNAME@', 'ts_time_to_internal' LANGUAGE C VOLATILE STRICT;

INSERT INTO _timescaledb_catalog.dimension_slice
WITH
   -- All dimension slices that are mentioned in the chunk_constraint
   -- table but are missing from the dimension_slice table. There can
   -- be duplicates since several chunk constraints can refer to one
   -- dimension slice.
   missing_slices AS (
      SELECT DISTINCT ch.hypertable_id,
             di.id as dimension_id,
             dimension_slice_id,
             constraint_name,
             di.column_type,
             attname AS column_name,
             pg_get_expr(conbin, conrelid) AS constraint_expr
      FROM _timescaledb_catalog.chunk_constraint cc
      JOIN _timescaledb_catalog.chunk ch ON cc.chunk_id = ch.id
      JOIN pg_constraint ON conname = constraint_name
      JOIN pg_namespace ns ON connamespace = ns.oid AND ns.nspname = ch.schema_name
      JOIN pg_attribute ON attnum = conkey[1] AND attrelid = conrelid
      JOIN _timescaledb_catalog.dimension di
           ON ch.hypertable_id = di.hypertable_id AND di.column_name = attname
      WHERE
         dimension_slice_id NOT IN (SELECT id FROM _timescaledb_catalog.dimension_slice)
   ),

  -- Unparsed range start and end for each dimension slice id that
  -- is missing.
   unparsed_missing_slices AS (
      SELECT dimension_id,
             dimension_slice_id,
             constraint_name,
             column_type,
             column_name,
             (SELECT SUBSTRING(constraint_expr, $$>=\s*'?([\d\s:+-]+)'?$$)) AS range_start,
             (SELECT SUBSTRING(constraint_expr, $$<\s*'?([\d\s:+-]+)'?$$)) AS range_end
        FROM missing_slices
   )
SELECT dimension_slice_id,
       dimension_id,
       CASE
       WHEN column_type IN ('smallint'::regtype, 'bigint'::regtype, 'integer'::regtype) THEN
            CASE
            WHEN range_start IS NULL
            THEN -9223372036854775808
            ELSE _timescaledb_internal.time_to_internal(range_start::bigint)
            END
       WHEN column_type = 'timestamptz'::regtype THEN
            _timescaledb_internal.time_to_internal(range_start::timestamptz)
       WHEN column_type = 'timestamp'::regtype THEN
            _timescaledb_internal.time_to_internal(range_start::timestamp)
       WHEN column_type = 'date'::regtype THEN
            _timescaledb_internal.time_to_internal(range_start::date)
       ELSE
            NULL
       END AS range_start,
       CASE
       WHEN column_type IN ('smallint'::regtype, 'bigint'::regtype, 'integer'::regtype) THEN
            CASE WHEN range_end IS NULL
            THEN 9223372036854775807
            ELSE _timescaledb_internal.time_to_internal(range_end::bigint)
            END
       WHEN column_type = 'timestamptz'::regtype THEN
            _timescaledb_internal.time_to_internal(range_end::timestamptz)
       WHEN column_type = 'timestamp'::regtype THEN
            _timescaledb_internal.time_to_internal(range_end::timestamp)
       WHEN column_type = 'date'::regtype THEN
            _timescaledb_internal.time_to_internal(range_end::date)
       ELSE NULL
       END AS range_end
  FROM unparsed_missing_slices;
