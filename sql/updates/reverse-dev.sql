
-- Warn about firstlast indexes when downgrading.
DO $$
DECLARE
  affected text;
BEGIN
  SELECT string_agg(DISTINCT relid::text, ', ')
  INTO affected
  FROM _timescaledb_catalog.compression_settings
  WHERE index @> '[{"type": "firstlast"}]';

  IF affected IS NOT NULL THEN
    RAISE WARNING 'Before upgrading again you have to recompress chunks with firstlast indexes.'
      USING
        DETAIL = format('The following chunk use firstlast sparse indexes: %s', affected);
  END IF;
END
$$;

DROP VIEW IF EXISTS _timescaledb_catalog.chunk_constraint;
DROP FUNCTION IF EXISTS _timescaledb_functions.chunk_constraint_add_table_constraint( integer, name, name);

ALTER TABLE _timescaledb_catalog.hypertable RESET (user_catalog_table);
ALTER TABLE _timescaledb_catalog.chunk RESET (user_catalog_table);

--
-- Rebuild the catalog table `_timescaledb_catalog.dimension_slice` back to
-- the pre-chunk_id schema. Per-chunk slice duplicates are collapsed back
-- into one shared row per (dimension_id, range_start, range_end), and the
-- old UNIQUE on that triple is restored.
--
CREATE TABLE _timescaledb_internal.tmp_dimension_slice AS
    SELECT * FROM _timescaledb_catalog.dimension_slice;
CREATE TABLE _timescaledb_internal.tmp_dimension_slice_seq_value AS
    SELECT last_value, is_called FROM _timescaledb_catalog.dimension_slice_id_seq;

ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.dimension_slice;
ALTER EXTENSION timescaledb DROP SEQUENCE _timescaledb_catalog.dimension_slice_id_seq;

DROP TABLE _timescaledb_catalog.dimension_slice;

CREATE TABLE _timescaledb_catalog.dimension_slice (
  id serial NOT NULL,
  dimension_id integer NOT NULL,
  range_start bigint NOT NULL,
  range_end bigint NOT NULL,
  CONSTRAINT dimension_slice_pkey PRIMARY KEY (id),
  CONSTRAINT dimension_slice_dimension_id_range_start_range_end_key UNIQUE (dimension_id, range_start, range_end),
  CONSTRAINT dimension_slice_check CHECK (range_start <= range_end),
  CONSTRAINT dimension_slice_dimension_id_fkey FOREIGN KEY (dimension_id) REFERENCES _timescaledb_catalog.dimension (id) ON DELETE CASCADE
);

-- One row per unique (dimension_id, range_start, range_end), reusing the
-- lowest old id so the chunk-side CHECK named constraint_<min_id> keeps the
-- correct name without renaming.
INSERT INTO _timescaledb_catalog.dimension_slice (id, dimension_id, range_start, range_end)
SELECT min(id), dimension_id, range_start, range_end
FROM _timescaledb_internal.tmp_dimension_slice
GROUP BY dimension_id, range_start, range_end;

-- Rename chunk-side CHECK constraints from constraint_<old_id> to
-- constraint_<kept_id> for chunks whose per-chunk slice was collapsed onto
-- a shared row. Keeps the legacy invariant
-- constraint_name == 'constraint_<dimension_slice_id>'.
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT pg_catalog.format('%I.%I', c.schema_name, c.table_name) AS chunk_table,
               format('constraint_%s', tmp.id)::name AS old_name,
               format('constraint_%s', ds.id)::name AS new_name
        FROM _timescaledb_internal.tmp_dimension_slice tmp
        JOIN _timescaledb_catalog.chunk c ON c.id = tmp.chunk_id
        JOIN _timescaledb_catalog.dimension_slice ds
            ON ds.dimension_id = tmp.dimension_id
           AND ds.range_start = tmp.range_start
           AND ds.range_end = tmp.range_end
        WHERE tmp.id <> ds.id
          AND EXISTS (
              SELECT 1 FROM pg_constraint pc
              WHERE pc.conrelid = pg_catalog.format('%I.%I', c.schema_name, c.table_name)::regclass
                AND pc.conname = format('constraint_%s', tmp.id)::name
                AND pc.contype = 'c'
          )
    LOOP
        EXECUTE pg_catalog.format('ALTER TABLE %s RENAME CONSTRAINT %I TO %I',
                                  r.chunk_table, r.old_name, r.new_name);
    END LOOP;
END
$$;

ALTER SEQUENCE _timescaledb_catalog.dimension_slice_id_seq OWNED BY _timescaledb_catalog.dimension_slice.id;
SELECT setval('_timescaledb_catalog.dimension_slice_id_seq', last_value, is_called)
    FROM _timescaledb_internal.tmp_dimension_slice_seq_value;

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.dimension_slice', '');
SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.dimension_slice', 'id'), '');

GRANT SELECT ON _timescaledb_catalog.dimension_slice TO PUBLIC;
GRANT SELECT ON _timescaledb_catalog.dimension_slice_id_seq TO PUBLIC;
-- end rebuild _timescaledb_catalog.dimension_slice table --

-- Recreate the chunk_constraint catalog table.
CREATE TABLE _timescaledb_catalog.chunk_constraint (
  chunk_id integer NOT NULL,
  dimension_slice_id integer NULL,
  constraint_name name NOT NULL,
  hypertable_constraint_name name NULL,
  CONSTRAINT chunk_constraint_chunk_id_constraint_name_key UNIQUE (chunk_id, constraint_name),
  CONSTRAINT chunk_constraint_chunk_id_fkey FOREIGN KEY (chunk_id) REFERENCES _timescaledb_catalog.chunk (id)
);

CREATE INDEX chunk_constraint_dimension_slice_id_idx
    ON _timescaledb_catalog.chunk_constraint (dimension_slice_id);

CREATE SEQUENCE _timescaledb_catalog.chunk_constraint_name;

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_constraint', '');
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_constraint_name', '');

GRANT SELECT ON _timescaledb_catalog.chunk_constraint TO PUBLIC;
GRANT SELECT ON _timescaledb_catalog.chunk_constraint_name TO PUBLIC;

-- Restore dimensional rows. Join the per-chunk snapshot to the rebuilt
-- (deduplicated) dimension_slice so each chunk_constraint row points
-- directly at the kept slice id.
INSERT INTO _timescaledb_catalog.chunk_constraint
    (chunk_id, dimension_slice_id, constraint_name, hypertable_constraint_name)
SELECT tmp.chunk_id, ds.id, format('constraint_%s', ds.id)::name, ''::name
FROM _timescaledb_internal.tmp_dimension_slice tmp
JOIN _timescaledb_catalog.dimension_slice ds
    ON ds.dimension_id = tmp.dimension_id
   AND ds.range_start = tmp.range_start
   AND ds.range_end = tmp.range_end;

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

ALTER TABLE _timescaledb_catalog.chunk_constraint
    ADD CONSTRAINT chunk_constraint_dimension_slice_id_fkey
        FOREIGN KEY (dimension_slice_id) REFERENCES _timescaledb_catalog.dimension_slice (id);

DROP TABLE _timescaledb_internal.tmp_dimension_slice;
DROP TABLE _timescaledb_internal.tmp_dimension_slice_seq_value;

-- Drop chunk stats related objects
DROP VIEW IF EXISTS timescaledb_information.stat_chunk_activity;
DROP FUNCTION IF EXISTS _timescaledb_functions.chunk_statistics(regclass, regclass, timestamptz);
DROP FUNCTION IF EXISTS _timescaledb_functions.chunk_statistics_reset();

DROP FUNCTION IF EXISTS @extschema@.create_hypertable(relation REGCLASS, time_column_name NAME, partitioning_column NAME, number_partitions INTEGER, associated_schema_name NAME, associated_table_prefix NAME, chunk_time_interval ANYELEMENT, create_default_indexes BOOLEAN, if_not_exists BOOLEAN, partitioning_func REGPROC, migrate_data BOOLEAN, time_partitioning_func REGPROC);

-- Restore the chunk_target_size check constraint
ALTER TABLE _timescaledb_catalog.hypertable
    ADD CONSTRAINT hypertable_chunk_target_size_check CHECK (chunk_target_size >= 0);

DROP FUNCTION IF EXISTS _timescaledb_functions.rebuild_sparse_index(REGCLASS, BOOLEAN);

-- Rebuild the catalog table `_timescaledb_catalog.continuous_agg` to drop the
-- `schema_change_timestamp` column.
DROP VIEW IF EXISTS timescaledb_experimental.policies;

ALTER TABLE _timescaledb_catalog.continuous_aggs_watermark
    DROP CONSTRAINT continuous_aggs_watermark_mat_hypertable_id_fkey;
ALTER TABLE _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
    DROP CONSTRAINT continuous_aggs_materialization_invalid_materialization_id_fkey;
ALTER TABLE _timescaledb_catalog.continuous_aggs_materialization_ranges
    DROP CONSTRAINT continuous_aggs_materialization_ranges_materialization_id_fkey;
ALTER TABLE _timescaledb_catalog.continuous_aggs_jobs_refresh_ranges
    DROP CONSTRAINT continuous_aggs_jobs_refresh_ranges_materialization_id_fkey;

ALTER EXTENSION timescaledb
    DROP TABLE _timescaledb_catalog.continuous_agg;

CREATE TABLE _timescaledb_catalog._tmp_continuous_agg AS
    SELECT
        mat_hypertable_id,
        raw_hypertable_id,
        parent_mat_hypertable_id,
        user_view_schema,
        user_view_name,
        partial_view_schema,
        partial_view_name,
        direct_view_schema,
        direct_view_name,
        materialized_only
    FROM
        _timescaledb_catalog.continuous_agg
    ORDER BY
        mat_hypertable_id;

DROP TABLE _timescaledb_catalog.continuous_agg;

CREATE TABLE _timescaledb_catalog.continuous_agg (
  mat_hypertable_id integer NOT NULL,
  raw_hypertable_id integer NOT NULL,
  parent_mat_hypertable_id integer,
  user_view_schema name NOT NULL,
  user_view_name name NOT NULL,
  partial_view_schema name NOT NULL,
  partial_view_name name NOT NULL,
  direct_view_schema name NOT NULL,
  direct_view_name name NOT NULL,
  materialized_only bool NOT NULL DEFAULT FALSE,
  -- table constraints
  CONSTRAINT continuous_agg_pkey PRIMARY KEY (mat_hypertable_id),
  CONSTRAINT continuous_agg_partial_view_schema_partial_view_name_key UNIQUE (partial_view_schema, partial_view_name),
  CONSTRAINT continuous_agg_user_view_schema_user_view_name_key UNIQUE (user_view_schema, user_view_name),
  CONSTRAINT continuous_agg_mat_hypertable_id_fkey FOREIGN KEY (mat_hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE,
  CONSTRAINT continuous_agg_raw_hypertable_id_fkey FOREIGN KEY (raw_hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE,
  CONSTRAINT continuous_agg_parent_mat_hypertable_id_fkey FOREIGN KEY (parent_mat_hypertable_id)
    REFERENCES _timescaledb_catalog.continuous_agg (mat_hypertable_id) ON DELETE CASCADE
);

INSERT INTO _timescaledb_catalog.continuous_agg
SELECT * FROM _timescaledb_catalog._tmp_continuous_agg;
DROP TABLE _timescaledb_catalog._tmp_continuous_agg;

CREATE INDEX continuous_agg_raw_hypertable_id_idx ON _timescaledb_catalog.continuous_agg (raw_hypertable_id);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_agg', '');

GRANT SELECT ON TABLE _timescaledb_catalog.continuous_agg TO PUBLIC;

ALTER TABLE _timescaledb_catalog.continuous_aggs_watermark
    ADD CONSTRAINT continuous_aggs_watermark_mat_hypertable_id_fkey
        FOREIGN KEY (mat_hypertable_id)
        REFERENCES _timescaledb_catalog.continuous_agg (mat_hypertable_id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
    ADD CONSTRAINT continuous_aggs_materialization_invalid_materialization_id_fkey
        FOREIGN KEY (materialization_id)
        REFERENCES _timescaledb_catalog.continuous_agg (mat_hypertable_id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.continuous_aggs_materialization_ranges
    ADD CONSTRAINT continuous_aggs_materialization_ranges_materialization_id_fkey
        FOREIGN KEY (materialization_id)
        REFERENCES _timescaledb_catalog.continuous_agg (mat_hypertable_id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.continuous_aggs_jobs_refresh_ranges
    ADD CONSTRAINT continuous_aggs_jobs_refresh_ranges_materialization_id_fkey
        FOREIGN KEY (materialization_id)
        REFERENCES _timescaledb_catalog.continuous_agg (mat_hypertable_id) ON DELETE CASCADE;

ANALYZE _timescaledb_catalog.continuous_agg;
-- end rebuild _timescaledb_catalog.continuous_agg --

-- restore telemetry_event
CREATE TABLE _timescaledb_catalog.telemetry_event (
       created timestamptz NOT NULL DEFAULT current_timestamp,
       tag name NOT NULL,
       body jsonb NOT NULL
);
GRANT SELECT ON _timescaledb_catalog.telemetry_event TO PUBLIC;

DROP FUNCTION IF EXISTS _timescaledb_functions.get_hypertable_info;
DROP FUNCTION IF EXISTS _timescaledb_functions.get_hypertable_info_by_id;
DROP FUNCTION IF EXISTS _timescaledb_functions.get_primary_dimension;
DROP FUNCTION IF EXISTS _timescaledb_functions.get_chunk_info;
DROP FUNCTION IF EXISTS _timescaledb_functions.get_chunk_info_by_id;
DROP FUNCTION IF EXISTS _timescaledb_functions.get_chunk_primary_range;
DROP FUNCTION IF EXISTS _timescaledb_functions.get_chunk_primary_range_by_id;
DROP FUNCTION IF EXISTS _timescaledb_functions.get_integer_now_func;
DROP FUNCTION IF EXISTS _timescaledb_functions.lock_osm_chunk_dimension_slice(regclass);

DROP FUNCTION IF EXISTS _timescaledb_functions.decompress_batch(record);

