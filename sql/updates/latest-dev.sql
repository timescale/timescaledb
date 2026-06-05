
-- block when adaptive chunking is in use
DO $$
BEGIN
  IF EXISTS (SELECT FROM _timescaledb_catalog.hypertable WHERE chunk_target_size > 0) THEN
    RAISE EXCEPTION 'cannot upgrade because there are hypertables using adaptive chunking'
      USING
        ERRCODE = 'object_not_in_prerequisite_state',
        DETAIL = format('Please disable adaptive chunking.');
  END IF;
END;
$$;

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

-- Add chunk_id to `_timescaledb_catalog.dimension_slice`
CREATE TABLE _timescaledb_internal.tmp_dimension_slice AS
    SELECT * FROM _timescaledb_catalog.dimension_slice;
CREATE TABLE _timescaledb_internal.tmp_dimension_slice_seq_value AS
    SELECT last_value, is_called FROM _timescaledb_catalog.dimension_slice_id_seq;

ALTER TABLE _timescaledb_catalog.chunk_constraint
    DROP CONSTRAINT IF EXISTS chunk_constraint_dimension_slice_id_fkey;

DROP VIEW IF EXISTS timescaledb_information.chunks;

ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.dimension_slice;
ALTER EXTENSION timescaledb DROP SEQUENCE _timescaledb_catalog.dimension_slice_id_seq;

DROP TABLE _timescaledb_catalog.dimension_slice;

CREATE TABLE _timescaledb_catalog.dimension_slice (
  id serial NOT NULL,
  chunk_id integer NOT NULL,
  dimension_id integer NOT NULL,
  range_start bigint NOT NULL,
  range_end bigint NOT NULL,
  CONSTRAINT dimension_slice_pkey PRIMARY KEY (id),
  CONSTRAINT dimension_slice_chunk_id_dimension_id_key UNIQUE (chunk_id, dimension_id),
  CONSTRAINT dimension_slice_check CHECK (range_start <= range_end),
  CONSTRAINT dimension_slice_chunk_id_fkey FOREIGN KEY (chunk_id) REFERENCES _timescaledb_catalog.chunk (id) ON DELETE CASCADE,
  CONSTRAINT dimension_slice_dimension_id_fkey FOREIGN KEY (dimension_id) REFERENCES _timescaledb_catalog.dimension (id) ON DELETE CASCADE
);

CREATE INDEX dimension_slice_dimension_id_range_start_range_end_idx
    ON _timescaledb_catalog.dimension_slice (dimension_id, range_start, range_end);

-- One fresh slice row per (chunk_id, old_slice) pair, derived from the
-- existing chunk_constraint mapping. Slices that were shared across
-- chunks become per-chunk duplicates here.
INSERT INTO _timescaledb_catalog.dimension_slice (chunk_id, dimension_id, range_start, range_end)
SELECT cc.chunk_id, tmp.dimension_id, tmp.range_start, tmp.range_end
FROM _timescaledb_catalog.chunk_constraint cc
JOIN _timescaledb_internal.tmp_dimension_slice tmp ON tmp.id = cc.dimension_slice_id
WHERE cc.dimension_slice_id IS NOT NULL;

ALTER SEQUENCE _timescaledb_catalog.dimension_slice_id_seq OWNED BY _timescaledb_catalog.dimension_slice.id;
SELECT setval('_timescaledb_catalog.dimension_slice_id_seq',
              GREATEST((SELECT last_value FROM _timescaledb_internal.tmp_dimension_slice_seq_value),
                       COALESCE((SELECT max(id) FROM _timescaledb_catalog.dimension_slice), 0)),
              true);

-- Rename each chunk-side dimensional CHECK from constraint_<old_slice_id> to
-- constraint_<new_slice_id> so the on-disk name matches the id stored in the
-- rebuilt dimension_slice. Without this, code that derives the name from
-- slice->fd.id (chunk_constraints_recreate, chunk detach, merge-on-dimension,
-- the ALTER TABLE DROP CONSTRAINT guard) fails to locate the existing CHECK.
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT pg_catalog.format('%I.%I', c.schema_name, c.table_name) AS chunk_table,
               format('constraint_%s', cc.dimension_slice_id)::name AS old_name,
               format('constraint_%s', ds.id)::name AS new_name
        FROM _timescaledb_catalog.chunk_constraint cc
        JOIN _timescaledb_internal.tmp_dimension_slice tmp
            ON tmp.id = cc.dimension_slice_id
        JOIN _timescaledb_catalog.chunk c ON c.id = cc.chunk_id
        JOIN _timescaledb_catalog.dimension_slice ds
            ON ds.chunk_id = cc.chunk_id
           AND ds.dimension_id = tmp.dimension_id
        WHERE cc.dimension_slice_id IS NOT NULL
          AND cc.dimension_slice_id <> ds.id
          AND EXISTS (
              SELECT 1 FROM pg_constraint pc
              WHERE pc.conrelid = pg_catalog.format('%I.%I', c.schema_name, c.table_name)::regclass
                AND pc.conname = format('constraint_%s', cc.dimension_slice_id)::name
                AND pc.contype = 'c'
          )
    LOOP
        EXECUTE pg_catalog.format('ALTER TABLE %s RENAME CONSTRAINT %I TO %I',
                                  r.chunk_table, r.old_name, r.new_name);
    END LOOP;
END
$$;

DROP TABLE _timescaledb_internal.tmp_dimension_slice;
DROP TABLE _timescaledb_internal.tmp_dimension_slice_seq_value;

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.dimension_slice', '');
SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.dimension_slice', 'id'), '');

GRANT SELECT ON _timescaledb_catalog.dimension_slice TO PUBLIC;
GRANT SELECT ON _timescaledb_catalog.dimension_slice_id_seq TO PUBLIC;
-- end rebuild _timescaledb_catalog.dimension_slice table --

-- Drop the chunk_constraint catalog. Dimensional ownership lives on
-- dimension_slice.chunk_id now and non-dimensional constraints are
-- located by name on the chunk relation. The PL/pgSQL helper that
-- took a chunk_constraint row gets replaced by one with scalar args.
DROP FUNCTION _timescaledb_functions.chunk_constraint_add_table_constraint(
    _timescaledb_catalog.chunk_constraint);
DROP FUNCTION IF EXISTS _timescaledb_internal.chunk_constraint_add_table_constraint(
    _timescaledb_catalog.chunk_constraint);
DROP VIEW IF EXISTS timescaledb_information.chunks;

ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.chunk_constraint;
ALTER EXTENSION timescaledb DROP SEQUENCE _timescaledb_catalog.chunk_constraint_name;

DROP TABLE _timescaledb_catalog.chunk_constraint;
DROP SEQUENCE _timescaledb_catalog.chunk_constraint_name;

-- remove adaptive chunking
DROP FUNCTION IF EXISTS @extschema@.set_adaptive_chunking(regclass, text, regproc);
DROP FUNCTION IF EXISTS @extschema@.create_hypertable(relation REGCLASS, time_column_name NAME, partitioning_column NAME, number_partitions INTEGER, associated_schema_name NAME, associated_table_prefix NAME, chunk_time_interval ANYELEMENT, create_default_indexes BOOLEAN, if_not_exists BOOLEAN, partitioning_func REGPROC, migrate_data BOOLEAN, chunk_target_size TEXT, chunk_sizing_func REGPROC, time_partitioning_func REGPROC);
DROP FUNCTION IF EXISTS _timescaledb_internal.calculate_chunk_interval(integer, bigint, bigint);
DROP FUNCTION IF EXISTS _timescaledb_functions.calculate_chunk_interval(integer, bigint, bigint);


-- chunk_target_size is no longer used, so its check constraint is dropped.
ALTER TABLE _timescaledb_catalog.hypertable
    DROP CONSTRAINT IF EXISTS hypertable_chunk_target_size_check;

-- Rebuild the catalog table `_timescaledb_catalog.continuous_agg` to add the
-- `schema_change_timestamp` column.

-- Drop views and foreign keys that depend on the catalog table.
DROP VIEW IF EXISTS timescaledb_experimental.policies;
DROP VIEW IF EXISTS timescaledb_information.hypertables;
DROP VIEW IF EXISTS timescaledb_information.continuous_aggregates;
DROP VIEW IF EXISTS timescaledb_information.jobs;

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
  schema_change_timestamp bigint,
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
    (mat_hypertable_id, raw_hypertable_id, parent_mat_hypertable_id, user_view_schema,
     user_view_name, partial_view_schema, partial_view_name, direct_view_schema,
     direct_view_name, materialized_only)
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
