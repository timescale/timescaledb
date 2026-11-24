-- Rebuild the catalog table `_timescaledb_catalog.continuous_aggs_hypertable_invalidation_log`
-- to add the `seqnum` column.
CREATE TABLE _timescaledb_catalog._tmp_continuous_aggs_hypertable_invalidation_log AS
    SELECT hypertable_id, lowest_modified_value, greatest_modified_value
    FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;

ALTER EXTENSION timescaledb
    DROP TABLE _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;
DROP TABLE _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;

CREATE TABLE _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log (
  hypertable_id integer NOT NULL,
  lowest_modified_value bigint NOT NULL,
  greatest_modified_value bigint NOT NULL,
  seqnum integer
);

INSERT INTO _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
    (hypertable_id, lowest_modified_value, greatest_modified_value, seqnum)
SELECT *, NULL::integer FROM _timescaledb_catalog._tmp_continuous_aggs_hypertable_invalidation_log;
DROP TABLE _timescaledb_catalog._tmp_continuous_aggs_hypertable_invalidation_log;

CREATE INDEX continuous_aggs_hypertable_invalidation_log_idx ON _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log (hypertable_id, lowest_modified_value ASC);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_aggs_hypertable_invalidation_log', '');
-- end rebuild _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log --

-- Rebuild the catalog table `_timescaledb_catalog.continuous_aggs_materialization_invalidation_log`
-- to add the `seqnum` column.
CREATE TABLE _timescaledb_catalog._tmp_continuous_aggs_materialization_invalidation_log AS
    SELECT materialization_id, lowest_modified_value, greatest_modified_value
    FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log;

ALTER EXTENSION timescaledb
    DROP TABLE _timescaledb_catalog.continuous_aggs_materialization_invalidation_log;
DROP TABLE _timescaledb_catalog.continuous_aggs_materialization_invalidation_log;

CREATE TABLE _timescaledb_catalog.continuous_aggs_materialization_invalidation_log (
  materialization_id integer,
  lowest_modified_value bigint NOT NULL,
  greatest_modified_value bigint NOT NULL,
  seqnum integer,
  CONSTRAINT continuous_aggs_materialization_invalid_materialization_id_fkey FOREIGN KEY (materialization_id) REFERENCES _timescaledb_catalog.continuous_agg (mat_hypertable_id) ON DELETE CASCADE
);

INSERT INTO _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
    (materialization_id, lowest_modified_value, greatest_modified_value, seqnum)
SELECT *, NULL::integer FROM _timescaledb_catalog._tmp_continuous_aggs_materialization_invalidation_log;
DROP TABLE _timescaledb_catalog._tmp_continuous_aggs_materialization_invalidation_log;

CREATE INDEX continuous_aggs_materialization_invalidation_log_idx ON _timescaledb_catalog.continuous_aggs_materialization_invalidation_log (materialization_id, lowest_modified_value ASC);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_aggs_materialization_invalidation_log', '');
-- end rebuild _timescaledb_catalog.continuous_aggs_materialization_invalidation_log --

-- Rebuild _timescaledb_catalog.continuous_agg to add granular_refresh_enabled.
DROP VIEW IF EXISTS timescaledb_experimental.policies;

ALTER TABLE _timescaledb_catalog.continuous_aggs_jobs_refresh_ranges
    DROP CONSTRAINT continuous_aggs_jobs_refresh_ranges_materialization_id_fkey;
ALTER TABLE _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
    DROP CONSTRAINT continuous_aggs_materialization_invalid_materialization_id_fkey;
ALTER TABLE _timescaledb_catalog.continuous_aggs_watermark
    DROP CONSTRAINT continuous_aggs_watermark_mat_hypertable_id_fkey;
-- We're dropping this FK and not recreating it since continuous_aggs_materialization_ranges
-- is being deprecated and will be removed in a future release.
ALTER TABLE _timescaledb_catalog.continuous_aggs_materialization_ranges
    DROP CONSTRAINT continuous_aggs_materialization_ranges_materialization_id_fkey;

ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.continuous_agg;

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
        materialized_only,
        schema_change_timestamp
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
    granular_refresh_enabled bool NOT NULL DEFAULT FALSE,
    CONSTRAINT continuous_agg_pkey PRIMARY KEY (mat_hypertable_id),
    CONSTRAINT continuous_agg_partial_view_schema_partial_view_name_key UNIQUE (partial_view_schema, partial_view_name),
    CONSTRAINT continuous_agg_user_view_schema_user_view_name_key UNIQUE (user_view_schema, user_view_name),
    CONSTRAINT continuous_agg_mat_hypertable_id_fkey
        FOREIGN KEY (mat_hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE,
    CONSTRAINT continuous_agg_raw_hypertable_id_fkey
        FOREIGN KEY (raw_hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE,
    CONSTRAINT continuous_agg_parent_mat_hypertable_id_fkey
        FOREIGN KEY (parent_mat_hypertable_id)
        REFERENCES _timescaledb_catalog.continuous_agg (mat_hypertable_id) ON DELETE CASCADE
);

INSERT INTO _timescaledb_catalog.continuous_agg (
    mat_hypertable_id,
    raw_hypertable_id,
    parent_mat_hypertable_id,
    user_view_schema,
    user_view_name,
    partial_view_schema,
    partial_view_name,
    direct_view_schema,
    direct_view_name,
    materialized_only,
    schema_change_timestamp
)
SELECT * FROM _timescaledb_catalog._tmp_continuous_agg;
DROP TABLE _timescaledb_catalog._tmp_continuous_agg;

CREATE INDEX continuous_agg_raw_hypertable_id_idx ON _timescaledb_catalog.continuous_agg (raw_hypertable_id);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_agg', '');

GRANT SELECT ON TABLE _timescaledb_catalog.continuous_agg TO PUBLIC;

ALTER TABLE _timescaledb_catalog.continuous_aggs_jobs_refresh_ranges
    ADD CONSTRAINT continuous_aggs_jobs_refresh_ranges_materialization_id_fkey
        FOREIGN KEY (materialization_id) REFERENCES _timescaledb_catalog.continuous_agg(mat_hypertable_id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
    ADD CONSTRAINT continuous_aggs_materialization_invalid_materialization_id_fkey
        FOREIGN KEY (materialization_id) REFERENCES _timescaledb_catalog.continuous_agg(mat_hypertable_id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.continuous_aggs_watermark
    ADD CONSTRAINT continuous_aggs_watermark_mat_hypertable_id_fkey
        FOREIGN KEY (mat_hypertable_id) REFERENCES _timescaledb_catalog.continuous_agg(mat_hypertable_id) ON DELETE CASCADE;
-- end rebuild _timescaledb_catalog.continuous_agg --

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

-- Remove the user_catalog_table option from the catalog tables.
DO $$
BEGIN
  IF EXISTS (SELECT FROM pg_catalog.pg_class c WHERE oid = '_timescaledb_catalog.hypertable'::regclass AND 'user_catalog_table=true' = ANY(reloptions)) THEN
    ALTER TABLE _timescaledb_catalog.hypertable RESET (user_catalog_table);
  END IF;
  IF EXISTS (SELECT FROM pg_catalog.pg_class c WHERE oid = '_timescaledb_catalog.chunk'::regclass AND 'user_catalog_table=true' = ANY(reloptions)) THEN
    ALTER TABLE _timescaledb_catalog.chunk RESET (user_catalog_table);
  END IF;
END
$$;

-- Promote the (job_id, chunk_id) unique constraint on the policy chunk stats
-- table to a primary key to avoid problems with publications.
ALTER TABLE _timescaledb_internal.bgw_policy_chunk_stats
  DROP CONSTRAINT bgw_policy_chunk_stats_job_id_chunk_id_key,
  ADD CONSTRAINT bgw_policy_chunk_stats_job_id_chunk_id_key PRIMARY KEY (job_id, chunk_id);

-- Anyone who hit this problem could work around it by pointing the replica identity at
-- the unique index. Dropping that index leaves the setting behind naming
-- nothing, so send it back to the new primary key.
DO $$
BEGIN
  IF (SELECT relreplident FROM pg_class
      WHERE oid = '_timescaledb_internal.bgw_policy_chunk_stats'::regclass) = 'i'
     AND NOT EXISTS (
       SELECT FROM pg_index
       WHERE indrelid = '_timescaledb_internal.bgw_policy_chunk_stats'::regclass
         AND indisreplident)
  THEN
    ALTER TABLE _timescaledb_internal.bgw_policy_chunk_stats
      REPLICA IDENTITY DEFAULT;
  END IF;
END
$$;
