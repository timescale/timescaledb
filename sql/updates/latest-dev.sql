DROP VIEW IF EXISTS timescaledb_information.dimensions;

-- Drop old function signatures that are being replaced with new signatures
-- that include origin parameter
DROP FUNCTION IF EXISTS @extschema@.create_hypertable(
    regclass, name, name, integer, name, name, anyelement,
    boolean, boolean, regproc, boolean, text, regproc, regproc
);
DROP FUNCTION IF EXISTS @extschema@.set_chunk_time_interval(regclass, anyelement, name);
DROP FUNCTION IF EXISTS @extschema@.set_partitioning_interval(regclass, anyelement, name);
DROP FUNCTION IF EXISTS @extschema@.add_dimension(regclass, name, integer, anyelement, regproc, boolean);
DROP FUNCTION IF EXISTS @extschema@.by_range(name, anyelement, regproc);

-- Block update if CAggs in old format are found
DO
$$
DECLARE
  caggs text;
BEGIN
  SELECT string_agg(format('%I.%I', user_view_schema, user_view_name), ', ')
  INTO caggs
  FROM _timescaledb_catalog.continuous_agg
  WHERE finalized IS FALSE
  GROUP BY user_view_schema, user_view_name
  ORDER BY user_view_schema, user_view_name;

  IF caggs IS NOT NULL THEN
    RAISE
      EXCEPTION 'continuous aggregates with old format found, update blocked'
      USING
        DETAIL = format('Continuous Aggregates: %s', caggs),
        HINT = 'You should use `cagg_migrate` procedure to migrate to the new format.';
  END IF;
END
$$;

-- Block update if CAggs using time_bucket_ng are found
DO
$$
DECLARE
  caggs text;
BEGIN
  SELECT string_agg(pg_catalog.format('%I.%I', user_view_schema, user_view_name), ', ')
  INTO caggs
  FROM _timescaledb_catalog.continuous_agg cagg
  JOIN _timescaledb_catalog.continuous_aggs_bucket_function AS bf ON (cagg.mat_hypertable_id = bf.mat_hypertable_id)
  WHERE bf.bucket_func::text LIKE '%time_bucket_ng%';

  IF caggs IS NOT NULL THEN
    RAISE
      EXCEPTION 'continuous aggregates using time_bucket_ng found, update blocked'
      USING
        DETAIL = format('Continuous Aggregates: %s', caggs),
        HINT = 'time_bucket_ng has been removed. Please migrate the continuous aggregates using `cagg_migrate` before updating';
  END IF;
END
$$;

--
-- Rebuild the catalog table `_timescaledb_catalog.continuous_agg` to remove `finalized` column
--

-- (1) Remove cagg migration functions and procedures from public and internal schemas
DROP PROCEDURE IF EXISTS @extschema@.cagg_migrate (REGCLASS, BOOLEAN, BOOLEAN);
DROP FUNCTION IF EXISTS _timescaledb_internal.cagg_migrate_pre_validation (TEXT, TEXT, TEXT);
DROP FUNCTION IF EXISTS _timescaledb_functions.cagg_migrate_pre_validation (TEXT, TEXT, TEXT);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_create_plan (_timescaledb_catalog.continuous_agg, TEXT, BOOLEAN, BOOLEAN);
DROP PROCEDURE IF EXISTS _timescaledb_functions.cagg_migrate_create_plan (_timescaledb_catalog.continuous_agg, TEXT, BOOLEAN, BOOLEAN);

DROP FUNCTION IF EXISTS _timescaledb_internal.cagg_migrate_plan_exists (INTEGER);
DROP FUNCTION IF EXISTS _timescaledb_functions.cagg_migrate_plan_exists (INTEGER);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_execute_plan (_timescaledb_catalog.continuous_agg);
DROP PROCEDURE IF EXISTS _timescaledb_functions.cagg_migrate_execute_plan (_timescaledb_catalog.continuous_agg);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_execute_create_new_cagg (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_functions.cagg_migrate_execute_create_new_cagg (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_execute_disable_policies (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_functions.cagg_migrate_execute_disable_policies (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_execute_enable_policies (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_functions.cagg_migrate_execute_enable_policies (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_execute_copy_policies (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_functions.cagg_migrate_execute_copy_policies (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_execute_refresh_new_cagg (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_functions.cagg_migrate_execute_refresh_new_cagg (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_functions.cagg_migrate_update_watermark(integer);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_execute_copy_data (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_functions.cagg_migrate_execute_copy_data (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_execute_override_cagg (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_functions.cagg_migrate_execute_override_cagg (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_execute_drop_old_cagg (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_functions.cagg_migrate_execute_drop_old_cagg (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);

-- (2) Rebuild catalog table
DROP VIEW IF EXISTS timescaledb_experimental.policies;
DROP VIEW IF EXISTS timescaledb_information.hypertables;
DROP VIEW IF EXISTS timescaledb_information.continuous_aggregates;
DROP VIEW IF EXISTS timescaledb_information.jobs;

ALTER TABLE _timescaledb_catalog.continuous_aggs_materialization_ranges
    DROP CONSTRAINT continuous_aggs_materialization_ranges_materialization_id_fkey;

ALTER TABLE _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
    DROP CONSTRAINT continuous_aggs_materialization_invalid_materialization_id_fkey;

ALTER TABLE _timescaledb_catalog.continuous_aggs_watermark
    DROP CONSTRAINT continuous_aggs_watermark_mat_hypertable_id_fkey;

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
    CONSTRAINT continuous_agg_mat_hypertable_id_fkey
        FOREIGN KEY (mat_hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE,
    CONSTRAINT continuous_agg_raw_hypertable_id_fkey
        FOREIGN KEY (raw_hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE,
    CONSTRAINT continuous_agg_parent_mat_hypertable_id_fkey
        FOREIGN KEY (parent_mat_hypertable_id)
        REFERENCES _timescaledb_catalog.continuous_agg (mat_hypertable_id) ON DELETE CASCADE
);

INSERT INTO _timescaledb_catalog.continuous_agg
SELECT * FROM _timescaledb_catalog._tmp_continuous_agg;
DROP TABLE _timescaledb_catalog._tmp_continuous_agg;

CREATE INDEX continuous_agg_raw_hypertable_id_idx ON _timescaledb_catalog.continuous_agg (raw_hypertable_id);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_agg', '');

GRANT SELECT ON TABLE _timescaledb_catalog.continuous_agg TO PUBLIC;

ALTER TABLE _timescaledb_catalog.continuous_aggs_materialization_ranges
    ADD CONSTRAINT continuous_aggs_materialization_ranges_materialization_id_fkey
        FOREIGN KEY (materialization_id)
        REFERENCES _timescaledb_catalog.continuous_agg(mat_hypertable_id) ON DELETE CASCADE;

ALTER TABLE _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
    ADD CONSTRAINT continuous_aggs_materialization_invalid_materialization_id_fkey
        FOREIGN KEY (materialization_id)
        REFERENCES _timescaledb_catalog.continuous_agg(mat_hypertable_id) ON DELETE CASCADE;

ALTER TABLE _timescaledb_catalog.continuous_aggs_watermark
    ADD CONSTRAINT continuous_aggs_watermark_mat_hypertable_id_fkey
        FOREIGN KEY (mat_hypertable_id)
        REFERENCES _timescaledb_catalog.continuous_agg (mat_hypertable_id) ON DELETE CASCADE;

ANALYZE _timescaledb_catalog.continuous_agg;

--
-- END Rebuild the catalog table `_timescaledb_catalog.continuous_agg`
--

DROP FUNCTION IF EXISTS _timescaledb_debug.extension_state();
DROP SCHEMA IF EXISTS _timescaledb_debug;

ALTER TABLE _timescaledb_config.bgw_job SET SCHEMA _timescaledb_catalog;
DROP SCHEMA IF EXISTS _timescaledb_config;

-- Remove legacy partialize/finalize aggregate functions. It should be
-- conditional because on 2.12.0 we moved from internal to functions schema
DO
$$
DECLARE
    foid regprocedure;
    fkind text;
    fargs text;
    funcs text[] = '{finalize_agg, finalize_agg_sfunc, finalize_agg_ffunc, partialize_agg}';
BEGIN
    FOR foid, fkind, fargs IN
        SELECT
            p.oid,
            CASE
                WHEN p.prokind = 'f' THEN 'FUNCTION'
                WHEN p.prokind = 'a' THEN 'AGGREGATE'
                ELSE 'PROCEDURE'
            END,
            pg_catalog.pg_get_function_arguments(p.oid)
        FROM
            pg_catalog.pg_proc AS p
        WHERE
            p.proname = ANY(funcs)
            AND p.pronamespace IN ('_timescaledb_internal'::regnamespace, '_timescaledb_functions'::regnamespace)
        ORDER BY
            p.proname
    LOOP
        EXECUTE format('ALTER EXTENSION timescaledb DROP %s %s (%s);', fkind, foid::regproc, fargs);
        EXECUTE format('DROP %s %s (%s);', fkind, foid::regproc, fargs);
    END LOOP;
END;
$$
LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS _timescaledb_functions.cagg_parse_invalidation_record(bytea);
DROP FUNCTION IF EXISTS _timescaledb_functions.get_hypertable_id(regclass, regtype);
DROP FUNCTION IF EXISTS _timescaledb_functions.get_hypertable_invalidations(regclass,timestamp without time zone,interval[]);
DROP FUNCTION IF EXISTS _timescaledb_functions.get_hypertable_invalidations(regclass,timestamp with time zone,interval[]);
DROP FUNCTION IF EXISTS _timescaledb_functions.get_materialization_info(regclass);
DROP FUNCTION IF EXISTS _timescaledb_functions.get_materialization_invalidations(regclass,tsrange);
DROP FUNCTION IF EXISTS _timescaledb_functions.get_materialization_invalidations(regclass,tstzrange);
DROP FUNCTION IF EXISTS _timescaledb_functions.get_raw_materialization_ranges(regtype);
DROP FUNCTION IF EXISTS _timescaledb_functions.invalidation_plugin_name();
DROP PROCEDURE IF EXISTS _timescaledb_functions.accept_hypertable_invalidations(regclass,text);
DROP PROCEDURE IF EXISTS _timescaledb_functions.add_materialization_invalidations(regclass,tsrange);
DROP PROCEDURE IF EXISTS _timescaledb_functions.add_materialization_invalidations(regclass,tstzrange);
DROP PROCEDURE IF EXISTS _timescaledb_functions.process_hypertable_invalidations(name);
DROP PROCEDURE IF EXISTS _timescaledb_functions.process_hypertable_invalidations(regclass);
DROP PROCEDURE IF EXISTS _timescaledb_functions.process_hypertable_invalidations(regclass[]);

-- Remove orphaned entries in materialization ranges table
DELETE FROM _timescaledb_catalog.continuous_aggs_materialization_ranges
WHERE NOT EXISTS (SELECT FROM _timescaledb_catalog.continuous_agg WHERE mat_hypertable_id = materialization_id);

DROP PROCEDURE IF EXISTS _timescaledb_functions.cagg_migrate_to_time_bucket(regclass);

DROP FUNCTION timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts DATE);

DROP FUNCTION timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts DATE, origin DATE);

DROP FUNCTION timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts TIMESTAMP);

DROP FUNCTION timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts TIMESTAMP, origin TIMESTAMP);

DROP FUNCTION timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts TIMESTAMPTZ, timezone TEXT);

DROP FUNCTION timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts TIMESTAMPTZ, origin TIMESTAMPTZ, timezone TEXT);

DROP FUNCTION timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts TIMESTAMPTZ);

DROP FUNCTION timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts TIMESTAMPTZ, origin TIMESTAMPTZ);

--
-- Rebuild the catalog table `_timescaledb_catalog.dimension` to add interval_origin column
--

-- Drop views that depend on the dimension table
DROP VIEW IF EXISTS timescaledb_information.hypertable_columnstore_settings;
DROP VIEW IF EXISTS timescaledb_information.hypertable_compression_settings;
DROP VIEW IF EXISTS timescaledb_information.chunks;

-- Drop foreign key constraints referencing dimension table
ALTER TABLE _timescaledb_catalog.dimension_slice
    DROP CONSTRAINT dimension_slice_dimension_id_fkey;

-- Drop the dimension table and its sequence from the extension so we can rebuild it
ALTER EXTENSION timescaledb
    DROP TABLE _timescaledb_catalog.dimension;
ALTER EXTENSION timescaledb
    DROP SEQUENCE _timescaledb_catalog.dimension_id_seq;

-- Save existing data with new column
CREATE TABLE _timescaledb_catalog._tmp_dimension AS
    SELECT
        id,
        hypertable_id,
        column_name,
        column_type,
        aligned,
        num_slices,
        partitioning_func_schema,
        partitioning_func,
        NULL::bigint AS interval_origin,
        interval_length,
        compress_interval_length,
        integer_now_func_schema,
        integer_now_func
    FROM
        _timescaledb_catalog.dimension
    ORDER BY
        id;

-- Drop old table
DROP TABLE _timescaledb_catalog.dimension;

-- Create new table with interval_origin column
CREATE TABLE _timescaledb_catalog.dimension (
    id serial NOT NULL,
    hypertable_id integer NOT NULL,
    column_name name NOT NULL,
    column_type REGTYPE NOT NULL,
    aligned boolean NOT NULL,
    -- closed dimensions
    num_slices smallint NULL,
    partitioning_func_schema name NULL,
    partitioning_func name NULL,
    -- open dimensions (e.g., time)
    interval_origin bigint NULL,
    interval_length bigint NULL,
    -- compress interval for rollup during compression
    compress_interval_length bigint NULL,
    integer_now_func_schema name NULL,
    integer_now_func name NULL,
    -- table constraints
    CONSTRAINT dimension_pkey PRIMARY KEY (id),
    CONSTRAINT dimension_hypertable_id_column_name_key UNIQUE (hypertable_id, column_name),
    CONSTRAINT dimension_check CHECK ((partitioning_func_schema IS NULL AND partitioning_func IS NULL) OR (partitioning_func_schema IS NOT NULL AND partitioning_func IS NOT NULL)),
    CONSTRAINT dimension_check1 CHECK ((num_slices IS NULL AND interval_length IS NOT NULL) OR (num_slices IS NOT NULL AND interval_length IS NULL)),
    CONSTRAINT dimension_check2 CHECK ((integer_now_func_schema IS NULL AND integer_now_func IS NULL) OR (integer_now_func_schema IS NOT NULL AND integer_now_func IS NOT NULL)),
    CONSTRAINT dimension_interval_length_check CHECK (interval_length IS NULL OR interval_length > 0),
    CONSTRAINT dimension_compress_interval_length_check CHECK (compress_interval_length IS NULL OR compress_interval_length > 0),
    CONSTRAINT dimension_hypertable_id_fkey FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE
);

-- Copy data from temp table
INSERT INTO _timescaledb_catalog.dimension
SELECT * FROM _timescaledb_catalog._tmp_dimension;

-- Drop temp table
DROP TABLE _timescaledb_catalog._tmp_dimension;

-- Restore sequence value
SELECT setval(pg_get_serial_sequence('_timescaledb_catalog.dimension', 'id'),
              max(id), true)
FROM _timescaledb_catalog.dimension;

-- Re-add foreign key constraint
ALTER TABLE _timescaledb_catalog.dimension_slice
    ADD CONSTRAINT dimension_slice_dimension_id_fkey
        FOREIGN KEY (dimension_id) REFERENCES _timescaledb_catalog.dimension (id) ON DELETE CASCADE;

-- Register for pg_dump
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.dimension', '');
SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.dimension', 'id'), '');

GRANT SELECT ON TABLE _timescaledb_catalog.dimension TO PUBLIC;
GRANT SELECT ON SEQUENCE _timescaledb_catalog.dimension_id_seq TO PUBLIC;

ANALYZE _timescaledb_catalog.dimension;

--
-- END Rebuild the catalog table `_timescaledb_catalog.dimension`
--
