DROP VIEW IF EXISTS timescaledb_information.dimensions;
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

-- Add new time_bucket_gapfill variants with offset and origin parameters

-- Integer variants with offset (5 args)
CREATE OR REPLACE FUNCTION @extschema@.time_bucket_gapfill(bucket_width SMALLINT, ts SMALLINT, start SMALLINT, finish SMALLINT, "offset" SMALLINT) RETURNS SMALLINT
	AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION @extschema@.time_bucket_gapfill(bucket_width INT, ts INT, start INT, finish INT, "offset" INT) RETURNS INT
	AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION @extschema@.time_bucket_gapfill(bucket_width BIGINT, ts BIGINT, start BIGINT, finish BIGINT, "offset" BIGINT) RETURNS BIGINT
	AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C VOLATILE PARALLEL SAFE;

-- Timestamp variants with origin and offset (6 args)
CREATE OR REPLACE FUNCTION @extschema@.time_bucket_gapfill(bucket_width INTERVAL, ts DATE, start DATE, finish DATE, origin DATE, "offset" INTERVAL) RETURNS DATE
	AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION @extschema@.time_bucket_gapfill(bucket_width INTERVAL, ts TIMESTAMP, start TIMESTAMP, finish TIMESTAMP, origin TIMESTAMP, "offset" INTERVAL) RETURNS TIMESTAMP
	AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION @extschema@.time_bucket_gapfill(bucket_width INTERVAL, ts TIMESTAMPTZ, start TIMESTAMPTZ, finish TIMESTAMPTZ, origin TIMESTAMPTZ, "offset" INTERVAL) RETURNS TIMESTAMPTZ
	AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C VOLATILE PARALLEL SAFE;

-- Timezone variant with origin and offset (7 args)
CREATE OR REPLACE FUNCTION @extschema@.time_bucket_gapfill(bucket_width INTERVAL, ts TIMESTAMPTZ, timezone TEXT, start TIMESTAMPTZ, finish TIMESTAMPTZ, origin TIMESTAMPTZ, "offset" INTERVAL) RETURNS TIMESTAMPTZ
	AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C VOLATILE PARALLEL SAFE;
