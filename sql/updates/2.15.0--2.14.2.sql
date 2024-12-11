DROP FUNCTION IF EXISTS _timescaledb_functions.remove_dropped_chunk_metadata(INTEGER);


--
-- Rebuild the catalog table `_timescaledb_catalog.continuous_agg`
--
DROP VIEW IF EXISTS timescaledb_experimental.policies;
DROP VIEW IF EXISTS timescaledb_information.hypertables;
DROP VIEW IF EXISTS timescaledb_information.continuous_aggregates;

DROP PROCEDURE IF EXISTS @extschema@.cagg_migrate (REGCLASS, BOOLEAN, BOOLEAN);
DROP FUNCTION IF EXISTS _timescaledb_internal.cagg_migrate_pre_validation (TEXT, TEXT, TEXT);
DROP FUNCTION IF EXISTS _timescaledb_functions.cagg_migrate_pre_validation (TEXT, TEXT, TEXT);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_create_plan (_timescaledb_catalog.continuous_agg, TEXT, BOOLEAN, BOOLEAN);
DROP PROCEDURE IF EXISTS _timescaledb_functions.cagg_migrate_create_plan (_timescaledb_catalog.continuous_agg, TEXT, BOOLEAN, BOOLEAN);

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
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_execute_copy_data (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_functions.cagg_migrate_execute_copy_data (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_execute_override_cagg (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_functions.cagg_migrate_execute_override_cagg (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_execute_drop_old_cagg (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_functions.cagg_migrate_execute_drop_old_cagg (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);

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
        -1::bigint as bucket_width, -- -1 means variable width. Will be modified soon if not variable.
        direct_view_schema,
        direct_view_name,
        materialized_only,
        finalized
    FROM
        _timescaledb_catalog.continuous_agg
    ORDER BY
        mat_hypertable_id;

-- Migrate CAggs with fixed bucket on interval back
WITH fixed_buckets AS (
    SELECT * FROM _timescaledb_catalog.continuous_aggs_bucket_function WHERE bucket_fixed_width = true AND bucket_func::text LIKE '%time_bucket(interval%'
)
UPDATE _timescaledb_catalog._tmp_continuous_agg cagg
    SET bucket_width = _timescaledb_functions.interval_to_usec(fb.bucket_width::interval)
    FROM fixed_buckets fb
    WHERE cagg.mat_hypertable_id = fb.mat_hypertable_id;

-- Migrate CAggs with fixed bucket on integer back
WITH fixed_buckets AS (
    SELECT * FROM _timescaledb_catalog.continuous_aggs_bucket_function WHERE bucket_fixed_width = true AND bucket_func::text NOT LIKE '%time_bucket(interval%'
)
UPDATE _timescaledb_catalog._tmp_continuous_agg cagg
    SET bucket_width = fb.bucket_width::bigint
    FROM fixed_buckets fb
    WHERE cagg.mat_hypertable_id = fb.mat_hypertable_id;

DELETE FROM _timescaledb_catalog.continuous_aggs_bucket_function WHERE bucket_fixed_width = true;

DROP TABLE _timescaledb_catalog.continuous_agg;

CREATE TABLE _timescaledb_catalog.continuous_agg (
    mat_hypertable_id integer NOT NULL,
    raw_hypertable_id integer NOT NULL,
    parent_mat_hypertable_id integer,
    user_view_schema name NOT NULL,
    user_view_name name NOT NULL,
    partial_view_schema name NOT NULL,
    partial_view_name name NOT NULL,
    bucket_width bigint NOT NULL,
    direct_view_schema name NOT NULL,
    direct_view_name name NOT NULL,
    materialized_only bool NOT NULL DEFAULT FALSE,
    finalized bool NOT NULL DEFAULT TRUE,
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

--
-- Rebuild the catalog table `_timescaledb_catalog.continuous_aggs_bucket_function`
--
UPDATE _timescaledb_catalog.continuous_aggs_bucket_function SET bucket_origin = '' WHERE bucket_origin IS NULL;
UPDATE _timescaledb_catalog.continuous_aggs_bucket_function SET bucket_timezone = '' WHERE bucket_timezone IS NULL;

CREATE TABLE _timescaledb_catalog._tmp_continuous_aggs_bucket_function AS
    SELECT
      mat_hypertable_id,
      CASE WHEN bucket_func::text like 'timescaledb_experimental%' THEN true ELSE false END,
      split_part(bucket_func::regproc::text, '.', 2),
      bucket_width,
      bucket_origin,
      bucket_timezone
    FROM
      _timescaledb_catalog.continuous_aggs_bucket_function
    ORDER BY
         mat_hypertable_id;

ALTER EXTENSION timescaledb
    DROP TABLE _timescaledb_catalog.continuous_aggs_bucket_function;

DROP TABLE _timescaledb_catalog.continuous_aggs_bucket_function;

CREATE TABLE _timescaledb_catalog.continuous_aggs_bucket_function (
  mat_hypertable_id integer NOT NULL,
  -- The schema of the function. Equals TRUE for "timescaledb_experimental", FALSE otherwise.
  experimental bool NOT NULL,
  -- Name of the bucketing function, e.g. "time_bucket" or "time_bucket_ng"
  name text NOT NULL,
  -- `bucket_width` argument of the function, e.g. "1 month"
  bucket_width text NOT NULL,
  -- `origin` argument of the function provided by the user
  origin text NOT NULL,
  -- `timezone` argument of the function provided by the user
  timezone text NOT NULL,
  -- table constraints
  CONSTRAINT continuous_aggs_bucket_function_pkey PRIMARY KEY (mat_hypertable_id),
  CONSTRAINT continuous_aggs_bucket_function_mat_hypertable_id_fkey FOREIGN KEY (mat_hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE
);

INSERT INTO _timescaledb_catalog.continuous_aggs_bucket_function
  SELECT * FROM _timescaledb_catalog._tmp_continuous_aggs_bucket_function;

DROP TABLE _timescaledb_catalog._tmp_continuous_aggs_bucket_function;

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_aggs_bucket_function', '');

GRANT SELECT ON TABLE _timescaledb_catalog.continuous_aggs_bucket_function TO PUBLIC;

ANALYZE _timescaledb_catalog.continuous_aggs_bucket_function;

--
-- End rebuild the catalog table `_timescaledb_catalog.continuous_aggs_bucket_function`
--

-- Convert _timescaledb_catalog.continuous_aggs_bucket_function.origin back to Timestamp
UPDATE _timescaledb_catalog.continuous_aggs_bucket_function
   SET origin = origin::timestamptz::timestamp::text
   WHERE length(origin) > 1;

-- only create stub
CREATE FUNCTION _timescaledb_functions.get_chunk_relstats(relid REGCLASS)
RETURNS TABLE(chunk_id INTEGER, hypertable_id INTEGER, num_pages INTEGER, num_tuples REAL, num_allvisible INTEGER)
AS $$BEGIN END$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE FUNCTION _timescaledb_functions.get_chunk_colstats(relid REGCLASS)
RETURNS TABLE(chunk_id INTEGER, hypertable_id INTEGER, att_num INTEGER, nullfrac REAL, width INTEGER, distinctval REAL, slotkind INTEGER[], slotopstrings CSTRING[], slotcollations OID[], slot1numbers FLOAT4[], slot2numbers FLOAT4[], slot3numbers FLOAT4[], slot4numbers FLOAT4[], slot5numbers FLOAT4[], slotvaluetypetrings CSTRING[], slot1values CSTRING[], slot2values CSTRING[], slot3values CSTRING[], slot4values CSTRING[], slot5values CSTRING[])
AS $$BEGIN END$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

--
-- START bgw_job_stat_history
--
DROP VIEW IF EXISTS timescaledb_information.job_errors;

ALTER EXTENSION timescaledb
  DROP VIEW timescaledb_information.job_history;

DROP VIEW IF EXISTS timescaledb_information.job_history;

CREATE TABLE _timescaledb_internal.job_errors (
  job_id integer not null,
  pid integer,
  start_time timestamptz,
  finish_time timestamptz,
  error_data jsonb
);

INSERT INTO _timescaledb_internal.job_errors (job_id, pid, start_time, finish_time, error_data)
SELECT
  job_id,
  pid,
  execution_start,
  execution_finish,
  data->'error_data'
FROM
  _timescaledb_internal.bgw_job_stat_history
WHERE
  succeeded IS FALSE
ORDER BY
  job_id, execution_start;

ALTER EXTENSION timescaledb
    DROP TABLE _timescaledb_internal.bgw_job_stat_history;

DROP TABLE IF EXISTS _timescaledb_internal.bgw_job_stat_history;

REVOKE ALL ON _timescaledb_internal.job_errors FROM PUBLIC;

DROP FUNCTION IF EXISTS _timescaledb_internal.policy_job_stat_history_retention(job_id integer,config jsonb);
DROP FUNCTION IF EXISTS _timescaledb_internal.policy_job_stat_history_retention_check(config jsonb);
DROP FUNCTION IF EXISTS _timescaledb_functions.policy_job_stat_history_retention(job_id integer,config jsonb);
DROP FUNCTION IF EXISTS _timescaledb_functions.policy_job_stat_history_retention_check(config jsonb);

CREATE OR REPLACE FUNCTION _timescaledb_functions.policy_job_error_retention(job_id integer, config JSONB) RETURNS integer
LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    drop_after INTERVAL;
    numrows INTEGER;
BEGIN
    SELECT  config->>'drop_after' INTO STRICT drop_after;
    WITH deleted AS
        (DELETE
        FROM _timescaledb_internal.job_errors
        WHERE finish_time < (now() - drop_after) RETURNING *)
        SELECT count(*)
        FROM deleted INTO numrows;
    RETURN numrows;
END;
$BODY$ SET search_path TO pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION _timescaledb_functions.policy_job_error_retention_check(config JSONB) RETURNS VOID
LANGUAGE PLPGSQL AS
$BODY$
DECLARE
  drop_after interval;
BEGIN
    IF config IS NULL THEN
        RAISE EXCEPTION 'config cannot be NULL, and must contain drop_after';
    END IF;
    SELECT config->>'drop_after' INTO STRICT drop_after;
    IF drop_after IS NULL THEN
        RAISE EXCEPTION 'drop_after interval not provided';
    END IF ;
END;
$BODY$ SET search_path TO pg_catalog, pg_temp;

UPDATE _timescaledb_config.bgw_job SET scheduled = true WHERE id = 2;
DELETE FROM _timescaledb_config.bgw_job WHERE id = 3;

DROP PROCEDURE IF EXISTS _timescaledb_functions.cagg_migrate_to_time_bucket(cagg REGCLASS);
