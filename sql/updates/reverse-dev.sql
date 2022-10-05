-- gapfill with timezone support
DROP FUNCTION @extschema@.time_bucket_gapfill(INTERVAL,TIMESTAMPTZ,TEXT,TIMESTAMPTZ,TIMESTAMPTZ);

ALTER TABLE _timescaledb_catalog.compression_chunk_size DROP CONSTRAINT compression_chunk_size_pkey;
ALTER TABLE _timescaledb_catalog.compression_chunk_size ADD CONSTRAINT compression_chunk_size_pkey PRIMARY KEY(chunk_id,compressed_chunk_id);

DROP FUNCTION _timescaledb_internal.policy_job_error_retention(integer, JSONB);
DROP FUNCTION _timescaledb_internal.policy_job_error_retention_check(JSONB);
DELETE FROM _timescaledb_config.bgw_job WHERE id = 2;

ALTER EXTENSION timescaledb DROP VIEW timescaledb_information.job_errors;
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_internal.job_errors;

DROP VIEW timescaledb_information.job_errors;
DROP TABLE _timescaledb_internal.job_errors;

-- drop dependent views
DROP VIEW IF EXISTS timescaledb_information.job_stats;
DROP VIEW IF EXISTS timescaledb_information.jobs;

CREATE TABLE _timescaledb_internal._tmp_bgw_job_stat AS SELECT * FROM _timescaledb_internal.bgw_job_stat;
DROP TABLE _timescaledb_internal.bgw_job_stat;

CREATE TABLE _timescaledb_internal.bgw_job_stat (
  job_id integer NOT NULL,
  last_start timestamptz NOT NULL DEFAULT NOW(),
  last_finish timestamptz NOT NULL,
  next_start timestamptz NOT NULL,
  last_successful_finish timestamptz NOT NULL,
  last_run_success bool NOT NULL,
  total_runs bigint NOT NULL,
  total_duration interval NOT NULL,
  total_successes bigint NOT NULL,
  total_failures bigint NOT NULL,
  total_crashes bigint NOT NULL,
  consecutive_failures int NOT NULL,
  consecutive_crashes int NOT NULL,
  -- table constraints
  CONSTRAINT bgw_job_stat_pkey PRIMARY KEY (job_id),
  CONSTRAINT bgw_job_stat_job_id_fkey FOREIGN KEY (job_id) REFERENCES _timescaledb_config.bgw_job (id) ON DELETE CASCADE
);

INSERT INTO _timescaledb_internal.bgw_job_stat SELECT
  job_id, last_start, last_finish, next_start, last_successful_finish, last_run_success, total_runs, total_duration, total_successes, total_failures, total_crashes, consecutive_failures, consecutive_crashes
FROM _timescaledb_internal._tmp_bgw_job_stat;
DROP TABLE _timescaledb_internal._tmp_bgw_job_stat;

GRANT SELECT ON TABLE _timescaledb_internal.bgw_job_stat TO PUBLIC;

DROP VIEW _timescaledb_internal.hypertable_chunk_local_size;
DROP FUNCTION _timescaledb_internal.hypertable_local_size(name, name);

CREATE FUNCTION _timescaledb_internal.hypertable_local_size(
	schema_name_in name,
	table_name_in name)
RETURNS TABLE (
	table_bytes BIGINT,
	index_bytes BIGINT,
	toast_bytes BIGINT,
	total_bytes BIGINT)
LANGUAGE SQL VOLATILE STRICT AS
$BODY$
    /* get the main hypertable id and sizes */
    WITH _hypertable AS (
        SELECT
            id,
            _timescaledb_internal.relation_size(format('%I.%I', schema_name, table_name)::regclass) AS relsize
        FROM
            _timescaledb_catalog.hypertable
        WHERE
            schema_name = schema_name_in
            AND table_name = table_name_in
    ),
    /* project the size of the parent hypertable */
    _hypertable_sizes AS (
        SELECT
            id,
            COALESCE((relsize).total_size, 0) AS total_bytes,
            COALESCE((relsize).heap_size, 0) AS heap_bytes,
            COALESCE((relsize).index_size, 0) AS index_bytes,
            COALESCE((relsize).toast_size, 0) AS toast_bytes,
            0::BIGINT AS compressed_total_size,
            0::BIGINT AS compressed_index_size,
            0::BIGINT AS compressed_toast_size,
            0::BIGINT AS compressed_heap_size
        FROM
            _hypertable
    ),
    /* calculate the size of the hypertable chunks */
    _chunk_sizes AS (
        SELECT
            chunk_id,
            COALESCE(ch.total_bytes, 0) AS total_bytes,
            COALESCE(ch.heap_bytes, 0) AS heap_bytes,
            COALESCE(ch.index_bytes, 0) AS index_bytes,
            COALESCE(ch.toast_bytes, 0) AS toast_bytes,
            COALESCE(ch.compressed_total_size, 0) AS compressed_total_size,
            COALESCE(ch.compressed_index_size, 0) AS compressed_index_size,
            COALESCE(ch.compressed_toast_size, 0) AS compressed_toast_size,
            COALESCE(ch.compressed_heap_size, 0) AS compressed_heap_size
        FROM
            _timescaledb_internal.hypertable_chunk_local_size ch
            JOIN _hypertable_sizes ht ON ht.id = ch.hypertable_id
    )
    /* calculate the SUM of the hypertable and chunk sizes */
	SELECT
		(SUM(heap_bytes)  + SUM(compressed_heap_size))::BIGINT AS heap_bytes,
		(SUM(index_bytes) + SUM(compressed_index_size))::BIGINT AS index_bytes,
		(SUM(toast_bytes) + SUM(compressed_toast_size))::BIGINT AS toast_bytes,
		(SUM(total_bytes) + SUM(compressed_total_size))::BIGINT AS total_bytes
	FROM
		(SELECT * FROM _hypertable_sizes
         UNION ALL
         SELECT * FROM _chunk_sizes) AS sizes;
$BODY$ SET search_path TO pg_catalog, pg_temp;


DROP VIEW IF EXISTS timescaledb_information.job_stats;
DROP VIEW IF EXISTS timescaledb_information.jobs;
DROP VIEW IF EXISTS timescaledb_experimental.policies;
-- fixed schedule
DROP FUNCTION IF EXISTS  @extschema@.add_retention_policy(REGCLASS, "any", BOOL, INTERVAL, TIMESTAMPTZ, BOOL);

DROP FUNCTION IF EXISTS  @extschema@.add_compression_policy(REGCLASS, "any", BOOL, INTERVAL);

-- fixed schedule changes
-- drop and recreate functions with modified signatures, modified views, modified tables
DROP FUNCTION IF EXISTS @extschema@.add_job(REGPROC, INTERVAL, JSONB, TIMESTAMPTZ, BOOL, REGPROC, BOOL, TEXT);
DROP FUNCTION IF EXISTS @extschema@.add_continuous_aggregate_policy(REGCLASS, "any", "any", INTERVAL, BOOL, TIMESTAMPTZ, TEXT);
DROP FUNCTION IF EXISTS @extschema@.add_compression_policy(REGCLASS, "any", BOOL, INTERVAL, TIMESTAMPTZ, TEXT);
DROP FUNCTION IF EXISTS @extschema@.add_retention_policy(REGCLASS, "any", BOOL, INTERVAL, TIMESTAMPTZ, TEXT);
DROP FUNCTION IF EXISTS @extschema@.add_reorder_policy(REGCLASS, NAME, BOOL, TIMESTAMPTZ, TEXT);
-- recreate functions with the previous signature
CREATE FUNCTION @extschema@.add_job(
  proc REGPROC,
  schedule_interval INTERVAL,
  config JSONB DEFAULT NULL,
  initial_start TIMESTAMPTZ DEFAULT NULL,
  scheduled BOOL DEFAULT true,
  check_config REGPROC DEFAULT NULL
) RETURNS INTEGER AS '@MODULE_PATHNAME@', 'ts_job_add' LANGUAGE C VOLATILE;

CREATE FUNCTION @extschema@.add_compression_policy(hypertable REGCLASS, compress_after "any", if_not_exists BOOL = false, schedule_interval INTERVAL = NULL)
RETURNS INTEGER AS '@MODULE_PATHNAME@', 'ts_policy_compression_add' LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION @extschema@.add_retention_policy(
       relation REGCLASS,
       drop_after "any",
       if_not_exists BOOL = false,
       schedule_interval INTERVAL = NULL
)
RETURNS INTEGER AS '@MODULE_PATHNAME@', 'ts_policy_retention_add'
LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION @extschema@.add_continuous_aggregate_policy(continuous_aggregate REGCLASS, start_offset "any", end_offset "any", schedule_interval INTERVAL, if_not_exists BOOL = false)
RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_policy_refresh_cagg_add'
LANGUAGE C VOLATILE;

CREATE FUNCTION @extschema@.add_reorder_policy(
    hypertable REGCLASS,
    index_name NAME,
    if_not_exists BOOL = false
) RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_policy_reorder_add'
LANGUAGE C VOLATILE STRICT;

DROP VIEW IF EXISTS timescaledb_information.jobs;
DROP VIEW IF EXISTS timescaledb_information.job_stats;

-- now need to rebuild the table
ALTER TABLE _timescaledb_internal.bgw_job_stat
      DROP CONSTRAINT bgw_job_stat_job_id_fkey;
ALTER TABLE _timescaledb_internal.bgw_policy_chunk_stats
      DROP CONSTRAINT bgw_policy_chunk_stats_chunk_id_fkey,
      DROP CONSTRAINT bgw_policy_chunk_stats_job_id_fkey;

CREATE TABLE _timescaledb_config.bgw_job_tmp AS SELECT * FROM _timescaledb_config.bgw_job;
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_config.bgw_job;
ALTER EXTENSION timescaledb DROP SEQUENCE _timescaledb_config.bgw_job_id_seq;

CREATE TABLE _timescaledb_internal.tmp_bgw_job_seq_value AS SELECT last_value, is_called FROM _timescaledb_config.bgw_job_id_seq;
DROP TABLE _timescaledb_config.bgw_job;

CREATE SEQUENCE _timescaledb_config.bgw_job_id_seq MINVALUE 1000;
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_config.bgw_job_id_seq', '');
SELECT setval('_timescaledb_config.bgw_job_id_seq', last_value, is_called) FROM _timescaledb_internal.tmp_bgw_job_seq_value;
DROP TABLE _timescaledb_internal.tmp_bgw_job_seq_value;

CREATE TABLE _timescaledb_config.bgw_job (
  id integer PRIMARY KEY DEFAULT nextval('_timescaledb_config.bgw_job_id_seq'),
  application_name name NOT NULL,
  schedule_interval interval NOT NULL,
  max_runtime interval NOT NULL,
  max_retries integer NOT NULL,
  retry_period interval NOT NULL,
  proc_schema name NOT NULL,
  proc_name name NOT NULL,
  owner name NOT NULL DEFAULT CURRENT_ROLE,
  scheduled bool NOT NULL DEFAULT TRUE,
  hypertable_id integer REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE,
  config jsonb,
  check_schema NAME,
  check_name NAME
);

INSERT INTO _timescaledb_config.bgw_job(id, application_name, schedule_interval, max_runtime, max_retries, retry_period, proc_schema, proc_name, owner, scheduled, hypertable_id, config)
SELECT id, application_name, schedule_interval, max_runtime, max_retries, retry_period, proc_schema, proc_name, owner, scheduled, hypertable_id, config FROM _timescaledb_config.bgw_job_tmp ORDER BY id;

ALTER SEQUENCE _timescaledb_config.bgw_job_id_seq OWNED BY _timescaledb_config.bgw_job.id;
CREATE INDEX bgw_job_proc_hypertable_id_idx ON _timescaledb_config.bgw_job(proc_schema,proc_name,hypertable_id);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_config.bgw_job', 'WHERE id >= 1000');
GRANT SELECT ON _timescaledb_config.bgw_job TO PUBLIC;
GRANT SELECT ON _timescaledb_config.bgw_job_id_seq TO PUBLIC;

DROP TABLE _timescaledb_config.bgw_job_tmp;
ALTER TABLE _timescaledb_internal.bgw_job_stat ADD CONSTRAINT bgw_job_stat_job_id_fkey FOREIGN KEY(job_id) REFERENCES _timescaledb_config.bgw_job(id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_internal.bgw_policy_chunk_stats
      ADD CONSTRAINT bgw_policy_chunk_stats_chunk_id_fkey
          FOREIGN KEY (chunk_id) REFERENCES _timescaledb_catalog.chunk(id)
          ON DELETE CASCADE,
      ADD CONSTRAINT bgw_policy_chunk_stats_job_id_fkey
          FOREIGN KEY(job_id) REFERENCES _timescaledb_config.bgw_job(id)
          ON DELETE CASCADE;

DROP FUNCTION _timescaledb_internal.health;

-- Recreate _timescaledb_catalog.dimension table without the compress_interval_length column --
CREATE TABLE _timescaledb_internal.dimension_tmp
AS SELECT * from _timescaledb_catalog.dimension;

CREATE TABLE _timescaledb_internal.tmp_dimension_seq_value AS
SELECT last_value, is_called FROM _timescaledb_catalog.dimension_id_seq;

--drop foreign keys on dimension table
ALTER TABLE _timescaledb_catalog.dimension_partition DROP CONSTRAINT
dimension_partition_dimension_id_fkey;
ALTER TABLE _timescaledb_catalog.dimension_slice DROP CONSTRAINT
dimension_slice_dimension_id_fkey;

--drop dependent views
DROP VIEW IF EXISTS timescaledb_information.chunks;
DROP VIEW IF EXISTS timescaledb_information.dimensions;

ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.dimension;
ALTER EXTENSION timescaledb DROP SEQUENCE _timescaledb_catalog.dimension_id_seq;
DROP TABLE _timescaledb_catalog.dimension;

CREATE TABLE _timescaledb_catalog.dimension (
  id serial NOT NULL ,
  hypertable_id integer NOT NULL,
  column_name name NOT NULL,
  column_type REGTYPE NOT NULL,
  aligned boolean NOT NULL,
  -- closed dimensions
  num_slices smallint NULL,
  partitioning_func_schema name NULL,
  partitioning_func name NULL,
  -- open dimensions (e.g., time)
  interval_length bigint NULL,
  integer_now_func_schema name NULL,
  integer_now_func name NULL,
  -- table constraints
  CONSTRAINT dimension_pkey PRIMARY KEY (id),
  CONSTRAINT dimension_hypertable_id_column_name_key UNIQUE (hypertable_id, column_name),
  CONSTRAINT dimension_check CHECK ((partitioning_func_schema IS NULL AND partitioning_func IS NULL) OR (partitioning_func_schema IS NOT NULL AND partitioning_func IS NOT NULL)),
  CONSTRAINT dimension_check1 CHECK ((num_slices IS NULL AND interval_length IS NOT NULL) OR (num_slices IS NOT NULL AND interval_length IS NULL)),
  CONSTRAINT dimension_check2 CHECK ((integer_now_func_schema IS NULL AND integer_now_func IS NULL) OR (integer_now_func_schema IS NOT NULL AND integer_now_func IS NOT NULL)),
  CONSTRAINT dimension_interval_length_check CHECK (interval_length IS NULL OR interval_length > 0),
  CONSTRAINT dimension_hypertable_id_fkey FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE
);

INSERT INTO _timescaledb_catalog.dimension
( id, hypertable_id, column_name, column_type,
  aligned, num_slices, partitioning_func_schema,
  partitioning_func, interval_length,
  integer_now_func_schema, integer_now_func)
SELECT id, hypertable_id, column_name, column_type,
  aligned, num_slices, partitioning_func_schema,
  partitioning_func, interval_length,
  integer_now_func_schema, integer_now_func
FROM _timescaledb_internal.dimension_tmp;

ALTER SEQUENCE _timescaledb_catalog.dimension_id_seq OWNED BY _timescaledb_catalog.dimension.id;
SELECT setval('_timescaledb_catalog.dimension_id_seq', last_value, is_called) FROM _timescaledb_internal.tmp_dimension_seq_value;

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.dimension', '');
SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.dimension', 'id'), '');

--add the foreign key constraints
ALTER TABLE _timescaledb_catalog.dimension_partition ADD CONSTRAINT
dimension_partition_dimension_id_fkey FOREIGN KEY (dimension_id)
REFERENCES _timescaledb_catalog.dimension(id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.dimension_slice ADD CONSTRAINT
dimension_slice_dimension_id_fkey FOREIGN KEY (dimension_id)
REFERENCES _timescaledb_catalog.dimension(id) ON DELETE CASCADE;

--cleanup
DROP TABLE _timescaledb_internal.dimension_tmp;
DROP TABLE _timescaledb_internal.tmp_dimension_seq_value;

GRANT SELECT ON _timescaledb_catalog.dimension_id_seq TO PUBLIC;
GRANT SELECT ON _timescaledb_catalog.dimension TO PUBLIC;

-- end recreate _timescaledb_catalog.dimension table --

-- changes related to alter_data_node()
DROP INDEX _timescaledb_catalog.chunk_data_node_node_name_idx;
DROP FUNCTION @extschema@.alter_data_node;

DROP FUNCTION _timescaledb_internal.frozen_chunk_modify_blocker;

--
-- Prevent downgrading if there are hierarchical continuous aggregates
--
DO
$$
DECLARE
    caggs_hierarchical TEXT;
    caggs_count INTEGER;
BEGIN
    SELECT
        string_agg(format('%I.%I', user_view_schema, user_view_name), ', '),
        count(*)
    INTO
        caggs_hierarchical,
        caggs_count
    FROM
        _timescaledb_catalog.continuous_agg
    WHERE
        parent_mat_hypertable_id IS NOT NULL;

    IF caggs_count > 0 THEN
        RAISE EXCEPTION 'Downgrade is not possible because there are % hierarchical continuous aggregates: %', caggs_count, caggs_nested
            USING HINT = 'Remove the corresponding continuous aggregates manually before downgrading';
    END IF;
END;
$$
LANGUAGE 'plpgsql';

--
-- Rebuild the catalog table `_timescaledb_catalog.continuous_agg`
--
DROP VIEW IF EXISTS timescaledb_information.hypertables;
DROP VIEW IF EXISTS timescaledb_information.continuous_aggregates;
DROP PROCEDURE IF EXISTS @extschema@.cagg_migrate (REGCLASS, BOOLEAN, BOOLEAN);
DROP FUNCTION IF EXISTS _timescaledb_internal.cagg_migrate_pre_validation (TEXT, TEXT, TEXT);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_create_plan (_timescaledb_catalog.continuous_agg, TEXT, BOOLEAN, BOOLEAN);
DROP FUNCTION IF EXISTS _timescaledb_internal.cagg_migrate_plan_exists (INTEGER);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_execute_plan (_timescaledb_catalog.continuous_agg);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_execute_create_new_cagg (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_execute_disable_policies (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_execute_enable_policies (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_execute_copy_policies (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_execute_refresh_new_cagg (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_execute_copy_data (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_execute_override_cagg (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_execute_drop_old_cagg (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);

ALTER EXTENSION timescaledb
    DROP TABLE _timescaledb_catalog.continuous_agg;

ALTER TABLE _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
    DROP CONSTRAINT continuous_aggs_materialization_invalid_materialization_id_fkey;

ALTER TABLE _timescaledb_catalog.continuous_agg_migrate_plan
    DROP CONSTRAINT continuous_agg_migrate_plan_mat_hypertable_id_fkey;

CREATE TABLE _timescaledb_catalog._tmp_continuous_agg AS
    SELECT
        mat_hypertable_id,
        raw_hypertable_id,
        user_view_schema,
        user_view_name,
        partial_view_schema,
        partial_view_name,
        bucket_width,
        direct_view_schema,
        direct_view_name,
        materialized_only,
        finalized
    FROM
        _timescaledb_catalog.continuous_agg
    ORDER BY
        mat_hypertable_id;

DROP TABLE _timescaledb_catalog.continuous_agg;

CREATE TABLE _timescaledb_catalog.continuous_agg (
    mat_hypertable_id integer NOT NULL,
    raw_hypertable_id integer NOT NULL,
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
        FOREIGN KEY (raw_hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE
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

ALTER TABLE _timescaledb_catalog.continuous_agg_migrate_plan
    ADD CONSTRAINT continuous_agg_migrate_plan_mat_hypertable_id_fkey
        FOREIGN KEY (mat_hypertable_id)
        REFERENCES _timescaledb_catalog.continuous_agg (mat_hypertable_id);

ANALYZE _timescaledb_catalog.continuous_agg;
