DROP FUNCTION IF EXISTS @extschema@.add_retention_policy(REGCLASS, "any", BOOL);
DROP FUNCTION IF EXISTS @extschema@.add_compression_policy(REGCLASS, "any", BOOL);
DROP FUNCTION IF EXISTS @extschema@.detach_data_node;
CREATE TABLE _timescaledb_catalog.dimension_partition (
  dimension_id integer NOT NULL REFERENCES _timescaledb_catalog.dimension (id) ON DELETE CASCADE,
  range_start bigint NOT NULL,
  data_nodes name[] NULL,
  UNIQUE (dimension_id, range_start)
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.dimension_partition', '');
GRANT SELECT ON _timescaledb_catalog.dimension_partition TO PUBLIC;
DROP FUNCTION IF EXISTS @extschema@.remove_continuous_aggregate_policy(REGCLASS, BOOL);

-- add a new column to chunk catalog table
ALTER TABLE _timescaledb_catalog.chunk ADD COLUMN  osm_chunk boolean ;
UPDATE _timescaledb_catalog.chunk SET osm_chunk = FALSE;

ALTER TABLE _timescaledb_catalog.chunk
  ALTER COLUMN  osm_chunk SET NOT NULL;
ALTER TABLE _timescaledb_catalog.chunk
  ALTER COLUMN  osm_chunk SET DEFAULT FALSE;

CREATE INDEX chunk_osm_chunk_idx ON _timescaledb_catalog.chunk (osm_chunk, hypertable_id);

DROP FUNCTION IF EXISTS @extschema@.add_job(REGPROC, INTERVAL, JSONB, TIMESTAMPTZ, BOOL);
DROP FUNCTION IF EXISTS @extschema@.alter_job(INTEGER, INTERVAL, INTERVAL, INTEGER, INTERVAL, BOOL, JSONB, TIMESTAMPTZ, BOOL);

-- add fields for check function
ALTER TABLE _timescaledb_config.bgw_job
      ADD COLUMN check_schema NAME,
      ADD COLUMN check_name NAME;

-- no need to touch the telemetry jobs since they do not have a check
-- function.

-- add check function to reorder jobs
UPDATE
  _timescaledb_config.bgw_job job
SET
  check_schema = '_timescaledb_internal',
  check_name = 'policy_reorder_check'
WHERE proc_schema = '_timescaledb_internal'
  AND proc_name = 'policy_reorder';

-- add check function to compression jobs
UPDATE
  _timescaledb_config.bgw_job job
SET
  check_schema = '_timescaledb_internal',
  check_name = 'policy_compression_check'
WHERE proc_schema = '_timescaledb_internal'
  AND proc_name = 'policy_compression';

-- add check function to retention jobs
UPDATE
  _timescaledb_config.bgw_job job
SET
  check_schema = '_timescaledb_internal',
  check_name = 'policy_retention_check'
WHERE proc_schema = '_timescaledb_internal'
  AND proc_name = 'policy_retention';

-- add check function to continuous aggregate refresh jobs
UPDATE
  _timescaledb_config.bgw_job job
SET
  check_schema = '_timescaledb_internal',
  check_name = 'policy_refresh_continuous_aggregate_check'
WHERE proc_schema = '_timescaledb_internal'
  AND proc_name = 'policy_refresh_continuous_aggregate';

DROP VIEW IF EXISTS timescaledb_information.jobs;-- cagg migration catalog relations

CREATE TABLE _timescaledb_catalog.continuous_agg_migrate_plan (
  mat_hypertable_id integer NOT NULL,
  start_ts TIMESTAMPTZ NOT NULL DEFAULT pg_catalog.now(),
  end_ts TIMESTAMPTZ,
  -- table constraints
  CONSTRAINT continuous_agg_migrate_plan_pkey PRIMARY KEY (mat_hypertable_id),
  CONSTRAINT continuous_agg_migrate_plan_mat_hypertable_id_fkey FOREIGN KEY (mat_hypertable_id) REFERENCES _timescaledb_catalog.continuous_agg (mat_hypertable_id)
);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_agg_migrate_plan', '');

CREATE TABLE _timescaledb_catalog.continuous_agg_migrate_plan_step (
  mat_hypertable_id integer NOT NULL,
  step_id serial NOT NULL,
  status TEXT NOT NULL DEFAULT 'NOT STARTED', -- NOT STARTED, STARTED, FINISHED, CANCELED
  start_ts TIMESTAMPTZ,
  end_ts TIMESTAMPTZ,
  type TEXT NOT NULL,
  config JSONB,
  -- table constraints
  CONSTRAINT continuous_agg_migrate_plan_step_pkey PRIMARY KEY (mat_hypertable_id, step_id),
  CONSTRAINT continuous_agg_migrate_plan_step_mat_hypertable_id_fkey FOREIGN KEY (mat_hypertable_id) REFERENCES _timescaledb_catalog.continuous_agg_migrate_plan (mat_hypertable_id) ON DELETE CASCADE,
  CONSTRAINT continuous_agg_migrate_plan_step_check CHECK (start_ts <= end_ts),
  CONSTRAINT continuous_agg_migrate_plan_step_check2 CHECK (type IN ('CREATE NEW CAGG', 'DISABLE POLICIES', 'COPY POLICIES', 'ENABLE POLICIES', 'SAVE WATERMARK', 'REFRESH NEW CAGG', 'COPY DATA'))
);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_agg_migrate_plan_step', '');

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_agg_migrate_plan_step_step_id_seq', '');

-- in tables.sql the same is done with GRANT SELECT ON ALL TABLES IN SCHEMA
GRANT SELECT ON _timescaledb_catalog.continuous_agg_migrate_plan TO PUBLIC;
GRANT SELECT ON _timescaledb_catalog.continuous_agg_migrate_plan_step TO PUBLIC;
GRANT SELECT ON _timescaledb_catalog.continuous_agg_migrate_plan_step_step_id_seq TO PUBLIC;

-- add distributed argument
DROP FUNCTION IF EXISTS @extschema@.create_hypertable;
DROP FUNCTION IF EXISTS @extschema@.create_distributed_hypertable;

CREATE FUNCTION @extschema@.create_hypertable(
    relation                REGCLASS,
    time_column_name        NAME,
    partitioning_column     NAME = NULL,
    number_partitions       INTEGER = NULL,
    associated_schema_name  NAME = NULL,
    associated_table_prefix NAME = NULL,
    chunk_time_interval     ANYELEMENT = NULL::bigint,
    create_default_indexes  BOOLEAN = TRUE,
    if_not_exists           BOOLEAN = FALSE,
    partitioning_func       REGPROC = NULL,
    migrate_data            BOOLEAN = FALSE,
    chunk_target_size       TEXT = NULL,
    chunk_sizing_func       REGPROC = '_timescaledb_internal.calculate_chunk_interval'::regproc,
    time_partitioning_func  REGPROC = NULL,
    replication_factor      INTEGER = NULL,
    data_nodes              NAME[] = NULL,
    distributed             BOOLEAN = NULL
) RETURNS TABLE(hypertable_id INT, schema_name NAME, table_name NAME, created BOOL) AS '@MODULE_PATHNAME@', 'ts_hypertable_create' LANGUAGE C VOLATILE;

-- change replication_factor to NULL by default
CREATE FUNCTION @extschema@.create_distributed_hypertable(
    relation                REGCLASS,
    time_column_name        NAME,
    partitioning_column     NAME = NULL,
    number_partitions       INTEGER = NULL,
    associated_schema_name  NAME = NULL,
    associated_table_prefix NAME = NULL,
    chunk_time_interval     ANYELEMENT = NULL::bigint,
    create_default_indexes  BOOLEAN = TRUE,
    if_not_exists           BOOLEAN = FALSE,
    partitioning_func       REGPROC = NULL,
    migrate_data            BOOLEAN = FALSE,
    chunk_target_size       TEXT = NULL,
    chunk_sizing_func       REGPROC = '_timescaledb_internal.calculate_chunk_interval'::regproc,
    time_partitioning_func  REGPROC = NULL,
    replication_factor      INTEGER = NULL,
    data_nodes              NAME[] = NULL
) RETURNS TABLE(hypertable_id INT, schema_name NAME, table_name NAME, created BOOL) AS '@MODULE_PATHNAME@', 'ts_hypertable_distributed_create' LANGUAGE C VOLATILE;
