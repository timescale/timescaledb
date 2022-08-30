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

DROP FUNCTION IF EXISTS @extschema@.add_job(REGPROC, INTERVAL, JSONB, TIMESTAMPTZ, BOOL);
DROP FUNCTION IF EXISTS @extschema@.add_continuous_aggregate_policy(REGCLASS, "any", "any", INTERVAL, BOOL);
-- unnecessary, but for clarity
DROP FUNCTION IF EXISTS @extschema@.add_compression_policy(REGCLASS, "any", BOOL, INTERVAL);
DROP FUNCTION IF EXISTS @extschema@.add_retention_policy(REGCLASS, "any", BOOL, INTERVAL);

DROP VIEW IF EXISTS timescaledb_information.jobs;
DROP VIEW IF EXISTS timescaledb_information.job_stats;

-- rebuild _timescaledb_config.bgw_job

-- rebuild bgw_job table
CREATE TABLE _timescaledb_config.bgw_job_tmp AS SELECT * FROM _timescaledb_config.bgw_job;
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_config.bgw_job;
ALTER EXTENSION timescaledb DROP SEQUENCE _timescaledb_config.bgw_job_id_seq;
ALTER TABLE _timescaledb_internal.bgw_job_stat DROP CONSTRAINT IF EXISTS bgw_job_stat_job_id_fkey;
ALTER TABLE _timescaledb_internal.bgw_policy_chunk_stats DROP CONSTRAINT IF EXISTS bgw_policy_chunk_stats_job_id_fkey;

CREATE TABLE _timescaledb_internal.tmp_bgw_job_seq_value AS SELECT last_value, is_called FROM _timescaledb_config.bgw_job_id_seq;

DROP TABLE _timescaledb_config.bgw_job;

CREATE SEQUENCE _timescaledb_config.bgw_job_id_seq MINVALUE 1000;
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_config.bgw_job_id_seq', '');
SELECT setval('_timescaledb_config.bgw_job_id_seq', last_value, is_called) FROM _timescaledb_internal.tmp_bgw_job_seq_value;
DROP TABLE _timescaledb_internal.tmp_bgw_job_seq_value;

-- new table as we want it
CREATE TABLE _timescaledb_config.bgw_job (
  id INTEGER PRIMARY KEY DEFAULT nextval('_timescaledb_config.bgw_job_id_seq'),
  application_name name NOT NULL,
  schedule_interval interval NOT NULL,
  max_runtime interval NOT NULL,
  max_retries integer NOT NULL,
  retry_period interval NOT NULL,
  proc_schema name NOT NULL,
  proc_name name NOT NULL,
  owner name NOT NULL DEFAULT CURRENT_ROLE,
  scheduled bool NOT NULL DEFAULT TRUE,
  fixed_schedule bool not null default true,
  initial_start timestamptz,
  hypertable_id  integer REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE,
  config jsonb ,
  check_schema NAME,
  check_name NAME 
);

ALTER SEQUENCE _timescaledb_config.bgw_job_id_seq OWNED BY _timescaledb_config.bgw_job.id;
CREATE INDEX bgw_job_proc_hypertable_id_idx ON _timescaledb_config.bgw_job(proc_schema,proc_name,hypertable_id);

INSERT INTO _timescaledb_config.bgw_job(id, application_name, schedule_interval, max_runtime, max_retries, retry_period, proc_schema, proc_name, owner, scheduled, hypertable_id, config)
SELECT id, application_name, schedule_interval, max_runtime, max_retries, retry_period, proc_schema, proc_name, owner, scheduled, hypertable_id, config FROM _timescaledb_config.bgw_job_tmp ORDER BY id;
UPDATE _timescaledb_config.bgw_job SET fixed_schedule = FALSE;
DROP TABLE _timescaledb_config.bgw_job_tmp;
ALTER TABLE _timescaledb_internal.bgw_job_stat ADD CONSTRAINT bgw_job_stat_job_id_fkey FOREIGN KEY(job_id) REFERENCES _timescaledb_config.bgw_job(id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_internal.bgw_policy_chunk_stats ADD CONSTRAINT bgw_policy_chunk_stats_job_id_fkey FOREIGN KEY(job_id) REFERENCES _timescaledb_config.bgw_job(id) ON DELETE CASCADE;

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_config.bgw_job', 'WHERE id >= 1000');
GRANT SELECT ON _timescaledb_config.bgw_job TO PUBLIC;
GRANT SELECT ON _timescaledb_config.bgw_job_id_seq TO PUBLIC;












-- CREATE TABLE  _timescaledb_config.bgw_job_tmp (
--   id integer NOT NULL,
--   application_name name NOT NULL,
--   schedule_interval interval NOT NULL,
--   max_runtime interval NOT NULL,
--   max_retries integer NOT NULL,
--   retry_period interval NOT NULL,
--   proc_schema name NOT NULL,
--   proc_name name NOT NULL,
--   owner name NOT NULL DEFAULT CURRENT_ROLE,
--   scheduled bool NOT NULL DEFAULT TRUE,
--   fixed_schedule bool not null default true,
--   initial_start timestamptz,
--   hypertable_id integer,
--   config jsonb,
--   check_schema NAME,
--   check_name NAME
-- );
-- -- CREATE TABLE _timescaledb_config.bgw_job_tmp as 
-- -- SELECT bgj.id, bgj.application_name, bgj.schedule_interval, bgj.max_runtime, bgj.max_retries, bgj.retry_period, 
-- -- bgj.proc_schema, bgj.proc_name, bgj.owner, bgj.scheduled, nc.fixed_schedule, nc.initial_start, bgj.hypertable_id, bgj.config
-- -- FROM _timescaledb_config.bgw_job bgj, new_cols nc;

-- CREATE TABLE _timescaledb_internal.tmp_bgw_job_seq_value AS
-- SELECT last_value, is_called FROM _timescaledb_config.bgw_job_id_seq;

-- -- the fields from bgw_job
-- UPDATE _timescaledb_config.bgw_job_tmp SET id = old_bgw_job.id, 
-- application_name = old_bgw_job.application_name, schedule_interval = old_bgw_job.schedule_interval,
-- max_runtime = old_bgw_job.max_runtime, max_retries = old_bgw_job.max_retries,
-- retry_period = old_bgw_job.retry_period, proc_schema = old_bgw_job.proc_schema, 
-- proc_name = old_bgw_job.proc_name, owner = old_bgw_job.owner, 
-- scheduled = old_bgw_job.scheduled, hypertable_id = old_bgw_job.hypertable_id, 
-- config = old_bgw_job.config FROM _timescaledb_config.bgw_job old_bgw_job;
-- -- all previously existing jobs will remain on a drifting schedule, and no initial_start provided
-- UPDATE _timescaledb_config.bgw_job_tmp SET fixed_schedule = FALSE;
-- -- required in order to rebuild the table bgw_job
-- ALTER EXTENSION timescaledb DROP TABLE _timescaledb_config.bgw_job;
-- ALTER EXTENSION timescaledb DROP SEQUENCE _timescaledb_config.bgw_job_id_seq;
-- ALTER TABLE _timescaledb_internal.bgw_job_stat DROP CONSTRAINT IF EXISTS bgw_job_stat_job_id_fkey;
-- ALTER TABLE _timescaledb_internal.bgw_policy_chunk_stats DROP CONSTRAINT IF EXISTS bgw_policy_chunk_stats_job_id_fkey;

-- DROP TABLE _timescaledb_config.bgw_job;
-- DROP SEQUENCE _timescaledb_config.bgw_job_id_seq;

-- -- recreate the seq
-- CREATE SEQUENCE _timescaledb_config.bgw_job_id_seq MINVALUE 1000;
-- SELECT pg_catalog.pg_extension_config_dump('_timescaledb_config.bgw_job_id_seq', '');
-- SELECT pg_catalog.setval('_timescaledb_config.bgw_job_id_seq', last_value, is_called)
-- FROM _timescaledb_internal.tmp_bgw_job_seq_value;
-- DROP TABLE _timescaledb_internal.tmp_bgw_job_seq_value;

-- -- recreate the table with the appropriate fields and constraints
-- CREATE TABLE _timescaledb_config.bgw_job (
--   id integer NOT NULL DEFAULT nextval('_timescaledb_config.bgw_job_id_seq'),
--   application_name name NOT NULL,
--   schedule_interval interval NOT NULL,
--   max_runtime interval NOT NULL,
--   max_retries integer NOT NULL,
--   retry_period interval NOT NULL,
--   proc_schema name NOT NULL,
--   proc_name name NOT NULL,
--   owner name NOT NULL DEFAULT CURRENT_ROLE,
--   scheduled bool NOT NULL DEFAULT TRUE,
--   fixed_schedule bool not null default true,
--   initial_start timestamptz,
--   hypertable_id  integer REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE,
--   config jsonb ,
--   check_schema NAME,
--   check_name NAME 
-- );

-- ALTER SEQUENCE _timescaledb_config.bgw_job_id_seq OWNED BY _timescaledb_config.bgw_job.id;
-- CREATE INDEX bgw_job_proc_hypertable_id_idx
--        ON _timescaledb_config.bgw_job (proc_schema, proc_name, hypertable_id);

-- INSERT INTO _timescaledb_config.bgw_job SELECT * FROM _timescaledb_config.bgw_job_tmp ORDER BY id;
-- DROP TABLE _timescaledb_config.bgw_job_tmp;
-- -- add the constraints
-- ALTER TABLE _timescaledb_config.bgw_job
--   ADD CONSTRAINT bgw_job_pkey PRIMARY KEY (id),
--   ADD CONSTRAINT bgw_job_hypertable_id_fkey FOREIGN KEY (hypertable_id) 
--   REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE;

-- SELECT pg_catalog.pg_extension_config_dump('_timescaledb_config.bgw_job', 'WHERE id >= 1000');
-- GRANT SELECT ON _timescaledb_config.bgw_job TO PUBLIC;
-- GRANT SELECT ON _timescaledb_config.bgw_job_id_seq TO PUBLIC;
