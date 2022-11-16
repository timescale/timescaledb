DROP VIEW IF EXISTS timescaledb_information.job_stats;
DROP VIEW IF EXISTS timescaledb_information.jobs;
DROP FUNCTION IF EXISTS  @extschema@.add_retention_policy(REGCLASS, "any", BOOL, INTERVAL);
CREATE FUNCTION @extschema@.add_retention_policy(relation REGCLASS, drop_after "any", if_not_exists BOOL = false)
RETURNS INTEGER AS '@MODULE_PATHNAME@', 'ts_policy_retention_add' LANGUAGE C VOLATILE STRICT;

DROP FUNCTION IF EXISTS  @extschema@.add_compression_policy(REGCLASS, "any", BOOL, INTERVAL);
CREATE FUNCTION @extschema@.add_compression_policy(hypertable REGCLASS, compress_after "any", if_not_exists BOOL = false)
RETURNS INTEGER AS '@MODULE_PATHNAME@', 'ts_policy_compression_add' LANGUAGE C VOLATILE STRICT;

DROP FUNCTION IF EXISTS @extschema@.detach_data_node;
CREATE FUNCTION @extschema@.detach_data_node(
    node_name              NAME,
    hypertable             REGCLASS = NULL,
    if_attached            BOOLEAN = FALSE,
    force                  BOOLEAN = FALSE,
    repartition            BOOLEAN = TRUE
) RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_data_node_detach' LANGUAGE C VOLATILE;

DROP FUNCTION _timescaledb_internal.attach_osm_table_chunk( hypertable REGCLASS, chunk REGCLASS);
DROP FUNCTION _timescaledb_internal.alter_job_set_hypertable_id( job_id INTEGER, hypertable REGCLASS );
DROP FUNCTION _timescaledb_internal.unfreeze_chunk( chunk REGCLASS);
-- Drop dimension partition metadata table
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.dimension_partition;
DROP TABLE IF EXISTS _timescaledb_catalog.dimension_partition;

DROP FUNCTION IF EXISTS timescaledb_experimental.add_policies;
DROP FUNCTION IF EXISTS timescaledb_experimental.remove_policies;
DROP FUNCTION IF EXISTS timescaledb_experimental.remove_all_policies;
DROP FUNCTION IF EXISTS timescaledb_experimental.alter_policies;
DROP FUNCTION IF EXISTS timescaledb_experimental.show_policies;
DROP FUNCTION IF EXISTS @extschema@.remove_continuous_aggregate_policy(REGCLASS, BOOL, BOOL);
CREATE FUNCTION @extschema@.remove_continuous_aggregate_policy(continuous_aggregate REGCLASS, if_not_exists BOOL = false)
RETURNS VOID
AS '@MODULE_PATHNAME@', 'ts_policy_refresh_cagg_remove'
LANGUAGE C VOLATILE STRICT;

DROP VIEW IF EXISTS timescaledb_experimental.policies;

--
-- Rebuild the catalog table `_timescaledb_catalog.chunk`
--
-- We need to recreate the catalog from scratch because when we drop a column
-- Postgres marks `pg_attribute.attisdropped=TRUE` instead of removing it from
-- the `pg_catalog.pg_attribute` table.
--
-- If we downgrade and upgrade the extension without rebuilding the catalog table it
-- will mess up `pg_attribute.attnum` and we will end up with issues when trying
-- to update data in those catalog tables.

-- Recreate _timescaledb_catalog.chunk table --
CREATE TABLE _timescaledb_internal.chunk_tmp
AS SELECT * from _timescaledb_catalog.chunk;

CREATE TABLE _timescaledb_internal.tmp_chunk_seq_value AS
SELECT last_value, is_called FROM _timescaledb_catalog.chunk_id_seq;

--drop foreign keys on chunk table
ALTER TABLE _timescaledb_catalog.chunk_constraint DROP CONSTRAINT
chunk_constraint_chunk_id_fkey;
ALTER TABLE _timescaledb_catalog.chunk_index DROP CONSTRAINT
chunk_index_chunk_id_fkey;
ALTER TABLE _timescaledb_catalog.chunk_data_node DROP CONSTRAINT
chunk_data_node_chunk_id_fkey;
ALTER TABLE _timescaledb_internal.bgw_policy_chunk_stats DROP CONSTRAINT
bgw_policy_chunk_stats_chunk_id_fkey;
ALTER TABLE _timescaledb_catalog.compression_chunk_size DROP CONSTRAINT
compression_chunk_size_chunk_id_fkey;
ALTER TABLE _timescaledb_catalog.compression_chunk_size DROP CONSTRAINT
compression_chunk_size_compressed_chunk_id_fkey;
ALTER TABLE _timescaledb_catalog.chunk_copy_operation DROP CONSTRAINT
chunk_copy_operation_chunk_id_fkey;

--drop dependent views
DROP VIEW IF EXISTS timescaledb_information.hypertables;
DROP VIEW IF EXISTS timescaledb_information.chunks;
DROP VIEW IF EXISTS _timescaledb_internal.hypertable_chunk_local_size;
DROP VIEW IF EXISTS _timescaledb_internal.compressed_chunk_stats;
DROP VIEW IF EXISTS timescaledb_experimental.chunk_replication_status;

ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.chunk;
ALTER EXTENSION timescaledb DROP SEQUENCE _timescaledb_catalog.chunk_id_seq;
DROP TABLE _timescaledb_catalog.chunk;

CREATE SEQUENCE _timescaledb_catalog.chunk_id_seq MINVALUE 1;

-- now create table without self referential foreign key
CREATE TABLE _timescaledb_catalog.chunk (
  id integer NOT NULL DEFAULT nextval('_timescaledb_catalog.chunk_id_seq'),
  hypertable_id int NOT NULL,
  schema_name name NOT NULL,
  table_name name NOT NULL,
  compressed_chunk_id integer ,
  dropped boolean NOT NULL DEFAULT FALSE,
  status integer NOT NULL DEFAULT 0,
  -- table constraints
  CONSTRAINT chunk_pkey PRIMARY KEY (id),
  CONSTRAINT chunk_schema_name_table_name_key UNIQUE (schema_name, table_name)
);

INSERT INTO _timescaledb_catalog.chunk
( id, hypertable_id, schema_name, table_name,
  compressed_chunk_id, dropped, status)
SELECT id, hypertable_id, schema_name, table_name,
  compressed_chunk_id, dropped, status
FROM _timescaledb_internal.chunk_tmp;

--add indexes to the chunk table
CREATE INDEX chunk_hypertable_id_idx ON _timescaledb_catalog.chunk (hypertable_id);
CREATE INDEX chunk_compressed_chunk_id_idx ON _timescaledb_catalog.chunk (compressed_chunk_id);

ALTER SEQUENCE _timescaledb_catalog.chunk_id_seq OWNED BY _timescaledb_catalog.chunk.id;
SELECT setval('_timescaledb_catalog.chunk_id_seq', last_value, is_called) FROM _timescaledb_internal.tmp_chunk_seq_value;

-- add self referential foreign key
ALTER TABLE _timescaledb_catalog.chunk ADD CONSTRAINT chunk_compressed_chunk_id_fkey FOREIGN KEY ( compressed_chunk_id )
 REFERENCES _timescaledb_catalog.chunk( id );

--add foreign key constraint
ALTER TABLE _timescaledb_catalog.chunk
      ADD CONSTRAINT chunk_hypertable_id_fkey
      FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk', '');
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_id_seq', '');

--add the foreign key constraints
ALTER TABLE _timescaledb_catalog.chunk_constraint ADD CONSTRAINT
chunk_constraint_chunk_id_fkey FOREIGN KEY (chunk_id) REFERENCES _timescaledb_catalog.chunk(id);
ALTER TABLE _timescaledb_catalog.chunk_index ADD CONSTRAINT
chunk_index_chunk_id_fkey FOREIGN KEY (chunk_id)
REFERENCES _timescaledb_catalog.chunk(id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.chunk_data_node ADD CONSTRAINT
chunk_data_node_chunk_id_fkey FOREIGN KEY (chunk_id) REFERENCES _timescaledb_catalog.chunk(id);
ALTER TABLE _timescaledb_internal.bgw_policy_chunk_stats ADD CONSTRAINT
bgw_policy_chunk_stats_chunk_id_fkey FOREIGN KEY (chunk_id)
REFERENCES _timescaledb_catalog.chunk(id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.compression_chunk_size ADD CONSTRAINT
compression_chunk_size_chunk_id_fkey FOREIGN KEY (chunk_id)
REFERENCES _timescaledb_catalog.chunk(id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.compression_chunk_size ADD CONSTRAINT
compression_chunk_size_compressed_chunk_id_fkey FOREIGN KEY (compressed_chunk_id)
REFERENCES _timescaledb_catalog.chunk(id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.chunk_copy_operation ADD CONSTRAINT
chunk_copy_operation_chunk_id_fkey FOREIGN KEY (chunk_id) REFERENCES _timescaledb_catalog.chunk (id) ON DELETE CASCADE;

--cleanup
DROP TABLE _timescaledb_internal.chunk_tmp;
DROP TABLE _timescaledb_internal.tmp_chunk_seq_value;

GRANT SELECT ON _timescaledb_catalog.chunk_id_seq TO PUBLIC;
GRANT SELECT ON _timescaledb_catalog.chunk TO PUBLIC;

-- end recreate _timescaledb_catalog.chunk table --

ALTER TABLE _timescaledb_internal.bgw_job_stat
      DROP CONSTRAINT bgw_job_stat_job_id_fkey;
ALTER TABLE _timescaledb_internal.bgw_policy_chunk_stats
      DROP CONSTRAINT bgw_policy_chunk_stats_chunk_id_fkey,
      DROP CONSTRAINT bgw_policy_chunk_stats_job_id_fkey;

ALTER TABLE _timescaledb_config.bgw_job
      DROP COLUMN check_schema,
      DROP COLUMN check_name;
CREATE TABLE _timescaledb_config.bgw_job_tmp (LIKE _timescaledb_config.bgw_job);
INSERT INTO _timescaledb_config.bgw_job_tmp SELECT * FROM _timescaledb_config.bgw_job;
CREATE TABLE _timescaledb_internal.tmp_bgw_job_seq_value AS
SELECT last_value, is_called FROM _timescaledb_config.bgw_job_id_seq;

ALTER EXTENSION timescaledb DROP TABLE _timescaledb_config.bgw_job;
ALTER EXTENSION timescaledb DROP SEQUENCE _timescaledb_config.bgw_job_id_seq;

DROP TABLE _timescaledb_config.bgw_job;

CREATE SEQUENCE _timescaledb_config.bgw_job_id_seq MINVALUE 1000;
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_config.bgw_job_id_seq', '');
SELECT pg_catalog.setval('_timescaledb_config.bgw_job_id_seq', last_value, is_called)
FROM _timescaledb_internal.tmp_bgw_job_seq_value;
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
  config jsonb
);

INSERT INTO _timescaledb_config.bgw_job SELECT * FROM _timescaledb_config.bgw_job_tmp ORDER BY id;
DROP TABLE _timescaledb_config.bgw_job_tmp;

ALTER SEQUENCE _timescaledb_config.bgw_job_id_seq OWNED BY _timescaledb_config.bgw_job.id;

CREATE INDEX bgw_job_proc_hypertable_id_idx
       ON _timescaledb_config.bgw_job (proc_schema, proc_name, hypertable_id);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_config.bgw_job', 'WHERE id >= 1000');
GRANT SELECT ON _timescaledb_config.bgw_job TO PUBLIC;
GRANT SELECT ON _timescaledb_config.bgw_job_id_seq TO PUBLIC;

ALTER TABLE _timescaledb_internal.bgw_job_stat
      ADD CONSTRAINT bgw_job_stat_job_id_fkey
          FOREIGN KEY(job_id) REFERENCES _timescaledb_config.bgw_job(id)
          ON DELETE CASCADE;
ALTER TABLE _timescaledb_internal.bgw_policy_chunk_stats
      ADD CONSTRAINT bgw_policy_chunk_stats_chunk_id_fkey
          FOREIGN KEY (chunk_id) REFERENCES _timescaledb_catalog.chunk(id)
          ON DELETE CASCADE,
      ADD CONSTRAINT bgw_policy_chunk_stats_job_id_fkey
          FOREIGN KEY(job_id) REFERENCES _timescaledb_config.bgw_job(id)
          ON DELETE CASCADE;

DROP FUNCTION IF EXISTS @extschema@.add_job(REGPROC, INTERVAL, JSONB, TIMESTAMPTZ, BOOL, REGPROC);
DROP FUNCTION IF EXISTS _timescaledb_internal.policy_retention_check(JSONB);
DROP FUNCTION IF EXISTS _timescaledb_internal.policy_compression_check(JSONB);
DROP FUNCTION IF EXISTS _timescaledb_internal.policy_reorder_check(JSONB);
DROP FUNCTION IF EXISTS _timescaledb_internal.policy_refresh_continuous_aggregate_check(JSONB);
DROP FUNCTION IF EXISTS @extschema@.alter_job(INTEGER, INTERVAL, INTERVAL, INTEGER, INTERVAL, BOOL, JSONB, TIMESTAMPTZ, BOOL, REGPROC);

CREATE FUNCTION @extschema@.add_job(proc REGPROC,
  schedule_interval INTERVAL,
  config JSONB = NULL,
  initial_start TIMESTAMPTZ = NULL,
  scheduled BOOL = true)
RETURNS INTEGER AS '@MODULE_PATHNAME@', 'ts_job_add' LANGUAGE C VOLATILE;

CREATE FUNCTION @extschema@.alter_job(
    job_id INTEGER,
    schedule_interval INTERVAL = NULL,
    max_runtime INTERVAL = NULL,
    max_retries INTEGER = NULL,
    retry_period INTERVAL = NULL,
    scheduled BOOL = NULL,
    config JSONB = NULL,
    next_start TIMESTAMPTZ = NULL,
    if_exists BOOL = FALSE
)
RETURNS TABLE (job_id INTEGER, schedule_interval INTERVAL, max_runtime INTERVAL, max_retries INTEGER, retry_period INTERVAL, scheduled BOOL, config JSONB, next_start TIMESTAMPTZ)
AS '@MODULE_PATHNAME@', 'ts_job_alter'
LANGUAGE C VOLATILE;

DROP FUNCTION @extschema@.time_bucket(INTERVAL, TIMESTAMPTZ, TEXT, TIMESTAMPTZ, INTERVAL);

-- reverse cagg catalog relations
DROP PROCEDURE @extschema@.cagg_migrate (REGCLASS, BOOLEAN, BOOLEAN);
DROP FUNCTION _timescaledb_internal.cagg_migrate_pre_validation (TEXT, TEXT, TEXT);
DROP PROCEDURE _timescaledb_internal.cagg_migrate_create_plan (_timescaledb_catalog.continuous_agg, TEXT, BOOLEAN, BOOLEAN);
DROP FUNCTION _timescaledb_internal.cagg_migrate_plan_exists (INTEGER);
DROP PROCEDURE _timescaledb_internal.cagg_migrate_execute_plan (_timescaledb_catalog.continuous_agg);
DROP PROCEDURE _timescaledb_internal.cagg_migrate_execute_create_new_cagg (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE _timescaledb_internal.cagg_migrate_execute_disable_policies (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE _timescaledb_internal.cagg_migrate_execute_enable_policies (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE _timescaledb_internal.cagg_migrate_execute_copy_policies (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE _timescaledb_internal.cagg_migrate_execute_refresh_new_cagg (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE _timescaledb_internal.cagg_migrate_execute_copy_data (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE _timescaledb_internal.cagg_migrate_execute_override_cagg (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE _timescaledb_internal.cagg_migrate_execute_drop_old_cagg (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);

ALTER EXTENSION timescaledb DROP SEQUENCE _timescaledb_catalog.continuous_agg_migrate_plan_step_step_id_seq;
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.continuous_agg_migrate_plan_step;
DROP TABLE _timescaledb_catalog.continuous_agg_migrate_plan_step;
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.continuous_agg_migrate_plan;
DROP TABLE _timescaledb_catalog.continuous_agg_migrate_plan;

DROP FUNCTION IF EXISTS @extschema@.create_hypertable(regclass,name,name,integer,name,name,anyelement,boolean,boolean,regproc,boolean,text,regproc,regproc,integer,name[],boolean);
DROP FUNCTION IF EXISTS @extschema@.create_distributed_hypertable(regclass,name,name,integer,name,name,anyelement,boolean,boolean,regproc,boolean,text,regproc,regproc,integer,name[]);

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
    data_nodes              NAME[] = NULL
) RETURNS TABLE(hypertable_id INT, schema_name NAME, table_name NAME, created BOOL) AS '@MODULE_PATHNAME@', 'ts_hypertable_create' LANGUAGE C VOLATILE;

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
    replication_factor      INTEGER = 1,
    data_nodes              NAME[] = NULL
) RETURNS TABLE(hypertable_id INT, schema_name NAME, table_name NAME, created BOOL) AS '@MODULE_PATHNAME@', 'ts_hypertable_distributed_create' LANGUAGE C VOLATILE;
