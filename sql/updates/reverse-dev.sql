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

CREATE FUNCTION @extschema@.add_job(proc REGPROC,
  schedule_interval INTERVAL,
  config JSONB = NULL,
  initial_start TIMESTAMPTZ = NULL,
  scheduled BOOL = true)
RETURNS INTEGER AS '@MODULE_PATHNAME@', 'ts_job_add' LANGUAGE C VOLATILE;
