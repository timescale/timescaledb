--Drop functions in size_utils and dependencies, ordering matters.
-- Do not reorder
DROP VIEW IF EXISTS timescaledb_information.hypertable;
DROP FUNCTION IF EXISTS hypertable_relation_size_pretty;
DROP FUNCTION IF EXISTS hypertable_relation_size;
DROP FUNCTION IF EXISTS chunk_relation_size_pretty;
DROP FUNCTION IF EXISTS chunk_relation_size;
DROP FUNCTION IF EXISTS indexes_relation_size_pretty;
DROP FUNCTION IF EXISTS indexes_relation_size;
DROP FUNCTION IF EXISTS _timescaledb_internal.partitioning_column_to_pretty;
DROP FUNCTION IF EXISTS _timescaledb_internal.range_value_to_pretty;
-- end of do not reorder
DROP FUNCTION IF EXISTS hypertable_approximate_row_count;
DROP VIEW IF EXISTS timescaledb_information.compressed_chunk_stats;
DROP VIEW IF EXISTS timescaledb_information.compressed_hypertable_stats;
DROP VIEW IF EXISTS timescaledb_information.license;

-- Add new function definitions, columns and tables for distributed hypertables
DROP FUNCTION IF EXISTS create_hypertable(regclass,name,name,integer,name,name,anyelement,boolean,boolean,regproc,boolean,text,regproc,regproc);
DROP FUNCTION IF EXISTS add_drop_chunks_policy;
DROP FUNCTION IF EXISTS remove_drop_chunks_policy;
DROP FUNCTION IF EXISTS drop_chunks;
DROP FUNCTION IF EXISTS show_chunks;
DROP FUNCTION IF EXISTS add_compress_chunks_policy;
DROP FUNCTION IF EXISTS remove_compress_chunks_policy;
DROP FUNCTION IF EXISTS alter_job_schedule;
DROP FUNCTION IF EXISTS _timescaledb_internal.enterprise_enabled;
DROP FUNCTION IF EXISTS _timescaledb_internal.current_license_key;
DROP FUNCTION IF EXISTS _timescaledb_internal.license_expiration_time;
DROP FUNCTION IF EXISTS _timescaledb_internal.print_license_expiration_info;
DROP FUNCTION IF EXISTS _timescaledb_internal.license_edition;
DROP FUNCTION IF EXISTS _timescaledb_internal.current_db_set_license_key;

DROP VIEW IF EXISTS timescaledb_information.policy_stats;
DROP VIEW IF EXISTS timescaledb_information.drop_chunks_policies;
DROP VIEW IF EXISTS timescaledb_information.reorder_policies;

ALTER TABLE _timescaledb_catalog.hypertable ADD COLUMN replication_factor SMALLINT NULL CHECK (replication_factor > 0);

-- Table for hypertable -> node mappings
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.hypertable_data_node (
    hypertable_id          INTEGER NOT NULL     REFERENCES _timescaledb_catalog.hypertable(id),
    node_hypertable_id   INTEGER NULL,
    node_name            NAME NOT NULL,
    block_chunks           BOOLEAN NOT NULL,
    UNIQUE(node_hypertable_id, node_name),
    UNIQUE(hypertable_id, node_name)
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.hypertable_data_node', '');

GRANT SELECT ON _timescaledb_catalog.hypertable_data_node TO PUBLIC;

-- Table for chunk -> nodes mappings
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.chunk_data_node (
    chunk_id               INTEGER NOT NULL     REFERENCES _timescaledb_catalog.chunk(id),
    node_chunk_id        INTEGER NOT NULL,
    node_name            NAME NOT NULL,
    UNIQUE(node_chunk_id, node_name),
    UNIQUE(chunk_id, node_name)
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_data_node', '');

GRANT SELECT ON _timescaledb_catalog.chunk_data_node TO PUBLIC;

--placeholder to allow creation of functions below
CREATE TYPE rxid;

CREATE OR REPLACE FUNCTION _timescaledb_internal.rxid_in(cstring) RETURNS rxid
    AS '@MODULE_PATHNAME@', 'ts_remote_txn_id_in' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.rxid_out(rxid) RETURNS cstring
    AS '@MODULE_PATHNAME@', 'ts_remote_txn_id_out' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE TYPE rxid (
   internallength = 16,
   input = _timescaledb_internal.rxid_in,
   output = _timescaledb_internal.rxid_out
);

CREATE TABLE _timescaledb_catalog.remote_txn (
    data_node_name              NAME, --this is really only to allow us to cleanup stuff on a per-node basis.
    remote_transaction_id    TEXT CHECK (remote_transaction_id::rxid is not null),
    PRIMARY KEY (remote_transaction_id)
);
CREATE INDEX IF NOT EXISTS remote_txn_data_node_name_idx
ON _timescaledb_catalog.remote_txn(data_node_name);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.remote_txn', '');

GRANT SELECT ON _timescaledb_catalog.remote_txn TO PUBLIC;

DROP VIEW IF EXISTS timescaledb_information.compressed_hypertable_stats;
DROP VIEW IF EXISTS timescaledb_information.compressed_chunk_stats;

-- all existing compressed chunks have NULL value for the new columns
ALTER TABLE IF EXISTS _timescaledb_catalog.compression_chunk_size ADD COLUMN IF NOT EXISTS numrows_pre_compression BIGINT;
ALTER TABLE IF EXISTS _timescaledb_catalog.compression_chunk_size ADD COLUMN IF NOT EXISTS numrows_post_compression BIGINT;

--rewrite catalog table to not break catalog scans on tables with missingval optimization
CLUSTER  _timescaledb_catalog.compression_chunk_size USING compression_chunk_size_pkey;
ALTER TABLE _timescaledb_catalog.compression_chunk_size SET WITHOUT CLUSTER;

---Clean up constraints on hypertable catalog table ---
ALTER TABLE _timescaledb_catalog.hypertable ADD CONSTRAINT hypertable_table_name_schema_name_key UNIQUE(table_name, schema_name);
ALTER TABLE _timescaledb_catalog.hypertable DROP CONSTRAINT hypertable_schema_name_table_name_key;
ALTER TABLE _timescaledb_catalog.hypertable DROP CONSTRAINT hypertable_id_schema_name_key;

-- add fields for custom jobs/generic configuration to bgw_job table
ALTER TABLE _timescaledb_config.bgw_job ADD COLUMN proc_schema NAME NOT NULL DEFAULT '';
ALTER TABLE _timescaledb_config.bgw_job ADD COLUMN proc_name NAME NOT NULL DEFAULT '';
ALTER TABLE _timescaledb_config.bgw_job ADD COLUMN owner NAME NOT NULL DEFAULT CURRENT_ROLE;
ALTER TABLE _timescaledb_config.bgw_job ADD COLUMN scheduled BOOL NOT NULL DEFAULT true;
ALTER TABLE _timescaledb_config.bgw_job ADD COLUMN hypertable_id INTEGER REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_config.bgw_job ADD COLUMN config JSONB;

ALTER TABLE _timescaledb_config.bgw_job DROP CONSTRAINT valid_job_type;
ALTER TABLE _timescaledb_config.bgw_job ADD CONSTRAINT valid_job_type CHECK (job_type IN ('telemetry_and_version_check_if_enabled', 'reorder', 'drop_chunks', 'continuous_aggregate', 'compress_chunks', 'custom'));

-- migrate telemetry jobs
UPDATE
  _timescaledb_config.bgw_job job
SET
  application_name = format('%s [%s]', application_name, id),
  proc_schema = '_timescaledb_internal',
  proc_name = 'policy_telemetry',
  owner = CURRENT_ROLE
WHERE job_type = 'telemetry_and_version_check_if_enabled';

-- migrate reorder jobs
UPDATE
  _timescaledb_config.bgw_job job
SET
  application_name = format('%s [%s]', 'Reorder Policy', id),
  proc_schema = '_timescaledb_internal',
  proc_name = 'policy_reorder',
  config = jsonb_build_object('hypertable_id', reorder.hypertable_id, 'index_name', reorder.hypertable_index_name),
  hypertable_id = reorder.hypertable_id,
  owner = (
    SELECT relowner::regrole::text
    FROM _timescaledb_catalog.hypertable ht,
      pg_class cl
    WHERE ht.id = reorder.hypertable_id
      AND cl.oid = format('%I.%I', schema_name, table_name)::regclass)
FROM _timescaledb_config.bgw_policy_reorder reorder
WHERE job_type = 'reorder'
  AND job.id = reorder.job_id;

-- migrate compression jobs
UPDATE
  _timescaledb_config.bgw_job job
SET
  application_name = format('%s [%s]', 'Compression Policy', id),
  proc_schema = '_timescaledb_internal',
  proc_name = 'policy_compression',
  config = jsonb_build_object('hypertable_id', c.hypertable_id, 'older_than', CASE WHEN (older_than).is_time_interval THEN
    (older_than).time_interval::text
  ELSE
    (older_than).integer_interval::text
    END),
  hypertable_id = c.hypertable_id,
  owner = (
    SELECT relowner::regrole::text
    FROM _timescaledb_catalog.hypertable ht,
      pg_class cl
    WHERE ht.id = c.hypertable_id
      AND cl.oid = format('%I.%I', schema_name, table_name)::regclass)
FROM _timescaledb_config.bgw_policy_compress_chunks c
WHERE job_type = 'compress_chunks'
  AND job.id = c.job_id;

-- migrate retention jobs
UPDATE
  _timescaledb_config.bgw_job job
SET
  application_name = format('%s [%s]', 'Retention Policy', id),
  proc_schema = '_timescaledb_internal',
  proc_name = 'policy_retention',
  config = jsonb_build_object('hypertable_id', c.hypertable_id, 'retention_window', CASE WHEN (older_than).is_time_interval THEN
    (older_than).time_interval::text
  ELSE
    (older_than).integer_interval::text
    END),
  hypertable_id = c.hypertable_id,
  owner = (
    SELECT relowner::regrole::text
    FROM _timescaledb_catalog.hypertable ht,
      pg_class cl
    WHERE ht.id = c.hypertable_id
      AND cl.oid = format('%I.%I', schema_name, table_name)::regclass)
FROM _timescaledb_config.bgw_policy_drop_chunks c
WHERE job_type = 'drop_chunks'
  AND job.id = c.job_id;

-- migrate cagg jobs
--- timescale functions cannot be invoked in latest-dev.sql
--- this is a mapping for get_time_type
CREATE FUNCTION ts_tmp_get_time_type(htid integer)
RETURNS OID LANGUAGE SQL AS
$BODY$
  SELECT dim.column_type
  FROM _timescaledb_catalog.dimension dim
  WHERE dim.hypertable_id = htid and dim.num_slices is null
        and dim.interval_length is not null;
$BODY$;
--- this is a mapping for _timescaledb_internal.to_interval
CREATE FUNCTION ts_tmp_get_interval( intval bigint)
RETURNS INTERVAL LANGUAGE SQL AS
$BODY$
   SELECT format('%sd %ss', intval/86400000000, (intval%86400000000)/1E6)::interval; 
$BODY$;
 
UPDATE
  _timescaledb_config.bgw_job job
SET
  application_name = format('%s [%s]', 'Refresh Continuous Aggregate Policy', id),
  proc_schema = '_timescaledb_internal',
  proc_name = 'policy_refresh_continuous_aggregate',
  job_type = 'custom',
  config = jsonb_build_object('mat_hypertable_id', cagg.mat_hypertable_id, 'start_interval', 
   CASE WHEN cagg.ignore_invalidation_older_than IS NULL 
      THEN NULL 
      WHEN cagg.ignore_invalidation_older_than = 9223372036854775807 
         AND  
            ts_tmp_get_time_type( cagg.raw_hypertable_id ) IN  ('TIMESTAMP'::regtype, 'DATE'::regtype, 'TIMESTAMPTZ'::regtype) 
      THEN NULL 
      ELSE
         CASE WHEN 
            ts_tmp_get_time_type( cagg.raw_hypertable_id ) IN  ('TIMESTAMP'::regtype, 'DATE'::regtype, 'TIMESTAMPTZ'::regtype) 
            THEN ts_tmp_get_interval(cagg.ignore_invalidation_older_than)::TEXT 
            ELSE cagg.ignore_invalidation_older_than::TEXT 
         END
   END,
   'end_interval', 
   CASE
      WHEN 
            ts_tmp_get_time_type( cagg.raw_hypertable_id ) IN  ('TIMESTAMP'::regtype, 'DATE'::regtype, 'TIMESTAMPTZ'::regtype) 
      THEN ts_tmp_get_interval(cagg.refresh_lag)::TEXT 
      ELSE cagg.refresh_lag::TEXT 
   END
),
  hypertable_id = cagg.mat_hypertable_id,
  owner = (
    SELECT relowner::regrole::text
    FROM _timescaledb_catalog.hypertable ht,
      pg_class cl
    WHERE ht.id = cagg.mat_hypertable_id
      AND cl.oid = format('%I.%I', schema_name, table_name)::regclass)
FROM _timescaledb_catalog.continuous_agg cagg
WHERE job_type = 'continuous_aggregate'
  AND job.id = cagg.job_id ;

--drop tmp functions created for cont agg job migration
DROP FUNCTION ts_tmp_get_time_type;
DROP FUNCTION ts_tmp_get_interval;

ALTER EXTENSION timescaledb DROP TABLE _timescaledb_config.bgw_policy_reorder;
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_config.bgw_policy_compress_chunks;
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_config.bgw_policy_drop_chunks;
DROP TABLE IF EXISTS _timescaledb_config.bgw_policy_reorder CASCADE;
DROP TABLE IF EXISTS _timescaledb_config.bgw_policy_compress_chunks;
DROP TABLE IF EXISTS _timescaledb_config.bgw_policy_drop_chunks;

DROP FUNCTION IF EXISTS _timescaledb_internal.valid_ts_interval;
DROP TYPE IF EXISTS _timescaledb_catalog.ts_interval;

DROP VIEW IF EXISTS timescaledb_information.continuous_aggregates;
DROP VIEW IF EXISTS timescaledb_information.continuous_aggregate_stats;
ALTER TABLE IF EXISTS _timescaledb_catalog.continuous_agg DROP COLUMN IF EXISTS job_id;

-- rebuild bgw_job table
CREATE TABLE _timescaledb_config.bgw_job_tmp AS SELECT * FROM _timescaledb_config.bgw_job;
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_config.bgw_job;
ALTER EXTENSION timescaledb DROP SEQUENCE _timescaledb_config.bgw_job_id_seq;
ALTER TABLE _timescaledb_internal.bgw_job_stat DROP CONSTRAINT IF EXISTS bgw_job_stat_job_id_fkey;
ALTER TABLE _timescaledb_internal.bgw_policy_chunk_stats DROP CONSTRAINT IF EXISTS bgw_policy_chunk_stats_job_id_fkey;

-- remember sequence values so they can be restored in new sequence
CREATE TABLE tmp_bgw_job_seq_value AS SELECT last_value, is_called FROM _timescaledb_config.bgw_job_id_seq;

DROP TABLE _timescaledb_config.bgw_job;

CREATE SEQUENCE IF NOT EXISTS _timescaledb_config.bgw_job_id_seq MINVALUE 1000;
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_config.bgw_job_id_seq', '');
SELECT setval('_timescaledb_config.bgw_job_id_seq', last_value, is_called) FROM tmp_bgw_job_seq_value;
DROP TABLE tmp_bgw_job_seq_value;

CREATE TABLE IF NOT EXISTS _timescaledb_config.bgw_job (
    id                  INTEGER PRIMARY KEY DEFAULT nextval('_timescaledb_config.bgw_job_id_seq'),
    application_name    NAME        NOT NULL,
    schedule_interval   INTERVAL    NOT NULL,
    max_runtime         INTERVAL    NOT NULL,
    max_retries         INTEGER     NOT NULL,
    retry_period        INTERVAL    NOT NULL,
    proc_schema         NAME        NOT NULL,
    proc_name           NAME        NOT NULL,
    owner               NAME        NOT NULL DEFAULT CURRENT_ROLE,
    scheduled           BOOL        NOT NULL DEFAULT true,
    hypertable_id       INTEGER REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
    config              JSONB
);

ALTER SEQUENCE _timescaledb_config.bgw_job_id_seq OWNED BY _timescaledb_config.bgw_job.id;
CREATE INDEX IF NOT EXISTS bgw_job_proc_hypertable_id_idx ON _timescaledb_config.bgw_job(proc_schema,proc_name,hypertable_id);

INSERT INTO _timescaledb_config.bgw_job SELECT id, application_name, schedule_interval, max_runtime, max_retries, retry_period, proc_schema, proc_name, owner, scheduled, hypertable_id, config FROM _timescaledb_config.bgw_job_tmp ORDER BY id;
DROP TABLE _timescaledb_config.bgw_job_tmp;
ALTER TABLE _timescaledb_internal.bgw_job_stat ADD CONSTRAINT bgw_job_stat_job_id_fkey FOREIGN KEY(job_id) REFERENCES _timescaledb_config.bgw_job(id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_internal.bgw_policy_chunk_stats ADD CONSTRAINT bgw_policy_chunk_stats_job_id_fkey FOREIGN KEY(job_id) REFERENCES _timescaledb_config.bgw_job(id) ON DELETE CASCADE;

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_config.bgw_job', 'WHERE id >= 1000');
GRANT SELECT ON _timescaledb_config.bgw_job TO PUBLIC;
GRANT SELECT ON _timescaledb_config.bgw_job_id_seq TO PUBLIC;

-- Add entry to materialization invalidation log to indicate that [watermark, +infinity) is invalid
INSERT INTO _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
SELECT materialization_id, BIGINT '-9223372036854775808', watermark, 9223372036854775807
FROM _timescaledb_catalog.continuous_aggs_completed_threshold;
-- Also handle continuous aggs that have never been run
INSERT INTO _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
SELECT (SELECT mat_hypertable_id FROM _timescaledb_catalog.continuous_agg EXCEPT SELECT materialization_id FROM _timescaledb_catalog.continuous_aggs_completed_threshold), 
-9223372036854775808, -9223372036854775808, 9223372036854775807;

-- drop completed_threshold table, which is no longer used
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.continuous_aggs_completed_threshold;
DROP TABLE IF EXISTS _timescaledb_catalog.continuous_aggs_completed_threshold;

-- rebuild continuous aggregate table
CREATE TABLE _timescaledb_catalog.continuous_agg_tmp AS SELECT * FROM _timescaledb_catalog.continuous_agg;

ALTER TABLE _timescaledb_catalog.continuous_aggs_materialization_invalidation_log DROP CONSTRAINT continuous_aggs_materialization_invalid_materialization_id_fkey;

ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.continuous_agg;
DROP TABLE _timescaledb_catalog.continuous_agg;

CREATE TABLE IF NOT EXISTS _timescaledb_catalog.continuous_agg (
    mat_hypertable_id INTEGER PRIMARY KEY REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
    raw_hypertable_id INTEGER NOT NULL REFERENCES  _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
    user_view_schema NAME NOT NULL,
    user_view_name NAME NOT NULL,
    partial_view_schema NAME NOT NULL,
    partial_view_name NAME NOT NULL,
    bucket_width  BIGINT NOT NULL,
    direct_view_schema NAME NOT NULL,
    direct_view_name NAME NOT NULL,
    materialized_only BOOL NOT NULL DEFAULT false,
    UNIQUE(user_view_schema, user_view_name),
    UNIQUE(partial_view_schema, partial_view_name)
);

CREATE INDEX IF NOT EXISTS continuous_agg_raw_hypertable_id_idx
    ON _timescaledb_catalog.continuous_agg(raw_hypertable_id);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_agg', '');
GRANT SELECT ON _timescaledb_catalog.continuous_agg TO PUBLIC;

INSERT INTO _timescaledb_catalog.continuous_agg SELECT mat_hypertable_id,raw_hypertable_id,user_view_schema,user_view_name,partial_view_schema,partial_view_name,bucket_width,direct_view_schema,direct_view_name,materialized_only FROM _timescaledb_catalog.continuous_agg_tmp;
DROP TABLE _timescaledb_catalog.continuous_agg_tmp;

ALTER TABLE _timescaledb_catalog.continuous_aggs_materialization_invalidation_log ADD CONSTRAINT continuous_aggs_materialization_invalid_materialization_id_fkey FOREIGN KEY(materialization_id) REFERENCES _timescaledb_catalog.continuous_agg(mat_hypertable_id);

-- disable autovacuum for compressed chunks
DO $$
DECLARE
  chunk regclass;
BEGIN
  FOR chunk IN
  SELECT format('%I.%I', schema_name, table_name)::regclass
    FROM _timescaledb_catalog.chunk WHERE compressed_chunk_id IS NOT NULL
  LOOP
    EXECUTE format('ALTER TABLE %s SET (autovacuum_enabled=false);', chunk::text);
  END LOOP;
END
$$;

