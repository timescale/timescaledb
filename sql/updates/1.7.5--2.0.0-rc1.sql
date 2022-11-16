--Drop functions in size_utils and dependencies, ordering matters.
-- Do not reorder
DROP VIEW IF EXISTS timescaledb_information.hypertable;
DROP FUNCTION IF EXISTS @extschema@.hypertable_relation_size_pretty;
DROP FUNCTION IF EXISTS @extschema@.hypertable_relation_size;
DROP FUNCTION IF EXISTS @extschema@.chunk_relation_size_pretty;
DROP FUNCTION IF EXISTS @extschema@.chunk_relation_size;
DROP FUNCTION IF EXISTS @extschema@.indexes_relation_size_pretty;
DROP FUNCTION IF EXISTS @extschema@.indexes_relation_size;
DROP FUNCTION IF EXISTS _timescaledb_internal.partitioning_column_to_pretty;
DROP FUNCTION IF EXISTS _timescaledb_internal.range_value_to_pretty;
-- end of do not reorder
DROP FUNCTION IF EXISTS @extschema@.hypertable_approximate_row_count;
DROP VIEW IF EXISTS timescaledb_information.compressed_chunk_stats;
DROP VIEW IF EXISTS timescaledb_information.compressed_hypertable_stats;
DROP VIEW IF EXISTS timescaledb_information.license;

-- Add new function definitions, columns and tables for distributed hypertables
DROP FUNCTION IF EXISTS @extschema@.create_hypertable(regclass,name,name,integer,name,name,anyelement,boolean,boolean,regproc,boolean,text,regproc,regproc);
DROP FUNCTION IF EXISTS @extschema@.add_drop_chunks_policy;
DROP FUNCTION IF EXISTS @extschema@.remove_drop_chunks_policy;
DROP FUNCTION IF EXISTS @extschema@.drop_chunks;
DROP FUNCTION IF EXISTS @extschema@.show_chunks;
DROP FUNCTION IF EXISTS @extschema@.add_compress_chunks_policy;
DROP FUNCTION IF EXISTS @extschema@.remove_compress_chunks_policy;
DROP FUNCTION IF EXISTS @extschema@.alter_job_schedule;
DROP FUNCTION IF EXISTS @extschema@.set_chunk_time_interval;
DROP FUNCTION IF EXISTS @extschema@.set_number_partitions;
DROP FUNCTION IF EXISTS @extschema@.add_dimension;
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
CREATE TABLE _timescaledb_catalog.hypertable_data_node (
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
CREATE TABLE _timescaledb_catalog.chunk_data_node (
    chunk_id               INTEGER NOT NULL     REFERENCES _timescaledb_catalog.chunk(id),
    node_chunk_id        INTEGER NOT NULL,
    node_name            NAME NOT NULL,
    UNIQUE(node_chunk_id, node_name),
    UNIQUE(chunk_id, node_name)
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_data_node', '');

GRANT SELECT ON _timescaledb_catalog.chunk_data_node TO PUBLIC;

--placeholder to allow creation of functions below
CREATE TYPE @extschema@.rxid;

CREATE FUNCTION _timescaledb_internal.rxid_in(cstring) RETURNS @extschema@.rxid
    AS '@MODULE_PATHNAME@', 'ts_remote_txn_id_in' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION _timescaledb_internal.rxid_out(@extschema@.rxid) RETURNS cstring
    AS '@MODULE_PATHNAME@', 'ts_remote_txn_id_out' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE TYPE @extschema@.rxid (
   internallength = 16,
   input = _timescaledb_internal.rxid_in,
   output = _timescaledb_internal.rxid_out
);

CREATE TABLE _timescaledb_catalog.remote_txn (
    data_node_name              NAME, --this is really only to allow us to cleanup stuff on a per-node basis.
    remote_transaction_id    TEXT CHECK (remote_transaction_id::@extschema@.rxid is not null),
    PRIMARY KEY (remote_transaction_id)
);
CREATE INDEX remote_txn_data_node_name_idx
ON _timescaledb_catalog.remote_txn(data_node_name);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.remote_txn', '');

GRANT SELECT ON _timescaledb_catalog.remote_txn TO PUBLIC;

DROP VIEW IF EXISTS timescaledb_information.compressed_hypertable_stats;
DROP VIEW IF EXISTS timescaledb_information.compressed_chunk_stats;

-- all existing compressed chunks have NULL value for the new columns
ALTER TABLE IF EXISTS _timescaledb_catalog.compression_chunk_size ADD COLUMN numrows_pre_compression BIGINT;
ALTER TABLE IF EXISTS _timescaledb_catalog.compression_chunk_size ADD COLUMN numrows_post_compression BIGINT;

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
  config = jsonb_build_object('hypertable_id', c.hypertable_id, 'compress_after', CASE WHEN (older_than).is_time_interval THEN
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
  config = jsonb_build_object('hypertable_id', c.hypertable_id, 'drop_after', CASE WHEN (older_than).is_time_interval THEN
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
CREATE FUNCTION _timescaledb_internal.ts_tmp_get_time_type(htid integer)
RETURNS OID LANGUAGE SQL AS
$BODY$
  SELECT dim.column_type
  FROM _timescaledb_catalog.dimension dim
  WHERE dim.hypertable_id = htid and dim.num_slices is null
        and dim.interval_length is not null;
$BODY$;
--- this is a mapping for _timescaledb_internal.to_interval
CREATE FUNCTION _timescaledb_internal.ts_tmp_get_interval( intval bigint)
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
  config =
    CASE WHEN _timescaledb_internal.ts_tmp_get_time_type( cagg.raw_hypertable_id ) IN  ('TIMESTAMP'::regtype, 'DATE'::regtype, 'TIMESTAMPTZ'::regtype)
    THEN
    jsonb_build_object('mat_hypertable_id', cagg.mat_hypertable_id, 'start_offset',
        CASE WHEN cagg.ignore_invalidation_older_than IS NULL OR cagg.ignore_invalidation_older_than = 9223372036854775807
            THEN NULL
            ELSE _timescaledb_internal.ts_tmp_get_interval(cagg.ignore_invalidation_older_than)::TEXT
        END
    , 'end_offset', _timescaledb_internal.ts_tmp_get_interval(cagg.refresh_lag)::TEXT)
    ELSE
    jsonb_build_object('mat_hypertable_id', cagg.mat_hypertable_id, 'start_offset',
        CASE WHEN cagg.ignore_invalidation_older_than IS NULL OR cagg.ignore_invalidation_older_than = 9223372036854775807
            THEN NULL
            ELSE cagg.ignore_invalidation_older_than::BIGINT
        END
    , 'end_offset', cagg.refresh_lag::BIGINT)
    END,
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
DROP FUNCTION _timescaledb_internal.ts_tmp_get_time_type;
DROP FUNCTION _timescaledb_internal.ts_tmp_get_interval;

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
CREATE TABLE _timescaledb_internal.tmp_bgw_job_seq_value AS SELECT last_value, is_called FROM _timescaledb_config.bgw_job_id_seq;

DROP TABLE _timescaledb_config.bgw_job;

CREATE SEQUENCE _timescaledb_config.bgw_job_id_seq MINVALUE 1000;
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_config.bgw_job_id_seq', '');
SELECT setval('_timescaledb_config.bgw_job_id_seq', last_value, is_called) FROM _timescaledb_internal.tmp_bgw_job_seq_value;
DROP TABLE _timescaledb_internal.tmp_bgw_job_seq_value;

CREATE TABLE _timescaledb_config.bgw_job (
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
CREATE INDEX bgw_job_proc_hypertable_id_idx ON _timescaledb_config.bgw_job(proc_schema,proc_name,hypertable_id);

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
SELECT unrun_cagg.id, -9223372036854775808, -9223372036854775808, 9223372036854775807 FROM
(SELECT mat_hypertable_id id FROM _timescaledb_catalog.continuous_agg EXCEPT SELECT materialization_id FROM _timescaledb_catalog.continuous_aggs_completed_threshold) as unrun_cagg;

-- Also add an invalidation from [-infinity, now() - ignore_invaliation_older_than] to cover any missed invalidations
-- For NULL or inf ignore_invalidations_older_than, use julian 0 for consistency with 2.0 (for int tables, use INT_MIN - 1)
DO $$
DECLARE
  cagg _timescaledb_catalog.continuous_agg%ROWTYPE;
  dimrow _timescaledb_catalog.dimension%ROWTYPE;
  end_val bigint;
  getendval text;
BEGIN
    FOR cagg in SELECT * FROM _timescaledb_catalog.continuous_agg
    LOOP
        SELECT * INTO dimrow
        FROM _timescaledb_catalog.dimension dim
        WHERE dim.hypertable_id = cagg.raw_hypertable_id AND dim.num_slices IS NULL AND dim.interval_length IS NOT NULL;

        IF dimrow.column_type IN  ('TIMESTAMP'::regtype, 'DATE'::regtype, 'TIMESTAMPTZ'::regtype)
        THEN
            IF cagg.ignore_invalidation_older_than IS NULL OR cagg.ignore_invalidation_older_than = 9223372036854775807
            THEN
                end_val := -210866803200000001;
            ELSE
                end_val := (extract(epoch from now()) * 1000000 - cagg.ignore_invalidation_older_than)::int8;
            END IF;
        ELSE
            IF cagg.ignore_invalidation_older_than IS NULL OR cagg.ignore_invalidation_older_than = 9223372036854775807
            THEN
                end_val := -2147483649;
            ELSE
                getendval := format('SELECT %s.%s() - %s', dimrow.integer_now_func_schema, dimrow.integer_now_func, cagg.ignore_invalidation_older_than);
                EXECUTE getendval INTO end_val;
            END IF;
        END IF;

        INSERT INTO _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
          VALUES (cagg.mat_hypertable_id, -9223372036854775808, -9223372036854775808, end_val);
    END LOOP;
END $$;

-- drop completed_threshold table, which is no longer used
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.continuous_aggs_completed_threshold;
DROP TABLE IF EXISTS _timescaledb_catalog.continuous_aggs_completed_threshold;

-- rebuild continuous aggregate table
CREATE TABLE _timescaledb_catalog.continuous_agg_tmp AS SELECT * FROM _timescaledb_catalog.continuous_agg;

ALTER TABLE _timescaledb_catalog.continuous_aggs_materialization_invalidation_log DROP CONSTRAINT continuous_aggs_materialization_invalid_materialization_id_fkey;

ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.continuous_agg;
DROP TABLE _timescaledb_catalog.continuous_agg;

CREATE TABLE _timescaledb_catalog.continuous_agg (
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

CREATE INDEX continuous_agg_raw_hypertable_id_idx
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
    FROM _timescaledb_catalog.chunk WHERE compressed_chunk_id IS NOT NULL AND NOT dropped
  LOOP
    EXECUTE format('ALTER TABLE %s SET (autovacuum_enabled=false);', chunk::text);
  END LOOP;
END
$$;

CREATE FUNCTION @extschema@.timescaledb_fdw_handler()
RETURNS fdw_handler
AS '@MODULE_PATHNAME@', 'ts_timescaledb_fdw_handler'
LANGUAGE C STRICT;

CREATE FUNCTION @extschema@.timescaledb_fdw_validator(text[], oid)
RETURNS void
AS '@MODULE_PATHNAME@', 'ts_timescaledb_fdw_validator'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER timescaledb_fdw
  HANDLER @extschema@.timescaledb_fdw_handler
  VALIDATOR @extschema@.timescaledb_fdw_validator;

-- policy functions
CREATE FUNCTION @extschema@.add_retention_policy(relation REGCLASS, drop_after "any", if_not_exists BOOL = false)
RETURNS INTEGER AS '@MODULE_PATHNAME@', 'ts_policy_retention_add' LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION @extschema@.remove_retention_policy(relation REGCLASS, if_exists BOOL = false)
RETURNS VOID AS '@MODULE_PATHNAME@', 'ts_policy_retention_remove' LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION @extschema@.add_compression_policy(hypertable REGCLASS, compress_after "any", if_not_exists BOOL = false)
RETURNS INTEGER AS '@MODULE_PATHNAME@', 'ts_policy_compression_add' LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION @extschema@.remove_compression_policy(hypertable REGCLASS, if_exists BOOL = false) RETURNS BOOL
AS '@MODULE_PATHNAME@', 'ts_policy_compression_remove' LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION @extschema@.add_continuous_aggregate_policy(continuous_aggregate REGCLASS, start_offset "any", end_offset "any", schedule_interval INTERVAL, if_not_exists BOOL = false)
RETURNS INTEGER AS '@MODULE_PATHNAME@', 'ts_policy_refresh_cagg_add' LANGUAGE C VOLATILE;

CREATE FUNCTION @extschema@.remove_continuous_aggregate_policy(continuous_aggregate REGCLASS, if_not_exists BOOL = false)
RETURNS VOID AS '@MODULE_PATHNAME@', 'ts_policy_refresh_cagg_remove' LANGUAGE C VOLATILE STRICT;

-- job api
CREATE FUNCTION @extschema@.add_job(
  proc REGPROC,
  schedule_interval INTERVAL,
  config JSONB DEFAULT NULL,
  initial_start TIMESTAMPTZ DEFAULT NULL,
  scheduled BOOL DEFAULT true
) RETURNS INTEGER AS '@MODULE_PATHNAME@', 'ts_job_add' LANGUAGE C VOLATILE;

CREATE FUNCTION @extschema@.delete_job(job_id INTEGER) RETURNS VOID AS '@MODULE_PATHNAME@', 'ts_job_delete' LANGUAGE C VOLATILE STRICT;
CREATE PROCEDURE @extschema@.run_job(job_id INTEGER) AS '@MODULE_PATHNAME@', 'ts_job_run' LANGUAGE C;

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

-- ddl api
CREATE FUNCTION @extschema@.attach_data_node(
    node_name              NAME,
    hypertable             REGCLASS,
    if_not_attached        BOOLEAN = FALSE,
    repartition            BOOLEAN = TRUE
) RETURNS TABLE(hypertable_id INTEGER, node_hypertable_id INTEGER, node_name NAME)
AS '@MODULE_PATHNAME@', 'ts_data_node_attach' LANGUAGE C VOLATILE;

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

CREATE FUNCTION @extschema@.drop_chunks(
    relation               REGCLASS,
    older_than             "any" = NULL,
    newer_than             "any" = NULL,
    verbose                BOOLEAN = FALSE
) RETURNS SETOF TEXT AS '@MODULE_PATHNAME@', 'ts_chunk_drop_chunks'
LANGUAGE C VOLATILE PARALLEL UNSAFE;

CREATE PROCEDURE @extschema@.refresh_continuous_aggregate(
    cagg                     REGCLASS,
    window_start             "any",
    window_end               "any"
) LANGUAGE C AS '@MODULE_PATHNAME@', 'ts_continuous_agg_refresh';

CREATE FUNCTION @extschema@.set_replication_factor(
    hypertable              REGCLASS,
    replication_factor      INTEGER
) RETURNS VOID
AS '@MODULE_PATHNAME@', 'ts_hypertable_distributed_set_replication_factor' LANGUAGE C VOLATILE;

-- size utils
CREATE FUNCTION @extschema@.approximate_row_count(relation REGCLASS)
RETURNS BIGINT
LANGUAGE SQL VOLATILE STRICT AS
$BODY$
  WITH RECURSIVE inherited_id(oid) AS
  (
    SELECT relation
    UNION ALL
    SELECT i.inhrelid
    FROM pg_inherits i
    JOIN inherited_id b ON i.inhparent = b.oid
  )
  -- reltuples for partitioned tables is the sum of it's children in pg14 so we need to filter those out
  SELECT COALESCE((SUM(reltuples) FILTER (WHERE reltuples > 0 AND relkind <> 'p')), 0)::BIGINT
  FROM inherited_id
  JOIN pg_class USING (oid);
$BODY$;

CREATE VIEW _timescaledb_internal.compressed_chunk_stats AS
SELECT
    srcht.schema_name AS hypertable_schema,
    srcht.table_name AS hypertable_name,
    srcch.schema_name AS chunk_schema,
    srcch.table_name AS chunk_name,
    CASE WHEN srcch.compressed_chunk_id IS NULL THEN
        'Uncompressed'::text
    ELSE
        'Compressed'::text
    END AS compression_status,
    map.uncompressed_heap_size,
    map.uncompressed_index_size,
    map.uncompressed_toast_size,
    map.uncompressed_heap_size + map.uncompressed_toast_size + map.uncompressed_index_size AS uncompressed_total_size,
    map.compressed_heap_size,
    map.compressed_index_size,
    map.compressed_toast_size,
    map.compressed_heap_size + map.compressed_toast_size + map.compressed_index_size AS compressed_total_size
FROM
    _timescaledb_catalog.hypertable AS srcht
    JOIN _timescaledb_catalog.chunk AS srcch ON srcht.id = srcch.hypertable_id
        AND srcht.compressed_hypertable_id IS NOT NULL
        AND srcch.dropped = FALSE
    LEFT JOIN _timescaledb_catalog.compression_chunk_size map ON srcch.id = map.chunk_id;

CREATE FUNCTION _timescaledb_internal.data_node_compressed_chunk_stats (node_name name, schema_name_in name, table_name_in name)
    RETURNS TABLE (
        chunk_schema name,
        chunk_name name,
        compression_status text,
        before_compression_table_bytes bigint,
        before_compression_index_bytes bigint,
        before_compression_toast_bytes bigint,
        before_compression_total_bytes bigint,
        after_compression_table_bytes bigint,
        after_compression_index_bytes bigint,
        after_compression_toast_bytes bigint,
        after_compression_total_bytes bigint
    )
AS '@MODULE_PATHNAME@' , 'ts_dist_remote_compressed_chunk_info' LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION _timescaledb_internal.compressed_chunk_local_stats (schema_name_in name, table_name_in name)
    RETURNS TABLE (
        chunk_schema name,
        chunk_name name,
        compression_status text,
        before_compression_table_bytes bigint,
        before_compression_index_bytes bigint,
        before_compression_toast_bytes bigint,
        before_compression_total_bytes bigint,
        after_compression_table_bytes bigint,
        after_compression_index_bytes bigint,
        after_compression_toast_bytes bigint,
        after_compression_total_bytes bigint)
    LANGUAGE SQL
    STABLE STRICT
    AS
$BODY$
    SELECT
        ch.chunk_schema,
        ch.chunk_name,
        ch.compression_status,
        ch.uncompressed_heap_size,
        ch.uncompressed_index_size,
        ch.uncompressed_toast_size,
        ch.uncompressed_total_size,
        ch.compressed_heap_size,
        ch.compressed_index_size,
        ch.compressed_toast_size,
        ch.compressed_total_size
    FROM
        _timescaledb_internal.compressed_chunk_stats ch
    WHERE
        ch.hypertable_schema = schema_name_in
        AND ch.hypertable_name = table_name_in;
$BODY$;

CREATE FUNCTION _timescaledb_internal.compressed_chunk_remote_stats (schema_name_in name, table_name_in name)
    RETURNS TABLE (
        chunk_schema name,
        chunk_name name,
        compression_status text,
        before_compression_table_bytes bigint,
        before_compression_index_bytes bigint,
        before_compression_toast_bytes bigint,
        before_compression_total_bytes bigint,
        after_compression_table_bytes bigint,
        after_compression_index_bytes bigint,
        after_compression_toast_bytes bigint,
        after_compression_total_bytes bigint,
        node_name name)
    LANGUAGE SQL
    STABLE STRICT
    AS
$BODY$
    SELECT
        ch.*,
        srv.node_name
    FROM (
        SELECT
            s.node_name
        FROM
            _timescaledb_catalog.hypertable AS ht,
            _timescaledb_catalog.hypertable_data_node AS s
        WHERE
            ht.schema_name = schema_name_in
            AND ht.table_name = table_name_in
            AND s.hypertable_id = ht.id) AS srv
    LEFT OUTER JOIN LATERAL _timescaledb_internal.data_node_compressed_chunk_stats (
        srv.node_name, schema_name_in, table_name_in) ch ON TRUE
	WHERE ch.chunk_name IS NOT NULL;
$BODY$;

CREATE FUNCTION @extschema@.chunk_compression_stats (hypertable REGCLASS)
    RETURNS TABLE (
        chunk_schema name,
        chunk_name name,
        compression_status text,
        before_compression_table_bytes bigint,
        before_compression_index_bytes bigint,
        before_compression_toast_bytes bigint,
        before_compression_total_bytes bigint,
        after_compression_table_bytes bigint,
        after_compression_index_bytes bigint,
        after_compression_toast_bytes bigint,
        after_compression_total_bytes bigint,
        node_name name)
    LANGUAGE PLPGSQL
    STABLE STRICT
    AS $BODY$
DECLARE
    table_name name;
    schema_name name;
    is_distributed bool;
BEGIN
    SELECT
        relname,
        nspname,
        replication_factor > 0
    INTO
	    table_name,
        schema_name,
        is_distributed
    FROM
        pg_class c
        INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
        INNER JOIN _timescaledb_catalog.hypertable ht ON (ht.schema_name = n.nspname
                AND ht.table_name = c.relname)
    WHERE
        c.OID = hypertable;

    IF table_name IS NULL THEN
	    RETURN;
	END IF;

    CASE WHEN is_distributed THEN
        RETURN QUERY
        SELECT
            *
        FROM
            _timescaledb_internal.compressed_chunk_remote_stats (schema_name, table_name);
    ELSE
        RETURN QUERY
        SELECT
            *,
            NULL::name
        FROM
            _timescaledb_internal.compressed_chunk_local_stats (schema_name, table_name);
    END CASE;
END;
$BODY$;

CREATE FUNCTION _timescaledb_internal.chunks_local_size(
    schema_name_in name,
    table_name_in name)
RETURNS TABLE (
    chunk_id    integer,
    chunk_schema NAME,
    chunk_name  NAME,
    table_bytes bigint,
    index_bytes bigint,
    toast_bytes bigint,
    total_bytes bigint)
LANGUAGE SQL VOLATILE STRICT AS
$BODY$
   SELECT
      ch.chunk_id,
      ch.chunk_schema,
      ch.chunk_name,
      (ch.total_bytes - COALESCE( ch.index_bytes , 0 ) - COALESCE( ch.toast_bytes, 0 ) + COALESCE( ch.compressed_heap_size , 0 ))::bigint  as heap_bytes,
      (COALESCE( ch.index_bytes, 0 ) + COALESCE( ch.compressed_index_size , 0) )::bigint as index_bytes,
      (COALESCE( ch.toast_bytes, 0 ) + COALESCE( ch.compressed_toast_size, 0 ))::bigint as toast_bytes,
      (ch.total_bytes + COALESCE( ch.compressed_total_size, 0 ))::bigint as total_bytes
   FROM
	  _timescaledb_internal.hypertable_chunk_local_size ch
   WHERE
      ch.hypertable_schema = schema_name_in
      AND ch.hypertable_name = table_name_in;
$BODY$;

---should return same information as chunks_local_size--
CREATE FUNCTION _timescaledb_internal.chunks_remote_size(
    schema_name_in name,
    table_name_in name)
RETURNS TABLE (
    chunk_id    integer,
    chunk_schema NAME,
    chunk_name  NAME,
    table_bytes bigint,
    index_bytes bigint,
    toast_bytes bigint,
    total_bytes bigint,
    node_name NAME)
LANGUAGE SQL VOLATILE STRICT AS
$BODY$
    SELECT
        entry.chunk_id,
        entry.chunk_schema,
        entry.chunk_name,
        entry.table_bytes AS table_bytes,
        entry.index_bytes AS index_bytes,
        entry.toast_bytes AS toast_bytes,
        entry.total_bytes AS total_bytes,
        srv.node_name
    FROM (
        SELECT
            s.node_name
        FROM
            _timescaledb_catalog.hypertable AS ht,
            _timescaledb_catalog.hypertable_data_node AS s
        WHERE
            ht.schema_name = schema_name_in
            AND ht.table_name = table_name_in
            AND s.hypertable_id = ht.id
         ) AS srv
    LEFT OUTER JOIN LATERAL _timescaledb_internal.data_node_chunk_info(
        srv.node_name, schema_name_in, table_name_in) entry ON TRUE
	WHERE
	    entry.chunk_name IS NOT NULL;
$BODY$;

CREATE FUNCTION @extschema@.chunks_detailed_size(
    hypertable              REGCLASS
)
RETURNS TABLE (
               chunk_schema NAME,
               chunk_name NAME,
               table_bytes BIGINT,
               index_bytes BIGINT,
               toast_bytes BIGINT,
               total_bytes BIGINT,
               node_name   NAME)
LANGUAGE PLPGSQL VOLATILE STRICT AS
$BODY$
DECLARE
        table_name       NAME;
        schema_name      NAME;
        is_distributed   BOOL;
BEGIN
        SELECT relname, nspname, replication_factor > 0
        INTO table_name, schema_name, is_distributed
        FROM pg_class c
        INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
        INNER JOIN _timescaledb_catalog.hypertable ht ON (ht.schema_name = n.nspname AND ht.table_name = c.relname)
        WHERE c.OID = hypertable;

		IF table_name IS NULL THEN
		    RETURN;
		END IF;

        CASE WHEN is_distributed THEN
            RETURN QUERY SELECT ch.chunk_schema, ch.chunk_name, ch.table_bytes, ch.index_bytes,
                        ch.toast_bytes, ch.total_bytes, ch.node_name
            FROM _timescaledb_internal.chunks_remote_size(schema_name, table_name) ch;
        ELSE
            RETURN QUERY SELECT chl.chunk_schema, chl.chunk_name, chl.table_bytes, chl.index_bytes,
                        chl.toast_bytes, chl.total_bytes, NULL::NAME
            FROM _timescaledb_internal.chunks_local_size(schema_name, table_name) chl;
        END CASE;
END;
$BODY$;

CREATE FUNCTION @extschema@.hypertable_compression_stats (hypertable REGCLASS)
    RETURNS TABLE (
        total_chunks bigint,
        number_compressed_chunks bigint,
        before_compression_table_bytes bigint,
        before_compression_index_bytes bigint,
        before_compression_toast_bytes bigint,
        before_compression_total_bytes bigint,
        after_compression_table_bytes bigint,
        after_compression_index_bytes bigint,
        after_compression_toast_bytes bigint,
        after_compression_total_bytes bigint,
        node_name name)
    LANGUAGE SQL
    STABLE STRICT
    AS
$BODY$
	SELECT
        count(*)::bigint AS total_chunks,
        (count(*) FILTER (WHERE ch.compression_status = 'Compressed'))::bigint AS number_compressed_chunks,
        sum(ch.before_compression_table_bytes)::bigint AS before_compression_table_bytes,
        sum(ch.before_compression_index_bytes)::bigint AS before_compression_index_bytes,
        sum(ch.before_compression_toast_bytes)::bigint AS before_compression_toast_bytes,
        sum(ch.before_compression_total_bytes)::bigint AS before_compression_total_bytes,
        sum(ch.after_compression_table_bytes)::bigint AS after_compression_table_bytes,
        sum(ch.after_compression_index_bytes)::bigint AS after_compression_index_bytes,
        sum(ch.after_compression_toast_bytes)::bigint AS after_compression_toast_bytes,
        sum(ch.after_compression_total_bytes)::bigint AS after_compression_total_bytes,
        ch.node_name
    FROM
	    chunk_compression_stats(hypertable) ch
    GROUP BY
        ch.node_name;
$BODY$;

CREATE FUNCTION _timescaledb_internal.hypertable_local_size(
	schema_name_in name,
	table_name_in name)
RETURNS TABLE (
	table_bytes bigint,
	index_bytes bigint,
	toast_bytes bigint,
	total_bytes bigint)
LANGUAGE SQL VOLATILE STRICT AS
$BODY$
	SELECT
		(COALESCE(sum(ch.total_bytes), 0) - COALESCE(sum(ch.index_bytes), 0) - COALESCE(sum(ch.toast_bytes), 0) + COALESCE(sum(ch.compressed_heap_size), 0))::bigint + pg_relation_size(format('%I.%I', schema_name_in, table_name_in)::regclass)::bigint AS heap_bytes,
		(COALESCE(sum(ch.index_bytes), 0) + COALESCE(sum(ch.compressed_index_size), 0))::bigint + pg_indexes_size(format('%I.%I', schema_name_in, table_name_in)::regclass)::bigint AS index_bytes,
		(COALESCE(sum(ch.toast_bytes), 0) + COALESCE(sum(ch.compressed_toast_size), 0))::bigint AS toast_bytes,
		(COALESCE(sum(ch.total_bytes), 0) + COALESCE(sum(ch.compressed_total_size), 0))::bigint + pg_total_relation_size(format('%I.%I', schema_name_in, table_name_in)::regclass)::bigint AS total_bytes
	FROM
		_timescaledb_internal.hypertable_chunk_local_size ch
	WHERE
		hypertable_schema = schema_name_in
		AND hypertable_name = table_name_in
$BODY$;

CREATE FUNCTION _timescaledb_internal.hypertable_remote_size(
    schema_name_in name,
    table_name_in name)
RETURNS TABLE (
    table_bytes bigint,
    index_bytes bigint,
    toast_bytes bigint,
    total_bytes bigint,
    node_name   NAME)
LANGUAGE SQL VOLATILE STRICT AS
$BODY$
    SELECT
        sum(entry.table_bytes)::bigint AS table_bytes,
        sum(entry.index_bytes)::bigint AS index_bytes,
        sum(entry.toast_bytes)::bigint AS toast_bytes,
        sum(entry.total_bytes)::bigint AS total_bytes,
        srv.node_name
    FROM (
        SELECT
            s.node_name
        FROM
            _timescaledb_catalog.hypertable AS ht,
            _timescaledb_catalog.hypertable_data_node AS s
        WHERE
            ht.schema_name = schema_name_in
            AND ht.table_name = table_name_in
            AND s.hypertable_id = ht.id
         ) AS srv
    LEFT OUTER JOIN LATERAL _timescaledb_internal.data_node_hypertable_info(
        srv.node_name, schema_name_in, table_name_in) entry ON TRUE
    GROUP BY srv.node_name;
$BODY$;

CREATE FUNCTION @extschema@.hypertable_detailed_size(
    hypertable              REGCLASS)
RETURNS TABLE (table_bytes BIGINT,
               index_bytes BIGINT,
               toast_bytes BIGINT,
               total_bytes BIGINT,
               node_name   NAME)
LANGUAGE PLPGSQL VOLATILE STRICT AS
$BODY$
DECLARE
        table_name       NAME = NULL;
        schema_name      NAME = NULL;
        is_distributed   BOOL = FALSE;
BEGIN
        SELECT relname, nspname, replication_factor > 0
        INTO table_name, schema_name, is_distributed
        FROM pg_class c
        INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
        INNER JOIN _timescaledb_catalog.hypertable ht ON (ht.schema_name = n.nspname AND ht.table_name = c.relname)
        WHERE c.OID = hypertable;

		IF table_name IS NULL THEN
		    RETURN;
		END IF;

        CASE WHEN is_distributed THEN
			RETURN QUERY
			SELECT *, NULL::name
			FROM _timescaledb_internal.hypertable_local_size(schema_name, table_name)
			UNION
			SELECT *
			FROM _timescaledb_internal.hypertable_remote_size(schema_name, table_name);
        ELSE
			RETURN QUERY
			SELECT *, NULL::name
			FROM _timescaledb_internal.hypertable_local_size(schema_name, table_name);
        END CASE;
END;
$BODY$;

CREATE FUNCTION @extschema@.hypertable_size(
    hypertable              REGCLASS)
RETURNS BIGINT
LANGUAGE SQL VOLATILE STRICT AS
$BODY$
   -- One row per data node is returned (in case of a distributed
   -- hypertable), so sum them up:
   SELECT sum(total_bytes)::bigint
   FROM @extschema@.hypertable_detailed_size(hypertable);
$BODY$;

CREATE FUNCTION _timescaledb_internal.indexes_remote_size(
    schema_name_in             NAME,
    table_name_in              NAME,
    index_name_in              NAME
)
RETURNS BIGINT
LANGUAGE SQL VOLATILE STRICT AS
$BODY$
    SELECT
        sum(entry.total_bytes)::bigint AS total_bytes
    FROM (
        SELECT
            s.node_name
        FROM
            _timescaledb_catalog.hypertable AS ht,
            _timescaledb_catalog.hypertable_data_node AS s
        WHERE
            ht.schema_name = schema_name_in
            AND ht.table_name = table_name_in
            AND s.hypertable_id = ht.id
         ) AS srv
    JOIN LATERAL _timescaledb_internal.data_node_index_size(
        srv.node_name, schema_name_in, index_name_in) entry ON TRUE;
$BODY$;

CREATE FUNCTION @extschema@.hypertable_index_size(
    index_name              REGCLASS
)
RETURNS BIGINT
LANGUAGE PLPGSQL VOLATILE STRICT AS
$BODY$
DECLARE
        ht_index_name       NAME;
        ht_schema_name      NAME;
        ht_name      NAME;
        is_distributed   BOOL;
        ht_id INTEGER;
        index_bytes BIGINT;
BEGIN
   SELECT c.relname, cl.relname, nsp.nspname, ht.replication_factor > 0
   INTO ht_index_name, ht_name, ht_schema_name, is_distributed
   FROM pg_class c, pg_index cind, pg_class cl,
        pg_namespace nsp, _timescaledb_catalog.hypertable ht
   WHERE c.oid = cind.indexrelid AND cind.indrelid = cl.oid
         AND cl.relnamespace = nsp.oid AND c.oid = index_name
		 AND ht.schema_name = nsp.nspname ANd ht.table_name = cl.relname;

   IF ht_index_name IS NULL THEN
       RETURN NULL;
   END IF;

   -- get the local size or size of access node indexes
   SELECT il.total_bytes
   INTO index_bytes
   FROM _timescaledb_internal.indexes_local_size(ht_schema_name, ht_index_name) il;

   IF index_bytes IS NULL THEN
       index_bytes = 0;
   END IF;

   -- Add size from data nodes
   IF is_distributed THEN
       index_bytes = index_bytes + _timescaledb_internal.indexes_remote_size(ht_schema_name, ht_name, ht_index_name);
   END IF;

   RETURN index_bytes;
END;
$BODY$;
