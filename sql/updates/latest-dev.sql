--Drop functions in size_utils and dependencies, ordering matters.
-- Do not reorder
DROP VIEW timescaledb_information.hypertable;
DROP FUNCTION hypertable_relation_size_pretty;
DROP FUNCTION  hypertable_relation_size;
DROP FUNCTION indexes_relation_size_pretty;
DROP FUNCTION  indexes_relation_size;
DROP FUNCTION _timescaledb_internal.partitioning_column_to_pretty;
-- end of do not reorder

-- Add new function definitions, columns and tables for distributed hypertables
DROP FUNCTION IF EXISTS create_hypertable(regclass,name,name,integer,name,name,anyelement,boolean,boolean,regproc,boolean,text,regproc,regproc);
DROP FUNCTION IF EXISTS add_drop_chunks_policy;
DROP FUNCTION IF EXISTS remove_drop_chunks_policy;
DROP FUNCTION IF EXISTS drop_chunks;

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

-- Update drop_chunks policy table

--since we are recreating the bgw_policy_drop_chunks table, it has to be explicitly
-- removed from the extension as it was configured to be dumped by pg_extension_config_dump.
-- So remove it from the extension first and then recreate a new table.

CREATE TABLE _timescaledb_config.bgw_policy_drop_chunks_tmp (
    job_id INTEGER PRIMARY KEY
    	   REFERENCES _timescaledb_config.bgw_job(id)
	   ON DELETE CASCADE,
    hypertable_id INTEGER UNIQUE NOT NULL
    		  REFERENCES _timescaledb_catalog.hypertable(id)
		  ON DELETE CASCADE,
    older_than _timescaledb_catalog.ts_interval NOT NULL,
    cascade_to_materializations BOOLEAN,
    CONSTRAINT valid_older_than CHECK(_timescaledb_internal.valid_ts_interval(older_than))
);

INSERT INTO _timescaledb_config.bgw_policy_drop_chunks_tmp (
    SELECT job_id,
    	   hypertable_id,
	   older_than,
	   cascade_to_materializations
      FROM _timescaledb_config.bgw_policy_drop_chunks
);

ALTER EXTENSION timescaledb DROP TABLE _timescaledb_config.bgw_policy_drop_chunks;
DROP VIEW IF EXISTS timescaledb_information.policy_stats;
DROP VIEW IF EXISTS timescaledb_information.drop_chunks_policies;
DROP TABLE IF EXISTS _timescaledb_config.bgw_policy_drop_chunks;

CREATE TABLE _timescaledb_config.bgw_policy_drop_chunks (
    job_id INTEGER PRIMARY KEY
    	   REFERENCES _timescaledb_config.bgw_job(id)
	   ON DELETE CASCADE,
    hypertable_id INTEGER UNIQUE NOT NULL
    		  REFERENCES _timescaledb_catalog.hypertable(id)
		  ON DELETE CASCADE,
    older_than _timescaledb_catalog.ts_interval NOT NULL,
    cascade_to_materializations BOOLEAN,
    CONSTRAINT valid_older_than CHECK(_timescaledb_internal.valid_ts_interval(older_than))
);
INSERT INTO _timescaledb_config.bgw_policy_drop_chunks (
       SELECT * FROM _timescaledb_config.bgw_policy_drop_chunks_tmp
);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_config.bgw_policy_drop_chunks', '');
DROP TABLE _timescaledb_config.bgw_policy_drop_chunks_tmp;
GRANT SELECT ON _timescaledb_config.bgw_policy_drop_chunks TO PUBLIC;

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
ALTER TABLE _timescaledb_config.bgw_job ADD COLUMN proc_name NAME NOT NULL DEFAULT '';
ALTER TABLE _timescaledb_config.bgw_job ADD COLUMN proc_schema NAME NOT NULL DEFAULT '';
ALTER TABLE _timescaledb_config.bgw_job ADD COLUMN owner NAME NOT NULL DEFAULT CURRENT_ROLE;
ALTER TABLE _timescaledb_config.bgw_job ADD COLUMN scheduled BOOL NOT NULL DEFAULT true;
ALTER TABLE _timescaledb_config.bgw_job ADD COLUMN hypertable_id INTEGER REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_config.bgw_job ADD COLUMN config JSONB;

ALTER TABLE _timescaledb_config.bgw_job DROP CONSTRAINT valid_job_type;
ALTER TABLE _timescaledb_config.bgw_job ADD CONSTRAINT valid_job_type CHECK (job_type IN ('telemetry_and_version_check_if_enabled', 'reorder', 'drop_chunks', 'continuous_aggregate', 'compress_chunks', 'custom'));

-- migrate reorder jobs
UPDATE
  _timescaledb_config.bgw_job job
SET proc_name = 'policy_reorder',
  proc_schema = '_timescaledb_internal',
  config = jsonb_build_object('hypertable_id', reorder.hypertable_id, 'index_name', reorder.hypertable_index_name),
  hypertable_id = reorder.hypertable_id,
  OWNER = (
    SELECT relowner::regrole::text
    FROM _timescaledb_catalog.hypertable ht,
      pg_class cl
    WHERE ht.id = reorder.hypertable_id
      AND cl.oid = format('%I.%I', schema_name, table_name)::regclass)
FROM _timescaledb_config.bgw_policy_reorder reorder
WHERE job_type = 'reorder'
  AND job.id = reorder.job_id;

--rewrite catalog table to not break catalog scans on tables with missingval optimization
CLUSTER  _timescaledb_config.bgw_job USING bgw_job_pkey;
ALTER TABLE _timescaledb_config.bgw_job SET WITHOUT CLUSTER;

CREATE INDEX IF NOT EXISTS bgw_job_proc_hypertable_id_idx ON _timescaledb_config.bgw_job(proc_name,proc_schema,hypertable_id);

ALTER EXTENSION timescaledb DROP TABLE _timescaledb_config.bgw_policy_reorder;
DROP TABLE _timescaledb_config.bgw_policy_reorder CASCADE;

