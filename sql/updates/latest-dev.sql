CREATE OR REPLACE FUNCTION _timescaledb_internal.time_to_internal(time_val ANYELEMENT)
RETURNS BIGINT AS '@MODULE_PATHNAME@', 'ts_time_to_internal' LANGUAGE C VOLATILE STRICT;

-- Recreate missing dimension slices that might be missing due to a
-- bug that is fixed in this release. If the dimension slice table is
-- broken and there are dimension slices missing from the table, we
-- will repair it by:
--
--    1. Finding all chunk constraints that have missing dimension
--       slices and extract the constraint expression from the
--       associated constraint.
--       
--    2. Parse the constraint expression and extract the column name,
--       and upper and lower range values as text.
--       
--    3. Use the column type to construct the range values (UNIX
--       microseconds) from these values.
INSERT INTO _timescaledb_catalog.dimension_slice
WITH
   -- All dimension slices that are mentioned in the chunk_constraint
   -- table but are missing from the dimension_slice table.
   missing_slices AS (
      SELECT hypertable_id,
      	     chunk_id,
	     dimension_slice_id,
	     constraint_name,
	     attname AS column_name,
	     pg_get_expr(conbin, conrelid) AS constraint_expr
      FROM _timescaledb_catalog.chunk_constraint cc
      JOIN _timescaledb_catalog.chunk ch ON cc.chunk_id = ch.id
      JOIN pg_constraint ON conname = constraint_name
      JOIN pg_namespace ns ON connamespace = ns.oid AND ns.nspname = ch.schema_name
      JOIN pg_attribute ON attnum = conkey[1] AND attrelid = conrelid
      WHERE
	 dimension_slice_id NOT IN (SELECT id FROM _timescaledb_catalog.dimension_slice)
   ),

  -- Unparsed range start and end for each dimension slice id that
  -- is missing.
   unparsed_missing_slices AS (
      SELECT di.id AS dimension_id,
      	     dimension_slice_id,
             constraint_name,
	     column_type,
	     column_name,
	     (SELECT SUBSTRING(constraint_expr, $$>=\s*'?([\w\d\s:+-]+)'?$$)) AS range_start,
	     (SELECT SUBSTRING(constraint_expr, $$<\s*'?([\w\d\s:+-]+)'?$$)) AS range_end
	FROM missing_slices JOIN _timescaledb_catalog.dimension di USING (hypertable_id, column_name)
   )
SELECT DISTINCT
       dimension_slice_id,
       dimension_id,
       CASE
       WHEN column_type IN ('smallint'::regtype, 'bigint'::regtype, 'integer'::regtype) THEN
       	    CASE
	    WHEN range_start IS NULL
	    THEN -9223372036854775808
	    ELSE _timescaledb_internal.time_to_internal(range_start::bigint)
	    END
       WHEN column_type = 'timestamptz'::regtype THEN
       	    _timescaledb_internal.time_to_internal(range_start::timestamptz)
       WHEN column_type = 'timestamp'::regtype THEN
       	    _timescaledb_internal.time_to_internal(range_start::timestamp)
       WHEN column_type = 'date'::regtype THEN
       	    _timescaledb_internal.time_to_internal(range_start::date)
       ELSE
	    NULL
       END AS range_start,
       CASE 
       WHEN column_type IN ('smallint'::regtype, 'bigint'::regtype, 'integer'::regtype) THEN
       	    CASE WHEN range_end IS NULL
	    THEN 9223372036854775807
	    ELSE _timescaledb_internal.time_to_internal(range_end::bigint)
	    END
       WHEN column_type = 'timestamptz'::regtype THEN
       	    _timescaledb_internal.time_to_internal(range_end::timestamptz)
       WHEN column_type = 'timestamp'::regtype THEN
       	    _timescaledb_internal.time_to_internal(range_end::timestamp)
       WHEN column_type = 'date'::regtype THEN
       	    _timescaledb_internal.time_to_internal(range_end::date)
       ELSE NULL
       END AS range_end
  FROM unparsed_missing_slices;

-- Add new function definitions, columns and tables for distributed hypertables
DROP FUNCTION IF EXISTS create_hypertable(regclass,name,name,integer,name,name,anyelement,boolean,boolean,regproc,boolean,text,regproc,regproc);
DROP FUNCTION IF EXISTS add_drop_chunks_policy;
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

