DROP VIEW IF EXISTS timescaledb_information.continuous_aggregates;
DROP VIEW IF EXISTS timescaledb_information.job_stats;
DROP FUNCTION IF EXISTS _timescaledb_internal.get_git_commit;

-- Begin Modify hypertable table
-- we make a copy of the data, remove dependencies, drop the table
DROP VIEW IF EXISTS timescaledb_information.hypertables;
DROP VIEW IF EXISTS timescaledb_information.chunks;
DROP VIEW IF EXISTS timescaledb_information.dimensions;
DROP VIEW IF EXISTS timescaledb_information.jobs;
DROP VIEW IF EXISTS timescaledb_information.compression_settings;
DROP VIEW IF EXISTS _timescaledb_internal.compressed_chunk_stats;
DROP VIEW IF EXISTS _timescaledb_internal.hypertable_chunk_local_size;
DROP FUNCTION IF EXISTS _timescaledb_internal.hypertable_from_main_table;

CREATE TABLE _timescaledb_internal.hypertable_tmp
AS SELECT * from _timescaledb_catalog.hypertable;

--drop foreign keys on hypertable
ALTER TABLE _timescaledb_catalog.hypertable DROP CONSTRAINT hypertable_compressed_hypertable_id_fkey;
ALTER TABLE _timescaledb_catalog.hypertable_data_node DROP CONSTRAINT hypertable_data_node_hypertable_id_fkey;
ALTER TABLE _timescaledb_catalog.tablespace DROP CONSTRAINT tablespace_hypertable_id_fkey;
ALTER TABLE _timescaledb_catalog.dimension DROP CONSTRAINT dimension_hypertable_id_fkey;
ALTER TABLE _timescaledb_catalog.chunk DROP CONSTRAINT chunk_hypertable_id_fkey;
ALTER TABLE _timescaledb_catalog.chunk_index DROP CONSTRAINT chunk_index_hypertable_id_fkey;
ALTER TABLE _timescaledb_config.bgw_job DROP CONSTRAINT bgw_job_hypertable_id_fkey;
ALTER TABLE _timescaledb_catalog.continuous_agg DROP CONSTRAINT continuous_agg_mat_hypertable_id_fkey;
ALTER TABLE _timescaledb_catalog.continuous_agg DROP CONSTRAINT continuous_agg_raw_hypertable_id_fkey;
ALTER TABLE _timescaledb_catalog.continuous_aggs_invalidation_threshold DROP CONSTRAINT continuous_aggs_invalidation_threshold_hypertable_id_fkey;
ALTER TABLE _timescaledb_catalog.hypertable_compression DROP CONSTRAINT hypertable_compression_hypertable_id_fkey;

CREATE TABLE _timescaledb_internal.tmp_hypertable_seq_value AS
SELECT last_value, is_called FROM _timescaledb_catalog.hypertable_id_seq;
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.hypertable;
ALTER EXTENSION timescaledb DROP SEQUENCE _timescaledb_catalog.hypertable_id_seq;
DROP TABLE _timescaledb_catalog.hypertable;

CREATE SEQUENCE _timescaledb_catalog.hypertable_id_seq MINVALUE 1;

-- now create table without self referential foreign key
CREATE TABLE _timescaledb_catalog.hypertable(
  id INTEGER PRIMARY KEY DEFAULT nextval('_timescaledb_catalog.hypertable_id_seq'),
  schema_name name NOT NULL CHECK (schema_name != '_timescaledb_catalog'),
  table_name name NOT NULL,
  associated_schema_name name NOT NULL,
  associated_table_prefix name NOT NULL,
  num_dimensions smallint NOT NULL,
  chunk_sizing_func_schema name NOT NULL,
  chunk_sizing_func_name name NOT NULL,
  chunk_target_size bigint NOT NULL CHECK (chunk_target_size >= 0), -- size in bytes
  compression_state smallint NOT NULL DEFAULT 0,
  compressed_hypertable_id integer ,
  replication_factor smallint NULL,
  UNIQUE (associated_schema_name, associated_table_prefix),
  CONSTRAINT hypertable_table_name_schema_name_key UNIQUE (table_name, schema_name),
  -- internal compressed hypertables have compression state = 2
  CONSTRAINT hypertable_dim_compress_check CHECK (num_dimensions > 0 OR compression_state = 2),
  CONSTRAINT hypertable_compress_check CHECK ( (compression_state = 0 OR compression_state = 1 )  OR (compression_state = 2 AND compressed_hypertable_id IS NULL)),
  -- replication_factor NULL: regular hypertable
  -- replication_factor > 0: distributed hypertable on access node
  -- replication_factor -1: distributed hypertable on data node, which is part of a larger table
  CONSTRAINT hypertable_replication_factor_check CHECK (replication_factor > 0 OR replication_factor = -1)
);
ALTER SEQUENCE _timescaledb_catalog.hypertable_id_seq OWNED BY _timescaledb_catalog.hypertable.id;
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.hypertable_id_seq', '');
SELECT setval('_timescaledb_catalog.hypertable_id_seq', last_value, is_called) FROM _timescaledb_internal.tmp_hypertable_seq_value;

INSERT INTO _timescaledb_catalog.hypertable
( id, schema_name, table_name, associated_schema_name, associated_table_prefix,
       num_dimensions, chunk_sizing_func_schema, chunk_sizing_func_name,
       chunk_target_size, compression_state, compressed_hypertable_id,
       replication_factor)
SELECT id, schema_name, table_name, associated_schema_name, associated_table_prefix,
       num_dimensions, chunk_sizing_func_schema, chunk_sizing_func_name,
       chunk_target_size,
       CASE WHEN compressed is FALSE  AND compressed_hypertable_id IS NOT NULL THEN 1
            WHEN compressed is TRUE THEN 2
            ELSE 0
       END,
       compressed_hypertable_id,
       replication_factor
FROM _timescaledb_internal.hypertable_tmp;

-- add self referential foreign key
ALTER TABLE _timescaledb_catalog.hypertable ADD CONSTRAINT hypertable_compressed_hypertable_id_fkey FOREIGN KEY ( compressed_hypertable_id )
 REFERENCES _timescaledb_catalog.hypertable( id );
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.hypertable', '');
--cleanup
DROP TABLE _timescaledb_internal.hypertable_tmp;
DROP TABLE _timescaledb_internal.tmp_hypertable_seq_value;

-- add all the other foreign keys
ALTER TABLE _timescaledb_catalog.hypertable_data_node
ADD CONSTRAINT hypertable_data_node_hypertable_id_fkey
FOREIGN KEY ( hypertable_id ) REFERENCES _timescaledb_catalog.hypertable( id );
ALTER TABLE _timescaledb_catalog.tablespace ADD CONSTRAINT tablespace_hypertable_id_fkey
FOREIGN KEY ( hypertable_id ) REFERENCES _timescaledb_catalog.hypertable( id )
ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.dimension ADD CONSTRAINT dimension_hypertable_id_fkey
FOREIGN KEY ( hypertable_id ) REFERENCES _timescaledb_catalog.hypertable( id )
ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.chunk ADD CONSTRAINT chunk_hypertable_id_fkey
FOREIGN KEY ( hypertable_id ) REFERENCES _timescaledb_catalog.hypertable( id );
ALTER TABLE _timescaledb_catalog.chunk_index ADD CONSTRAINT chunk_index_hypertable_id_fkey
FOREIGN KEY ( hypertable_id ) REFERENCES _timescaledb_catalog.hypertable( id )
ON DELETE CASCADE;
ALTER TABLE _timescaledb_config.bgw_job ADD CONSTRAINT bgw_job_hypertable_id_fkey
FOREIGN KEY ( hypertable_id ) REFERENCES _timescaledb_catalog.hypertable( id )
ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.continuous_agg ADD CONSTRAINT
continuous_agg_mat_hypertable_id_fkey FOREIGN KEY ( mat_hypertable_id )
REFERENCES _timescaledb_catalog.hypertable( id )
ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.continuous_agg ADD CONSTRAINT
continuous_agg_raw_hypertable_id_fkey FOREIGN KEY ( raw_hypertable_id )
REFERENCES _timescaledb_catalog.hypertable( id )
ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.continuous_aggs_invalidation_threshold
ADD CONSTRAINT continuous_aggs_invalidation_threshold_hypertable_id_fkey
FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable( id )
ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.hypertable_compression ADD CONSTRAINT
hypertable_compression_hypertable_id_fkey FOREIGN KEY ( hypertable_id )
REFERENCES _timescaledb_catalog.hypertable( id )
ON DELETE CASCADE;

GRANT SELECT ON _timescaledb_catalog.hypertable_id_seq TO PUBLIC;
GRANT SELECT ON _timescaledb_catalog.hypertable TO PUBLIC;
--End Modify hypertable table
