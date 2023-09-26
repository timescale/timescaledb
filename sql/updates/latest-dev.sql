CREATE TYPE _timescaledb_internal.dimension_info;

CREATE OR REPLACE FUNCTION _timescaledb_functions.dimension_info_in(cstring)
    RETURNS _timescaledb_internal.dimension_info
    LANGUAGE C STRICT IMMUTABLE
    AS '@MODULE_PATHNAME@', 'ts_dimension_info_in';

CREATE OR REPLACE FUNCTION _timescaledb_functions.dimension_info_out(_timescaledb_internal.dimension_info)
    RETURNS cstring
    LANGUAGE C STRICT IMMUTABLE
    AS '@MODULE_PATHNAME@', 'ts_dimension_info_out';

CREATE TYPE _timescaledb_internal.dimension_info (
    INPUT = _timescaledb_functions.dimension_info_in,
    OUTPUT = _timescaledb_functions.dimension_info_out,
    INTERNALLENGTH = VARIABLE
);

CREATE FUNCTION @extschema@.create_hypertable(
    relation                REGCLASS,
    dimension               _timescaledb_internal.dimension_info,
    create_default_indexes  BOOLEAN = TRUE,
    if_not_exists           BOOLEAN = FALSE,
    migrate_data            BOOLEAN = FALSE
) RETURNS TABLE(hypertable_id INT, created BOOL) AS '@MODULE_PATHNAME@', 'ts_hypertable_create_general' LANGUAGE C VOLATILE;

CREATE FUNCTION @extschema@.add_dimension(
    hypertable              REGCLASS,
    dimension               _timescaledb_internal.dimension_info,
    if_not_exists           BOOLEAN = FALSE
) RETURNS TABLE(dimension_id INT, created BOOL)
AS '@MODULE_PATHNAME@', 'ts_dimension_add_general' LANGUAGE C VOLATILE;

CREATE FUNCTION @extschema@.set_partitioning_interval(
    hypertable              REGCLASS,
    partition_interval      ANYELEMENT,
    dimension_name          NAME = NULL
) RETURNS VOID AS '@MODULE_PATHNAME@', 'ts_dimension_set_interval' LANGUAGE C VOLATILE;

CREATE FUNCTION @extschema@.by_hash(column_name NAME, number_partitions INTEGER,
                                    partition_func regproc = NULL)
    RETURNS _timescaledb_internal.dimension_info LANGUAGE C
    AS '@MODULE_PATHNAME@', 'ts_hash_dimension';

CREATE FUNCTION @extschema@.by_range(column_name NAME,
                                     partition_interval ANYELEMENT = NULL::bigint,
                                     partition_func regproc = NULL)
    RETURNS _timescaledb_internal.dimension_info LANGUAGE C
    AS '@MODULE_PATHNAME@', 'ts_range_dimension';

--
-- Rebuild the catalog table `_timescaledb_catalog.chunk` to
-- add new column `creation_time`
--
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
  osm_chunk boolean NOT NULL DEFAULT FALSE,
  creation_time timestamptz,
  -- table constraints
  CONSTRAINT chunk_pkey PRIMARY KEY (id),
  CONSTRAINT chunk_schema_name_table_name_key UNIQUE (schema_name, table_name)
);

INSERT INTO _timescaledb_catalog.chunk
( id, hypertable_id, schema_name, table_name,
  compressed_chunk_id, dropped, status, osm_chunk)
SELECT id, hypertable_id, schema_name, table_name,
  compressed_chunk_id, dropped, status, osm_chunk
FROM _timescaledb_internal.chunk_tmp;

-- update creation_time for chunks
UPDATE
    _timescaledb_catalog.chunk c
SET
    creation_time = (pg_catalog.pg_stat_file(pg_catalog.pg_relation_filepath(r.oid))).modification
FROM
    pg_class r, pg_namespace n
WHERE
    r.relnamespace = n.oid
    AND r.relname = c.table_name
    AND n.nspname = c.schema_name
    AND r.relkind = 'r'
    AND c.dropped IS FALSE;

-- Make sure that there are no record with empty creation time
UPDATE _timescaledb_catalog.chunk SET creation_time = now() WHERE creation_time IS NULL;

--add indexes to the chunk table
CREATE INDEX chunk_hypertable_id_idx ON _timescaledb_catalog.chunk (hypertable_id);
CREATE INDEX chunk_compressed_chunk_id_idx ON _timescaledb_catalog.chunk (compressed_chunk_id);
CREATE INDEX chunk_osm_chunk_idx ON _timescaledb_catalog.chunk (osm_chunk, hypertable_id);
CREATE INDEX chunk_hypertable_id_creation_time_idx ON _timescaledb_catalog.chunk(hypertable_id, creation_time);

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

-- Add non-null constraint
ALTER TABLE _timescaledb_catalog.chunk
  ALTER COLUMN creation_time SET NOT NULL;

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

CREATE FUNCTION _timescaledb_functions.set_policy_scheduled(hypertable REGCLASS, policy_type TEXT, scheduled BOOL)
RETURNS INTEGER
AS $$
    WITH affected_policies AS (
        SELECT @extschema@.alter_job(j.id, scheduled => set_policy_scheduled.scheduled)
        FROM _timescaledb_config.bgw_job j
            JOIN _timescaledb_catalog.hypertable h ON h.id = j.hypertable_id
        WHERE j.proc_schema IN ('_timescaledb_internal', '_timescaledb_functions')
        AND j.proc_name = set_policy_scheduled.policy_type
        AND j.id >= 1000
        AND scheduled <> set_policy_scheduled.scheduled
        AND format('%I.%I', h.schema_name, h.table_name) = set_policy_scheduled.hypertable::text
    )
SELECT count(*) FROM affected_policies;
$$
LANGUAGE SQL SET search_path TO pg_catalog, pg_temp;

CREATE FUNCTION _timescaledb_functions.set_all_policy_scheduled(hypertable REGCLASS, scheduled BOOL)
RETURNS INTEGER
AS $$
    WITH affected_policies AS (
        SELECT @extschema@.alter_job(j.id, scheduled => set_all_policy_scheduled.scheduled)
        FROM _timescaledb_config.bgw_job j
            JOIN _timescaledb_catalog.hypertable h ON h.id = j.hypertable_id
        WHERE j.proc_schema IN ('_timescaledb_internal', '_timescaledb_functions')
        AND j.id >= 1000
        AND scheduled <> set_all_policy_scheduled.scheduled
        AND format('%I.%I', h.schema_name, h.table_name) = set_all_policy_scheduled.hypertable::text
    )
SELECT count(*) FROM affected_policies;
$$
LANGUAGE SQL SET search_path TO pg_catalog, pg_temp;

CREATE FUNCTION @extschema@.disable_all_policies(hypertable REGCLASS)
RETURNS INTEGER
AS $$
SELECT _timescaledb_functions.set_all_policy_scheduled(hypertable, false);
$$
LANGUAGE SQL SET search_path TO pg_catalog, pg_temp;

CREATE FUNCTION @extschema@.enable_all_policies(hypertable REGCLASS)
RETURNS INTEGER
AS $$
SELECT _timescaledb_functions.set_all_policy_scheduled(hypertable, true);
$$
LANGUAGE SQL SET search_path TO pg_catalog, pg_temp;

CREATE FUNCTION @extschema@.disable_compression_policy(hypertable REGCLASS)
RETURNS INTEGER
AS $$
SELECT _timescaledb_functions.set_policy_scheduled(hypertable, 'policy_compression', false);
$$
LANGUAGE SQL SET search_path TO pg_catalog, pg_temp;

CREATE FUNCTION @extschema@.enable_compression_policy(hypertable REGCLASS)
RETURNS INTEGER
AS $$
SELECT _timescaledb_functions.set_policy_scheduled(hypertable, 'policy_compression', true);
$$
LANGUAGE SQL SET search_path TO pg_catalog, pg_temp;

CREATE FUNCTION @extschema@.disable_reorder_policy(hypertable REGCLASS)
RETURNS INTEGER
AS $$
SELECT _timescaledb_functions.set_policy_scheduled(hypertable, 'policy_reorder', false);
$$
LANGUAGE SQL SET search_path TO pg_catalog, pg_temp;

CREATE FUNCTION @extschema@.enable_reorder_policy(hypertable REGCLASS)
RETURNS INTEGER
AS $$
SELECT _timescaledb_functions.set_policy_scheduled(hypertable, 'policy_reorder', true);
$$
LANGUAGE SQL SET search_path TO pg_catalog, pg_temp;

CREATE FUNCTION @extschema@.disable_retention_policy(hypertable REGCLASS)
RETURNS INTEGER
AS $$
SELECT _timescaledb_functions.set_policy_scheduled(hypertable, 'policy_retention', false);
$$
LANGUAGE SQL SET search_path TO pg_catalog, pg_temp;

CREATE FUNCTION @extschema@.enable_retention_policy(hypertable REGCLASS)
RETURNS INTEGER
AS $$
SELECT _timescaledb_functions.set_policy_scheduled(hypertable, 'policy_retention', true);
$$
LANGUAGE SQL SET search_path TO pg_catalog, pg_temp;

