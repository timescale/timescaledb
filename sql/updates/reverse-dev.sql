-- API changes related to hypertable generalization
DROP FUNCTION IF EXISTS @extschema@.add_dimension(regclass,dimension_info,boolean);
DROP FUNCTION IF EXISTS @extschema@.create_hypertable(regclass,dimension_info,boolean,boolean,boolean);
DROP FUNCTION IF EXISTS @extschema@.set_partitioning_interval(regclass,anyelement,name);
DROP FUNCTION IF EXISTS @extschema@.by_hash(name,integer,regproc);
DROP FUNCTION IF EXISTS @extschema@.by_range(name,anyelement,regproc);

DROP TYPE IF EXISTS _timescaledb_internal.dimension_info CASCADE;

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
  osm_chunk boolean NOT NULL DEFAULT FALSE,
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

--add indexes to the chunk table
CREATE INDEX chunk_hypertable_id_idx ON _timescaledb_catalog.chunk (hypertable_id);
CREATE INDEX chunk_compressed_chunk_id_idx ON _timescaledb_catalog.chunk (compressed_chunk_id);
CREATE INDEX chunk_osm_chunk_idx ON _timescaledb_catalog.chunk (osm_chunk, hypertable_id);

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


--
-- Rebuild the catalog table `_timescaledb_catalog.compression_chunk_size` to
-- remove column `numrows_frozen_immediately`
--
CREATE TABLE _timescaledb_internal.compression_chunk_size_tmp
    AS SELECT * from _timescaledb_catalog.compression_chunk_size;

-- Drop depended views
-- We assume that '_timescaledb_internal.compressed_chunk_stats' was already dropped in this update
-- (see above)

-- Drop table
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.compression_chunk_size;
DROP TABLE _timescaledb_catalog.compression_chunk_size;

CREATE TABLE _timescaledb_catalog.compression_chunk_size (
  chunk_id integer NOT NULL,
  compressed_chunk_id integer NOT NULL,
  uncompressed_heap_size bigint NOT NULL,
  uncompressed_toast_size bigint NOT NULL,
  uncompressed_index_size bigint NOT NULL,
  compressed_heap_size bigint NOT NULL,
  compressed_toast_size bigint NOT NULL,
  compressed_index_size bigint NOT NULL,
  numrows_pre_compression bigint,
  numrows_post_compression bigint,
  -- table constraints
  CONSTRAINT compression_chunk_size_pkey PRIMARY KEY (chunk_id),
  CONSTRAINT compression_chunk_size_chunk_id_fkey FOREIGN KEY (chunk_id) REFERENCES _timescaledb_catalog.chunk (id) ON DELETE CASCADE,
  CONSTRAINT compression_chunk_size_compressed_chunk_id_fkey FOREIGN KEY (compressed_chunk_id) REFERENCES _timescaledb_catalog.chunk (id) ON DELETE CASCADE
);

INSERT INTO _timescaledb_catalog.compression_chunk_size
(chunk_id, compressed_chunk_id, uncompressed_heap_size, uncompressed_toast_size,
  uncompressed_index_size, compressed_heap_size, compressed_toast_size,
  compressed_index_size, numrows_pre_compression, numrows_post_compression)
SELECT chunk_id, compressed_chunk_id, uncompressed_heap_size, uncompressed_toast_size,
  uncompressed_index_size, compressed_heap_size, compressed_toast_size,
  compressed_index_size, numrows_pre_compression, numrows_post_compression
FROM _timescaledb_internal.compression_chunk_size_tmp;

DROP TABLE _timescaledb_internal.compression_chunk_size_tmp;

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.compression_chunk_size', '');

GRANT SELECT ON _timescaledb_catalog.compression_chunk_size TO PUBLIC;

-- End modify `_timescaledb_catalog.compression_chunk_size`
