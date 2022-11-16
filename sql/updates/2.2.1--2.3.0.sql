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

--drop dependent views
DROP VIEW IF EXISTS timescaledb_information.hypertables;
DROP VIEW IF EXISTS timescaledb_information.chunks;
DROP VIEW IF EXISTS _timescaledb_internal.hypertable_chunk_local_size;
DROP VIEW IF EXISTS _timescaledb_internal.compressed_chunk_stats;

ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.chunk;
ALTER EXTENSION timescaledb DROP SEQUENCE _timescaledb_catalog.chunk_id_seq;
DROP TABLE _timescaledb_catalog.chunk;

CREATE SEQUENCE _timescaledb_catalog.chunk_id_seq MINVALUE 1;

-- now create table without self referential foreign key
CREATE TABLE _timescaledb_catalog.chunk (
  id integer PRIMARY KEY DEFAULT nextval('_timescaledb_catalog.chunk_id_seq'),
  hypertable_id int NOT NULL REFERENCES _timescaledb_catalog.hypertable (id),
  schema_name name NOT NULL,
  table_name name NOT NULL,
  compressed_chunk_id integer ,
  dropped boolean NOT NULL DEFAULT FALSE,
  status integer NOT NULL DEFAULT 0,
  UNIQUE (schema_name, table_name)
);

INSERT INTO _timescaledb_catalog.chunk
( id, hypertable_id, schema_name, table_name,
  compressed_chunk_id, dropped, status)
SELECT id, hypertable_id, schema_name, table_name,
  compressed_chunk_id, dropped, (
    CASE
      WHEN compressed_chunk_id IS NULL THEN 0
      ELSE 1
    END
  )
FROM _timescaledb_internal.chunk_tmp;

--add indexes to the chunk table
CREATE INDEX chunk_hypertable_id_idx ON _timescaledb_catalog.chunk (hypertable_id);
CREATE INDEX chunk_compressed_chunk_id_idx ON _timescaledb_catalog.chunk (compressed_chunk_id);

ALTER SEQUENCE _timescaledb_catalog.chunk_id_seq OWNED BY _timescaledb_catalog.chunk.id;
SELECT setval('_timescaledb_catalog.chunk_id_seq', last_value, is_called) FROM _timescaledb_internal.tmp_chunk_seq_value;

-- add self referential foreign key
ALTER TABLE _timescaledb_catalog.chunk ADD CONSTRAINT chunk_compressed_chunk_id_fkey FOREIGN KEY ( compressed_chunk_id )
 REFERENCES _timescaledb_catalog.chunk( id );
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

--cleanup
DROP TABLE _timescaledb_internal.chunk_tmp;
DROP TABLE _timescaledb_internal.tmp_chunk_seq_value;

GRANT SELECT ON _timescaledb_catalog.chunk_id_seq TO PUBLIC;
GRANT SELECT ON _timescaledb_catalog.chunk TO PUBLIC;

-- end recreate _timescaledb_catalog.chunk table --

-- First we update the permissions of the compressed hypertables to
-- match the associated hypertable.
WITH
    hypertables AS (
	SELECT format('%I.%I', ht.schema_name, ht.table_name)::regclass AS hypertable_oid,
	       format('%I.%I', ct.schema_name, ct.table_name)::regclass AS compressed_hypertable_oid
	FROM _timescaledb_catalog.hypertable ht
	JOIN _timescaledb_catalog.hypertable ct ON ht.compressed_hypertable_id = ct.id
    )
UPDATE pg_class
   SET relacl = (SELECT relacl FROM pg_class WHERE oid = hypertable_oid)
  FROM hypertables
 WHERE oid = compressed_hypertable_oid;

-- Now we update the permissions of chunks of both compressed and
-- uncompressed hypertables to match the permissions of the associated
-- hypertable.
WITH
    chunks AS (
        SELECT format('%I.%I', ht.schema_name, ht.table_name)::regclass AS hypertable_oid,
               format('%I.%I', ch.schema_name, ch.table_name)::regclass AS chunk_oid
          FROM _timescaledb_catalog.hypertable ht
	  JOIN _timescaledb_catalog.chunk ch ON ht.id = ch.hypertable_id
	  WHERE NOT ch.dropped
    )
UPDATE pg_class
   SET relacl = (SELECT relacl FROM pg_class WHERE oid = hypertable_oid)
  FROM chunks
 WHERE oid = chunk_oid;

DROP FUNCTION IF EXISTS _timescaledb_internal.chunk_dml_blocker CASCADE;

-- drop the view as view definition changed
DROP VIEW IF EXISTS _timescaledb_internal.hypertable_chunk_local_size;
