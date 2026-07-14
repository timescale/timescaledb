
DROP FUNCTION IF EXISTS _timescaledb_functions.estimate_uncompressed_size(regclass);

DROP PROCEDURE IF EXISTS _timescaledb_functions.policy_compression_execute(INTEGER, INTEGER, ANYELEMENT, INTEGER, BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN);

--
-- BEGIN chunk.compressed_chunk_id no longer used
--

ALTER TABLE _timescaledb_catalog.compression_chunk_size DROP CONSTRAINT IF EXISTS compression_chunk_size_compressed_chunk_id_fkey;
DROP INDEX IF EXISTS _timescaledb_catalog.chunk_compressed_chunk_id_idx;

UPDATE _timescaledb_catalog.chunk SET compressed_chunk_id = NULL WHERE compressed_chunk_id IS NOT NULL;
UPDATE _timescaledb_catalog.compression_chunk_size SET compressed_chunk_id = 0 WHERE compressed_chunk_id <> 0;

DELETE FROM _timescaledb_catalog.chunk
WHERE hypertable_id IN (
  SELECT compressed_hypertable_id
  FROM _timescaledb_catalog.hypertable
  WHERE compressed_hypertable_id IS NOT NULL
);

--
-- END chunk.compressed_chunk_id no longer used
--

--
-- BEGIN hypertable.compressed_hypertable_id no longer used
--

-- Compression no longer keeps a separate internal hypertable for the compressed
-- data. Detach every hypertable from its internal compressed hypertable and
-- remove the now unused compressed hypertable entries and their empty relations.
-- The compressed data itself is stored in the per chunk compressed relations and
-- is not affected by this.
DO $$
DECLARE
  compressed_relations text[];
  rel text;
BEGIN
  SELECT array_agg(format('%I.%I', schema_name, table_name))
  INTO compressed_relations
  FROM _timescaledb_catalog.hypertable
  WHERE compression_state = 2;

  UPDATE _timescaledb_catalog.hypertable SET compressed_hypertable_id = NULL WHERE compressed_hypertable_id IS NOT NULL;
  DELETE FROM _timescaledb_catalog.hypertable WHERE compression_state = 2;

  IF compressed_relations IS NOT NULL THEN
    FOREACH rel IN ARRAY compressed_relations
    LOOP
      EXECUTE format('DROP TABLE IF EXISTS %s', rel);
    END LOOP;
  END IF;
END
$$;

--
-- END hypertable.compressed_hypertable_id no longer used
--

--
-- BEGIN set compression status flag on hypertables
--

UPDATE _timescaledb_catalog.hypertable
SET status = status | 4, -- set compression bit
    compression_state = 0
WHERE compression_state = 1;

ALTER TABLE _timescaledb_catalog.hypertable DROP CONSTRAINT hypertable_dim_compress_check;
ALTER TABLE _timescaledb_catalog.hypertable ADD CONSTRAINT hypertable_num_dimensions_check CHECK (num_dimensions > 0);
ALTER TABLE _timescaledb_catalog.hypertable DROP CONSTRAINT hypertable_compress_check;

--
-- END set compression status flag on hypertables
--

DROP FUNCTION IF EXISTS @extschema@.alter_job(job_id INTEGER, schedule_interval INTERVAL, max_runtime INTERVAL, max_retries INTEGER, retry_period INTERVAL, scheduled BOOL, config JSONB, next_start TIMESTAMPTZ, if_exists BOOL, check_config REGPROC, fixed_schedule BOOL, initial_start TIMESTAMPTZ, timezone TEXT, job_name TEXT);

--
-- BEGIN add chunk.relid and drop chunk.compressed_chunk_id, schema_name, table_name
--

CREATE TABLE _timescaledb_internal.tmp_chunk AS SELECT * FROM _timescaledb_catalog.chunk;
CREATE TABLE _timescaledb_internal.tmp_chunk_seq_value AS SELECT last_value, is_called FROM _timescaledb_catalog.chunk_id_seq;

-- drop foreign keys referencing the chunk table
ALTER TABLE _timescaledb_catalog.dimension_slice DROP CONSTRAINT dimension_slice_chunk_id_fkey;
ALTER TABLE _timescaledb_catalog.chunk_column_stats DROP CONSTRAINT chunk_column_stats_chunk_id_fkey;
ALTER TABLE _timescaledb_internal.bgw_policy_chunk_stats DROP CONSTRAINT bgw_policy_chunk_stats_chunk_id_fkey;
ALTER TABLE _timescaledb_catalog.compression_chunk_size DROP CONSTRAINT compression_chunk_size_chunk_id_fkey;

-- drop dependent views, they are recreated later in the update
DROP VIEW IF EXISTS _timescaledb_internal.hypertable_chunk_local_size;
DROP VIEW IF EXISTS _timescaledb_internal.compressed_chunk_stats;

ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.chunk;
ALTER EXTENSION timescaledb DROP SEQUENCE _timescaledb_catalog.chunk_id_seq;

DROP TABLE _timescaledb_catalog.chunk;

CREATE SEQUENCE _timescaledb_catalog.chunk_id_seq MINVALUE 1;

CREATE TABLE _timescaledb_catalog.chunk (
  id integer NOT NULL DEFAULT nextval('_timescaledb_catalog.chunk_id_seq'),
  relid regclass NOT NULL,
  hypertable_id int NOT NULL,
  status integer NOT NULL DEFAULT 0,
  osm_chunk boolean NOT NULL DEFAULT FALSE,
  creation_time timestamptz NOT NULL,
  CONSTRAINT chunk_pkey PRIMARY KEY (id),
  CONSTRAINT chunk_relid_key UNIQUE (relid)
) WITH (user_catalog_table = true);

INSERT INTO _timescaledb_catalog.chunk
  (id, relid, hypertable_id, status, osm_chunk, creation_time)
SELECT
  id, format('%I.%I', schema_name, table_name)::regclass, hypertable_id, status, osm_chunk, creation_time
FROM _timescaledb_internal.tmp_chunk;

CREATE INDEX chunk_hypertable_id_idx ON _timescaledb_catalog.chunk (hypertable_id);
CREATE INDEX chunk_osm_chunk_idx ON _timescaledb_catalog.chunk (osm_chunk, hypertable_id);
CREATE INDEX chunk_hypertable_id_creation_time_idx ON _timescaledb_catalog.chunk (hypertable_id, creation_time);

ALTER SEQUENCE _timescaledb_catalog.chunk_id_seq OWNED BY _timescaledb_catalog.chunk.id;
SELECT setval('_timescaledb_catalog.chunk_id_seq', last_value, is_called) FROM _timescaledb_internal.tmp_chunk_seq_value;

ALTER TABLE _timescaledb_catalog.chunk ADD CONSTRAINT chunk_hypertable_id_fkey FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk', '');
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_id_seq', '');

-- restore foreign keys referencing the chunk table
ALTER TABLE _timescaledb_catalog.dimension_slice ADD CONSTRAINT dimension_slice_chunk_id_fkey FOREIGN KEY (chunk_id) REFERENCES _timescaledb_catalog.chunk (id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.chunk_column_stats ADD CONSTRAINT chunk_column_stats_chunk_id_fkey FOREIGN KEY (chunk_id) REFERENCES _timescaledb_catalog.chunk (id);
ALTER TABLE _timescaledb_internal.bgw_policy_chunk_stats ADD CONSTRAINT bgw_policy_chunk_stats_chunk_id_fkey FOREIGN KEY (chunk_id) REFERENCES _timescaledb_catalog.chunk (id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.compression_chunk_size ADD CONSTRAINT compression_chunk_size_chunk_id_fkey FOREIGN KEY (chunk_id) REFERENCES _timescaledb_catalog.chunk (id) ON DELETE CASCADE;

DROP TABLE _timescaledb_internal.tmp_chunk;
DROP TABLE _timescaledb_internal.tmp_chunk_seq_value;

GRANT SELECT ON _timescaledb_catalog.chunk_id_seq TO PUBLIC;
GRANT SELECT ON _timescaledb_catalog.chunk TO PUBLIC;

--
-- END add chunk.relid
--


INSERT INTO _timescaledb_catalog.compression_algorithm( id, version, name, description) values
( 8, 1, 'COMPRESSION_ALGORITHM_EXTERNAL', 'external');
