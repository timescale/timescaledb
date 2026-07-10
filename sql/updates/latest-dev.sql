
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

--
-- END set compression status flag on hypertables
--

ALTER TABLE _timescaledb_catalog.hypertable DROP CONSTRAINT hypertable_dim_compress_check;
ALTER TABLE _timescaledb_catalog.hypertable ADD CONSTRAINT hypertable_num_dimensions_check CHECK (num_dimensions > 0);
ALTER TABLE _timescaledb_catalog.hypertable DROP CONSTRAINT hypertable_compress_check;

DROP FUNCTION IF EXISTS @extschema@.alter_job(job_id INTEGER, schedule_interval INTERVAL, max_runtime INTERVAL, max_retries INTEGER, retry_period INTERVAL, scheduled BOOL, config JSONB, next_start TIMESTAMPTZ, if_exists BOOL, check_config REGPROC, fixed_schedule BOOL, initial_start TIMESTAMPTZ, timezone TEXT, job_name TEXT);

