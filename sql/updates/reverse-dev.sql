DROP FUNCTION IF EXISTS _timescaledb_functions.decompress_batch(record);
DROP FUNCTION IF EXISTS _timescaledb_functions.estimate_uncompressed_size(regclass, double precision);
DROP FUNCTION IF EXISTS _timescaledb_functions.compact_chunk(REGCLASS);

--
-- BEGIN repopulate chunk.compressed_chunk_id
--

-- Create the missing catalog row for every compressed relation that does not
-- have one yet. The row belongs to the internal compressed hypertable of the
-- chunk's hypertable.
INSERT INTO _timescaledb_catalog.chunk
  (id, hypertable_id, schema_name, table_name, compressed_chunk_id, status, osm_chunk, creation_time)
SELECT
  nextval('_timescaledb_catalog.chunk_id_seq'),
  uht.compressed_hypertable_id,
  n.nspname,
  c.relname,
  NULL,
  0,
  false,
  uc.creation_time
FROM _timescaledb_catalog.compression_settings cs
JOIN pg_class c ON c.oid = cs.compress_relid
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN _timescaledb_catalog.chunk uc
  ON format('%I.%I', uc.schema_name, uc.table_name)::regclass = cs.relid
JOIN _timescaledb_catalog.hypertable uht ON uht.id = uc.hypertable_id
WHERE cs.compress_relid IS NOT NULL
  AND uht.compressed_hypertable_id IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM _timescaledb_catalog.chunk cc
    WHERE format('%I.%I', cc.schema_name, cc.table_name)::regclass = cs.compress_relid
  );

-- Link the uncompressed chunk to its compressed chunk.
UPDATE _timescaledb_catalog.chunk uc
SET compressed_chunk_id = cc.id
FROM _timescaledb_catalog.compression_settings cs
JOIN _timescaledb_catalog.chunk cc
  ON format('%I.%I', cc.schema_name, cc.table_name)::regclass = cs.compress_relid
WHERE format('%I.%I', uc.schema_name, uc.table_name)::regclass = cs.relid
  AND cs.compress_relid IS NOT NULL
  AND uc.compressed_chunk_id IS NULL;

-- Restore the compressed_chunk_id reference in compression_chunk_size.
UPDATE _timescaledb_catalog.compression_chunk_size ccs
SET compressed_chunk_id = uc.compressed_chunk_id
FROM _timescaledb_catalog.chunk uc
WHERE uc.id = ccs.chunk_id
  AND uc.compressed_chunk_id IS NOT NULL
  AND (
    ccs.compressed_chunk_id = 0
    OR NOT EXISTS (SELECT 1 FROM _timescaledb_catalog.chunk c WHERE c.id = ccs.compressed_chunk_id)
  );

CREATE INDEX chunk_compressed_chunk_id_idx ON _timescaledb_catalog.chunk (compressed_chunk_id);

-- Drop any leftover size rows that still cannot be linked to a compressed chunk
-- so the foreign key can be recreated.
DELETE FROM _timescaledb_catalog.compression_chunk_size ccs
WHERE ccs.compressed_chunk_id = 0
  OR NOT EXISTS (SELECT 1 FROM _timescaledb_catalog.chunk c WHERE c.id = ccs.compressed_chunk_id);

ALTER TABLE _timescaledb_catalog.compression_chunk_size
  ADD CONSTRAINT compression_chunk_size_compressed_chunk_id_fkey
  FOREIGN KEY (compressed_chunk_id) REFERENCES _timescaledb_catalog.chunk (id) ON DELETE CASCADE;

--
-- END repopulate chunk.compressed_chunk_id
--

-- Remove compaction policy jobs since the policy does not exist in the older version.
DELETE FROM _timescaledb_config.bgw_job WHERE proc_schema = '_timescaledb_functions' AND proc_name = 'policy_compaction';
DROP FUNCTION IF EXISTS @extschema@.add_compaction_policy(REGCLASS, BOOL, INTERVAL, TIMESTAMPTZ, TEXT, INTEGER, INTERVAL);
DROP FUNCTION IF EXISTS @extschema@.remove_compaction_policy(REGCLASS, BOOL);
DROP PROCEDURE IF EXISTS _timescaledb_functions.policy_compaction(INTEGER, JSONB);
DROP FUNCTION IF EXISTS _timescaledb_functions.policy_compaction_check(JSONB);
