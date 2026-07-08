DROP FUNCTION IF EXISTS _timescaledb_functions.decompress_batch(record);
DROP FUNCTION IF EXISTS _timescaledb_functions.estimate_uncompressed_size(regclass, double precision);
DROP FUNCTION IF EXISTS _timescaledb_functions.compact_chunk(REGCLASS);

--
-- BEGIN compression status flag on hypertables
--

UPDATE _timescaledb_catalog.hypertable
SET compression_state = 1,
    status = status & ~4 -- clear compression bit
WHERE status & 4 <> 0;

--
-- END compression status flag on hypertables
--

ALTER TABLE _timescaledb_catalog.hypertable DROP CONSTRAINT hypertable_num_dimensions_check;
ALTER TABLE _timescaledb_catalog.hypertable ADD CONSTRAINT hypertable_dim_compress_check CHECK (num_dimensions > 0 OR compression_state = 2);
ALTER TABLE _timescaledb_catalog.hypertable ADD CONSTRAINT hypertable_compress_check CHECK ( (compression_state = 0 OR compression_state = 1 )  OR (compression_state = 2 AND compressed_hypertable_id IS NULL));

--
-- BEGIN restore internal compressed hypertables
--

-- Older versions kept a separate internal hypertable for the compressed data of
-- every hypertable with compression enabled. It only served bookkeeping
-- purposes and had no columns of its own. Recreate an empty compressed
-- hypertable for every hypertable with compression enabled and link it through
-- compressed_hypertable_id so the rest of the downgrade can repopulate the
-- compressed chunks below.
DO $$
DECLARE
  ht RECORD;
  compress_hypertable_id integer;
  compress_table_name name;
  ht_owner name;
  acl RECORD;
BEGIN
  FOR ht IN
    SELECT id, format('%I.%I', schema_name, table_name)::regclass AS relid
    FROM _timescaledb_catalog.hypertable
    WHERE compression_state = 1
      AND compressed_hypertable_id IS NULL
    ORDER BY id
  LOOP
    compress_hypertable_id := nextval('_timescaledb_catalog.hypertable_id_seq');
    compress_table_name := format('_compressed_hypertable_%s', compress_hypertable_id);

    EXECUTE format('CREATE TABLE _timescaledb_internal.%I ()', compress_table_name);

    -- The compressed hypertable takes owner and permissions from its
    -- uncompressed counterpart. Older versions copy these onto the compressed
    -- chunks when they are (re)created, so without this the compressed chunks
    -- would lose their access rights.
    SELECT pg_catalog.pg_get_userbyid(relowner) INTO ht_owner FROM pg_catalog.pg_class WHERE oid = ht.relid;
    EXECUTE format('ALTER TABLE _timescaledb_internal.%I OWNER TO %I', compress_table_name, ht_owner);
    FOR acl IN
      SELECT e.grantee, e.privilege_type, e.is_grantable
      FROM pg_catalog.pg_class c, pg_catalog.aclexplode(c.relacl) e
      WHERE c.oid = ht.relid AND e.grantee <> c.relowner
    LOOP
      EXECUTE format('GRANT %s ON _timescaledb_internal.%I TO %s%s',
                     acl.privilege_type,
                     compress_table_name,
                     CASE WHEN acl.grantee = 0 THEN 'PUBLIC' ELSE quote_ident(pg_catalog.pg_get_userbyid(acl.grantee)) END,
                     CASE WHEN acl.is_grantable THEN ' WITH GRANT OPTION' ELSE '' END);
    END LOOP;

    -- Tables created inside an extension script become members of the
    -- extension and would not be dumped by pg_dump. Detaching it after setting
    -- the permissions also clears the pg_init_privs entry so the grants above
    -- are dumped like those of a regular internal relation.
    EXECUTE format('ALTER EXTENSION timescaledb DROP TABLE _timescaledb_internal.%I', compress_table_name);

    INSERT INTO _timescaledb_catalog.hypertable
      (id, schema_name, table_name, associated_schema_name, associated_table_prefix,
       num_dimensions, chunk_sizing_func_schema, chunk_sizing_func_name, chunk_target_size,
       compression_state, compressed_hypertable_id, status)
    VALUES
      (compress_hypertable_id, '_timescaledb_internal', compress_table_name,
       '_timescaledb_internal', format('_hyper_%s', compress_hypertable_id),
       0, '_timescaledb_functions', 'calculate_chunk_interval', 0,
       2, NULL, 0);

    UPDATE _timescaledb_catalog.hypertable
    SET compressed_hypertable_id = compress_hypertable_id
    WHERE id = ht.id;
  END LOOP;
END
$$;

--
-- END restore internal compressed hypertables
--

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
