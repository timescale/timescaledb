DROP FUNCTION IF EXISTS _timescaledb_functions.cagg_get_bucket_function_info(INTEGER);
-- remove chunk column statistics related objects
DROP FUNCTION IF EXISTS @extschema@.enable_column_stats(REGCLASS, NAME, BOOLEAN);
DROP FUNCTION IF EXISTS @extschema@.disable_column_stats(REGCLASS, NAME, BOOLEAN);
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.chunk_column_stats;
ALTER EXTENSION timescaledb DROP SEQUENCE _timescaledb_catalog.chunk_column_stats_id_seq;
DROP TABLE IF EXISTS _timescaledb_catalog.chunk_column_stats;

-- Add foreign key constraints back to compressed chunks
DO $$
DECLARE
  chunkrelid regclass;
  conname name;
  conoid oid;
BEGIN
  FOR chunkrelid, conname, conoid IN
  SELECT format('%I.%I',ch.schema_name,ch.table_name)::regclass, con.conname, con.oid
  FROM _timescaledb_catalog.hypertable ht
  JOIN pg_constraint con ON con.contype = 'f' AND con.conrelid=format('%I.%I',ht.schema_name,ht.table_name)::regclass
  JOIN _timescaledb_catalog.chunk ch on ch.hypertable_id=ht.compressed_hypertable_id and not ch.dropped
  LOOP
    EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %I %s', chunkrelid, conname, pg_get_constraintdef(conoid));
  END LOOP;
END $$;
