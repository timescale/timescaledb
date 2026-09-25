
-- Remove the pg_depend link between the compressed relations and their chunk.
DELETE FROM pg_catalog.pg_depend d
USING _timescaledb_catalog.compression_settings cs
WHERE d.classid = 'pg_catalog.pg_class'::regclass::oid
  AND d.objid = cs.compress_relid::oid
  AND d.objsubid = 0
  AND d.refclassid = 'pg_catalog.pg_class'::regclass::oid
  AND d.refobjid = cs.relid::oid
  AND d.refobjsubid = 0
  AND d.deptype = 'i';

DROP FUNCTION IF EXISTS _timescaledb_functions.restore_compressed_relation_dependencies();

