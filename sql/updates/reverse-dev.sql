-- Remove dependency from internal compressed chunks to their hypertables
DELETE FROM pg_depend d
USING _timescaledb_catalog.compression_settings cs
WHERE cs.compress_relid IS NOT NULL
  AND d.classid = 'pg_class'::regclass
  AND d.objid = cs.compress_relid
  AND d.objsubid = 0
  AND d.refclassid = 'pg_class'::regclass
  AND d.refobjsubid = 0
  AND d.deptype = 'a';
