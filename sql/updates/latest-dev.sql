
-- Link the compressed relations to their chunk in pg_depend.
INSERT INTO pg_catalog.pg_depend (classid, objid, objsubid, refclassid, refobjid, refobjsubid, deptype)
SELECT 'pg_catalog.pg_class'::regclass::oid, cs.compress_relid::oid, 0,
       'pg_catalog.pg_class'::regclass::oid, cs.relid::oid, 0, 'i'
FROM _timescaledb_catalog.compression_settings cs
WHERE cs.compress_relid IS NOT NULL
  AND NOT EXISTS (
    SELECT FROM pg_catalog.pg_depend d
    WHERE d.classid = 'pg_catalog.pg_class'::regclass::oid
      AND d.objid = cs.compress_relid::oid
      AND d.objsubid = 0
      AND d.refclassid = 'pg_catalog.pg_class'::regclass::oid
      AND d.refobjid = cs.relid::oid
      AND d.refobjsubid = 0
      AND d.deptype = 'i');

