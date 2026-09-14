-- Add dependency from internal compressed chunks to their hypertables
WITH deps AS (
    SELECT cs.compress_relid, format('%I.%I', ht.schema_name, ht.table_name)::regclass AS hypertable_relid
    FROM _timescaledb_catalog.compression_settings cs
    JOIN _timescaledb_catalog.chunk ch ON ch.relid = cs.relid
    JOIN _timescaledb_catalog.hypertable ht ON ht.id = ch.hypertable_id
    WHERE cs.compress_relid IS NOT NULL
)
INSERT INTO pg_depend (classid, objid, objsubid, refclassid, refobjid, refobjsubid, deptype)
SELECT 'pg_class'::regclass, deps.compress_relid, 0, 'pg_class'::regclass, deps.hypertable_relid, 0, 'a'
FROM deps
WHERE NOT EXISTS (
    SELECT 1 FROM pg_depend d
    WHERE d.classid = 'pg_class'::regclass
      AND d.objid = deps.compress_relid
      AND d.objsubid = 0
      AND d.refclassid = 'pg_class'::regclass
      AND d.refobjid = deps.hypertable_relid
      AND d.refobjsubid = 0
      AND d.deptype = 'a'
);
