-- Reset the attislocal/conislocal flags on chunk columns and constraints left
-- marked local by a detach_chunk/attach_chunk round-trip so a later hypertable
-- DROP COLUMN or DROP CONSTRAINT propagates to them.
-- Locally-defined objects (inhcount = 0), such as the chunk's dimension
-- constraints, are left untouched.
UPDATE pg_catalog.pg_attribute a
SET attislocal = false
FROM _timescaledb_catalog.chunk c
WHERE a.attrelid = c.relid
  AND a.attnum > 0
  AND NOT a.attisdropped
  AND a.attislocal
  AND a.attinhcount > 0;

UPDATE pg_catalog.pg_constraint con
SET conislocal = false
FROM _timescaledb_catalog.chunk c
WHERE con.conrelid = c.relid
  AND con.conislocal
  AND con.coninhcount > 0;

