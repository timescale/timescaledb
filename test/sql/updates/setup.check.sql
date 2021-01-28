-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\c single
\echo **** Missing dimension slices ****
SELECT hypertable_id,
       chunk_id,
       dimension_slice_id,
       constraint_name,
       attname AS column_name
FROM _timescaledb_catalog.chunk_constraint cc
JOIN _timescaledb_catalog.chunk ch ON cc.chunk_id = ch.id
JOIN pg_constraint ON conname = constraint_name
JOIN pg_namespace ns ON connamespace = ns.oid AND ns.nspname = ch.schema_name
JOIN pg_attribute ON attnum = conkey[1] AND attrelid = conrelid
WHERE dimension_slice_id NOT IN (SELECT id FROM _timescaledb_catalog.dimension_slice);
