-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\echo **** Missing dimension slices ****
-- Every non-OSM chunk should have one dimension_slice row per dimension and
-- a matching constraint_<slice_id> CHECK on its relation. List any gaps.
SELECT ht.id AS hypertable_id,
       format('%I.%I', ht.schema_name, ht.table_name)::regclass AS hypertable,
       ch.id AS chunk_id,
       d.id AS dimension_id,
       d.column_name
FROM _timescaledb_catalog.chunk ch
JOIN _timescaledb_catalog.hypertable ht ON ht.id = ch.hypertable_id
JOIN _timescaledb_catalog.dimension d ON d.hypertable_id = ht.id
LEFT JOIN _timescaledb_catalog.dimension_slice ds
       ON ds.chunk_id = ch.id AND ds.dimension_id = d.id
WHERE NOT ch.osm_chunk
  AND ds.id IS NULL
ORDER BY ch.id, d.id;
