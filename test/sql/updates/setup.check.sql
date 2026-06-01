-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

SELECT (extversion >= '2.28.0') AS has_chunk_owned_slices
  FROM pg_extension WHERE extname = 'timescaledb' \gset

-- Every non-OSM chunk should have one dimension_slice per dimension. List gaps.
\echo **** Missing dimension slices ****
SELECT ht.id AS hypertable_id,
       format('%I.%I', ht.schema_name, ht.table_name)::regclass AS hypertable,
       ch.id AS chunk_id,
       d.id AS dimension_id,
       d.column_name
FROM _timescaledb_catalog.chunk ch
JOIN _timescaledb_catalog.hypertable ht ON ht.id = ch.hypertable_id
JOIN _timescaledb_catalog.dimension d ON d.hypertable_id = ht.id
WHERE NOT ch.osm_chunk
\if :has_chunk_owned_slices
  AND NOT EXISTS (SELECT 1 FROM _timescaledb_catalog.dimension_slice ds
                  WHERE ds.chunk_id = ch.id AND ds.dimension_id = d.id)
\else
  AND NOT EXISTS (SELECT 1 FROM _timescaledb_catalog.chunk_constraint cc
                  JOIN _timescaledb_catalog.dimension_slice ds ON ds.id = cc.dimension_slice_id
                  WHERE cc.chunk_id = ch.id AND ds.dimension_id = d.id)
\endif
ORDER BY ch.id, d.id;
