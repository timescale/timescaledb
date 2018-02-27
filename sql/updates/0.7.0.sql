-- Fix for potentially broken 0.6.1--0.7.0 update
WITH ind AS (
    SELECT chunk_con.chunk_id, pg_chunk_index_class.relname AS index_name
FROM _timescaledb_catalog.chunk_constraint chunk_con
INNER JOIN _timescaledb_catalog.chunk chunk ON (chunk_con.chunk_id = chunk.id)
INNER JOIN _timescaledb_catalog.hypertable hypertable ON (chunk.hypertable_id = hypertable.id)
INNER JOIN pg_constraint pg_chunk_con ON (
        pg_chunk_con.conrelid = format('%I.%I', chunk.schema_name, chunk.table_name)::regclass
        AND pg_chunk_con.conname = chunk_con.constraint_name
        AND pg_chunk_con.contype = 'f'
)
INNER JOIN pg_class pg_chunk_index_class ON (
    pg_chunk_con.conindid = pg_chunk_index_class.oid
)
)
DELETE
FROM _timescaledb_catalog.chunk_index ci
USING ind
WHERE ci.chunk_id = ind.chunk_id AND ci.index_name = ind.index_name;
