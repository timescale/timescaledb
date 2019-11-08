-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

SELECT * FROM compress ORDER BY time DESC, small_cardinality;

INSERT INTO compress
SELECT g, 'QW', g::text, 2, 0, (100,4)::custom_type_for_compression, false
FROM generate_series('2019-11-01 00:00'::timestamp, '2019-12-15 00:00'::timestamp, '1 day') g;

SELECT count(compress_chunk(chunk.schema_name|| '.' || chunk.table_name)) as count_compressed
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable hypertable ON (chunk.hypertable_id = hypertable.id)
WHERE hypertable.table_name = 'compress' and chunk.compressed_chunk_id IS NULL;

SELECT * FROM compress ORDER BY time DESC, small_cardinality;
