-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test array compression overallocation protection
CREATE UNLOGGED TABLE vector_overalloc (time timestamptz, device int8, ord numeric, value text);
SELECT create_hypertable('vector_overalloc', 'time', create_default_indexes => false);
-- This will try to create an array of chars over 1GB allocation limit
INSERT INTO vector_overalloc
VALUES
	('2025-01-01 00:00:00', 1, 1, repeat(md5(random()::text), 32*1024*200)), --200 MB value
	('2025-01-01 00:00:01', 1, 1, repeat(md5(random()::text), 32*1024*200)),
	('2025-01-01 00:00:02', 1, 1, repeat(md5(random()::text), 32*1024*200)),
	('2025-01-01 00:00:03', 1, 1, repeat(md5(random()::text), 32*1024*200)),
	('2025-01-01 00:00:04', 1, 1, repeat(md5(random()::text), 32*1024*200)),
	('2025-01-01 00:00:05', 1, 1, repeat(md5(random()::text), 32*1024*200));

ALTER TABLE vector_overalloc SET (tsdb.compress, tsdb.compress_segmentby='device', tsdb.compress_orderby='time');
SET timescaledb.enable_compressor_batch_limit to true;
SELECT compress_chunk(ch) FROM show_chunks('vector_overalloc') ch;

SELECT ch1.id "CHUNK_ID"
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht where ch1.hypertable_id = ht.id and ht.table_name like 'vector_overalloc'
ORDER BY ch1.id
LIMIT 1 \gset

select  compressed.schema_name|| '.' || compressed.table_name as "COMPRESSED_CHUNK_NAME"
from _timescaledb_catalog.chunk uncompressed, _timescaledb_catalog.chunk compressed
where uncompressed.compressed_chunk_id = compressed.id AND uncompressed.id = :'CHUNK_ID' \gset

-- Confirm we have multiple batches
SELECT _ts_meta_count FROM :COMPRESSED_CHUNK_NAME ORDER BY device, _ts_meta_min_1 ASC;

DROP TABLE vector_overalloc;
