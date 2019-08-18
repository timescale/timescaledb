-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set ECHO errors

DROP TABLE IF EXISTS original_result;


CREATE TABLE original_result AS :QUERY;

SELECT count(compress_chunk(chunk.schema_name|| '.' || chunk.table_name))
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable hypertable ON (chunk.hypertable_id = hypertable.id)
WHERE hypertable.table_name like :'HYPERTABLE_NAME' and chunk.compressed_chunk_id IS NULL;

--TODO: run query on compressed data and compare to original result once transparent decryption is in

SELECT count(decompress_chunk(chunk.schema_name|| '.' || chunk.table_name))
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable hypertable ON (chunk.hypertable_id = hypertable.id)
WHERE hypertable.table_name like :'HYPERTABLE_NAME' and chunk.compressed_chunk_id IS NOT NULL;

--run data on data that's been compressed and decompressed, make sure it's the same.
with original AS (
  SELECT row_number() OVER() row_number, * FROM original_result
),
uncompressed AS (
  SELECT row_number() OVER() row_number, * FROM (:QUERY) as q
)
SELECT 'Number of rows different between original and uncompressed (expect 0)', count(*)
--SELECT original."Time", uncompressed."Time"
FROM original
FULL OUTER JOIN uncompressed ON (original.row_number = uncompressed.row_number)
WHERE (original.*) IS DISTINCT FROM (uncompressed.*);

--TODO: add pg_dump, restore.

\set ECHO all
