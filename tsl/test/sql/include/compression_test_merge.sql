-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set ECHO errors

DROP TABLE IF EXISTS merge_result;

CREATE TABLE merge_result AS :QUERY;

SELECT count(compress_chunk(chunk, true)) as count_compressed
FROM show_chunks(:'HYPERTABLE_NAME') chunk;

with original AS (
  SELECT row_number() OVER() row_number, * FROM merge_result
),
compressed AS (
  SELECT row_number() OVER() row_number, * FROM (:QUERY) as q
)
SELECT 'Number of rows different between original and query on compressed data (expect 0)', count(*)
FROM original
FULL OUTER JOIN compressed ON (original.row_number = compressed.row_number)
WHERE (original.*) IS DISTINCT FROM (compressed.*);

SELECT count(decompress_chunk(chunk.schema_name|| '.' || chunk.table_name)) as count_decompressed
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable hypertable ON (chunk.hypertable_id = hypertable.id)
WHERE hypertable.table_name like :'HYPERTABLE_NAME' and chunk.status & 1 > 0;


--run data on data that's been compressed and decompressed, make sure it's the same.
with original AS (
  SELECT row_number() OVER() row_number, * FROM merge_result
),
uncompressed AS (
  SELECT row_number() OVER() row_number, * FROM (:QUERY) as q
)
SELECT 'Number of rows different between original and data that has been compressed and then decompressed (expect 0)', count(*)
FROM original
FULL OUTER JOIN uncompressed ON (original.row_number = uncompressed.row_number)
WHERE (original.*) IS DISTINCT FROM (uncompressed.*);

\set ECHO all
