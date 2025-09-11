-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set TEST_BASE_NAME recompression_intergrity_check
SELECT format('%s/results/%s_results_compressed.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_COMPRESS",
       format('%s/results/%s_results_recompressed.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_RECOMPRESS"
\gset
SELECT format('\! diff -u  --label "Compressed result" --label "Recompressed result" %s %s', :'TEST_RESULTS_COMPRESS', :'TEST_RESULTS_RECOMPRESS') as "DIFF_CMD"
\gset

-- Compress all uncompressed chunks
SELECT compress_chunk(ch)
FROM show_chunks(:'TEST_TABLE_NAME') ch;

-- Store initial compressed chunk info before recompression
SELECT uncompressed.schema_name || '.' || uncompressed.table_name AS "OLD_CHUNK_NAME",
        compressed.schema_name || '.' || compressed.table_name AS "OLD_COMPRESSED_CHUNK_NAME",
        compressed.id AS "OLD_CHUNK_ID"
FROM _timescaledb_catalog.chunk uncompressed
JOIN _timescaledb_catalog.chunk compressed
  ON uncompressed.compressed_chunk_id = compressed.id
WHERE uncompressed.hypertable_id = (
          SELECT id
          FROM _timescaledb_catalog.hypertable
          WHERE table_name = :'TEST_TABLE_NAME'
      )
LIMIT 1 \gset

\set COMPRESSED_CHUNK_NAME :OLD_COMPRESSED_CHUNK_NAME
:BATCH_METADATA_QUERY

\set QUERY1 'SELECT COUNT(*) FROM ' :OLD_CHUNK_NAME ';'
\set QUERY2 'SELECT * FROM ' :OLD_CHUNK_NAME ';'

\o :TEST_RESULTS_COMPRESS
:QUERY1
:QUERY2
\o

-- Recompress the chunk in-memory
SELECT compress_chunk(:'OLD_CHUNK_NAME', recompress := true);

-- Get info for the new compressed chunk
SELECT compressed.schema_name || '.' || compressed.table_name AS "NEW_COMPRESSED_CHUNK_NAME",
        compressed.id AS "NEW_CHUNK_ID"
FROM _timescaledb_catalog.chunk uncompressed
JOIN _timescaledb_catalog.chunk compressed
  ON uncompressed.compressed_chunk_id = compressed.id
WHERE uncompressed.schema_name || '.' || uncompressed.table_name = :'OLD_CHUNK_NAME'
LIMIT 1 \gset

\set COMPRESSED_CHUNK_NAME :NEW_COMPRESSED_CHUNK_NAME
:BATCH_METADATA_QUERY

-- Get data after in-memory recompression
\o :TEST_RESULTS_RECOMPRESS
:QUERY1
:QUERY2
\o

-- Check if a new chunk was created (this will show in the output)
SELECT
  CASE WHEN :'NEW_CHUNK_ID' IS NULL OR :'OLD_CHUNK_ID' = :'NEW_CHUNK_ID' THEN
    'ERROR: Recompression did not create a new chunk'
  ELSE
    'SUCCESS: New chunk created, old_chunk_id=' || :'OLD_CHUNK_ID' || ', new_chunk_id=' || :'NEW_CHUNK_ID'
  END AS recompression_status;

-- Compare result using diff to validate integrity of recompressed data
:DIFF_CMD