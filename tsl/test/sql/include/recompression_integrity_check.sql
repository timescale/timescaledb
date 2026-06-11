-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SELECT format('%s/results/%s_results_compressed.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_COMPRESS",
       format('%s/results/%s_results_recompressed.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_RECOMPRESS"
\gset
SELECT format('\! diff -u  --label "Compressed result" --label "Recompressed result" %s %s', :'TEST_RESULTS_COMPRESS', :'TEST_RESULTS_RECOMPRESS') as "DIFF_CMD"
\gset

-- Store initial compressed chunk info before recompression
SELECT uncompressed.schema_name || '.' || uncompressed.table_name AS "OLD_CHUNK_NAME",
        cs.compress_relid::regclass::text AS "OLD_COMPRESSED_CHUNK_NAME"
FROM _timescaledb_catalog.chunk uncompressed
JOIN _timescaledb_catalog.compression_settings cs
  ON cs.relid = format('%I.%I', uncompressed.schema_name, uncompressed.table_name)::regclass
WHERE uncompressed.hypertable_id = (
          SELECT id
          FROM _timescaledb_catalog.hypertable
          WHERE table_name = :'TEST_TABLE_NAME'
      )
LIMIT 1 \gset

\set COMPRESSED_CHUNK_NAME :OLD_COMPRESSED_CHUNK_NAME
:BATCH_METADATA_QUERY

\set QUERY1 'SELECT COUNT(*) FROM ' :OLD_CHUNK_NAME ';'
\set QUERY2 'SELECT * FROM ' :OLD_CHUNK_NAME :ORDER_BY_CLAUSE ';'
\o :TEST_RESULTS_COMPRESS
:QUERY1
:QUERY2
\o

-- Recompress the chunk in-memory
SELECT compress_chunk(:'OLD_CHUNK_NAME', recompress := true);

-- Get info for the new compressed chunk
SELECT cs.compress_relid::regclass::text AS "NEW_COMPRESSED_CHUNK_NAME"
FROM _timescaledb_catalog.chunk uncompressed
JOIN _timescaledb_catalog.compression_settings cs
  ON cs.relid = format('%I.%I', uncompressed.schema_name, uncompressed.table_name)::regclass
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
  CASE WHEN :'NEW_COMPRESSED_CHUNK_NAME' IS NULL OR :'OLD_COMPRESSED_CHUNK_NAME' = :'NEW_COMPRESSED_CHUNK_NAME' THEN
    'ERROR: Recompression did not create a new chunk'
  ELSE
    'SUCCESS: New chunk created, old_chunk=' || :'OLD_COMPRESSED_CHUNK_NAME' || ', new_chunk=' || :'NEW_COMPRESSED_CHUNK_NAME'
  END AS recompression_status;

-- Compare result using diff to validate integrity of recompressed data
:DIFF_CMD