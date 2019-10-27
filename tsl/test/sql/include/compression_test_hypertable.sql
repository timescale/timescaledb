-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set ECHO errors

DROP TABLE IF EXISTS original_result;


CREATE TABLE original_result AS :QUERY;

SELECT count(compress_chunk(chunk.schema_name|| '.' || chunk.table_name)) as count_compressed
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable hypertable ON (chunk.hypertable_id = hypertable.id)
WHERE hypertable.table_name like :'HYPERTABLE_NAME' and chunk.compressed_chunk_id IS NULL;

--dump & restore while data is in compressed state.
\c postgres :ROLE_SUPERUSER
SET client_min_messages = ERROR;
\! utils/pg_dump_aux_dump.sh dump/pg_dump.sql

\c :TEST_DBNAME
SET client_min_messages = ERROR;
CREATE EXTENSION timescaledb CASCADE;
RESET client_min_messages;

--\! cp dump/pg_dump.sql /tmp/dump.sql
SELECT timescaledb_pre_restore();
\! utils/pg_dump_aux_restore.sh dump/pg_dump.sql
SELECT timescaledb_post_restore();
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

with original AS (
  SELECT row_number() OVER() row_number, * FROM original_result
),
decompressed AS (
  SELECT row_number() OVER() row_number, * FROM (:QUERY) as q
)
SELECT 'Number of rows different between original and query on compressed data (expect 0)', count(*)
FROM original
FULL OUTER JOIN decompressed ON (original.row_number = decompressed.row_number)
WHERE (original.*) IS DISTINCT FROM (decompressed.*);

SELECT count(decompress_chunk(chunk.schema_name|| '.' || chunk.table_name)) as count_decompressed
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
SELECT 'Number of rows different between original and data that has been compressed and then decompressed (expect 0)', count(*)
FROM original
FULL OUTER JOIN uncompressed ON (original.row_number = uncompressed.row_number)
WHERE (original.*) IS DISTINCT FROM (uncompressed.*);

\set ECHO all
