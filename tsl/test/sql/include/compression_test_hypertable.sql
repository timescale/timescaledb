-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set ECHO errors

DROP TABLE IF EXISTS original_result;


CREATE TABLE original_result AS :QUERY;

SELECT count(compress_chunk(c, true)) AS "count compress" FROM show_chunks(:'HYPERTABLE_NAME') c;

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
  SELECT row_number() OVER() row_number, * FROM (SELECT * FROM original_result :QUERY_ORDER) qo
),
decompressed AS (
  SELECT row_number() OVER() row_number, * FROM (:QUERY :QUERY_ORDER) as q
)
select original, decompressed
FROM original
FULL OUTER JOIN decompressed ON (original.row_number = decompressed.row_number)
WHERE (original.*) IS DISTINCT FROM (decompressed.*);

SELECT count(decompress_chunk(c)) AS "count decompress" FROM show_chunks(:'HYPERTABLE_NAME') c;

--run data on data that's been compressed and decompressed, make sure it's the same.
with original AS (
  SELECT row_number() OVER() row_number, * FROM (SELECT * FROM original_result :QUERY_ORDER) qo
),
uncompressed AS (
  SELECT row_number() OVER() row_number, * FROM (:QUERY :QUERY_ORDER) as q
)
SELECT :'HYPERTABLE_NAME' AS table, count(*) AS "diff between original and compressed/decompressed"
FROM original
FULL OUTER JOIN uncompressed ON (original.row_number = uncompressed.row_number)
WHERE (original.*) IS DISTINCT FROM (uncompressed.*);

\set ECHO all
