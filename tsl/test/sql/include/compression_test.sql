-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set ECHO errors

DROP TABLE IF EXISTS compressed;
CREATE TABLE compressed AS SELECT :COMPRESSION_CMD AS c FROM (:QUERY) AS sub;
SELECT pg_column_size(c) as "compressed size" FROM compressed;

--test that decompression gives same result in forward direction
with original AS (
  SELECT row_number() OVER() row_number, * FROM (:QUERY) as q
),
decompressed AS (
  SELECT row_number() OVER () row_number, * FROM (SELECT :DECOMPRESS_FORWARD_CMD FROM compressed) as q
)
SELECT 'Number of rows different between original and decompressed forward (expect 0)', count(*)
FROM original
FULL OUTER JOIN decompressed ON (original.row_number = decompressed.row_number)
WHERE (original.*) IS DISTINCT FROM (decompressed.*);

--test the decompress reverse direction too
with count_orig AS (
  SELECT count(*) as cnt FROM (:QUERY) as q
),
original AS (
  SELECT count_orig.cnt + 1 - row_number() OVER() row_number, q.* FROM (:QUERY) as q, count_orig
),
decompressed AS (
  SELECT row_number() OVER () row_number, * FROM (SELECT :DECOMPRESS_REVERSE_CMD FROM compressed) as q
)
SELECT 'Number of rows different between original and decompressed reversed (expect 0)', count(*)
FROM original
FULL OUTER JOIN decompressed ON (original.row_number = decompressed.row_number)
WHERE (original.*) IS DISTINCT FROM (decompressed.*);

--Test IO
SELECT c "COMPRESSED_AS_TEXT" FROM compressed \gset

WITH original AS
(
  SELECT row_number() OVER() row_number, * FROM (:QUERY) as q
),
decompressed AS
(
  SELECT row_number() OVER () row_number, * FROM (SELECT :DECOMPRESS_FORWARD_CMD FROM compressed) as q
),
decompressed_from_text AS (
  SELECT row_number() OVER () row_number, * FROM (SELECT :DECOMPRESS_FORWARD_CMD FROM (SELECT :'COMPRESSED_AS_TEXT'::text as c) as txt) as q
)
SELECT 'Number of rows different between original, decompressed, and decompressed deserializeed (expect 0)', count(*)
FROM original
FULL OUTER JOIN decompressed ON (original.row_number = decompressed.row_number)
FULL OUTER JOIN decompressed_from_text ON (original.row_number = decompressed_from_text.row_number)
WHERE (original.*) IS DISTINCT FROM (decompressed_from_text.*) OR (decompressed.*) IS DISTINCT FROM (decompressed_from_text.*) ;

SELECT
  'Test that deserialization, decompression, recompression, and serialization results in the same text',
  :COMPRESSION_CMD::text IS NOT DISTINCT FROM :'COMPRESSED_AS_TEXT'
FROM
(SELECT :DECOMPRESS_FORWARD_CMD as item FROM (SELECT :'COMPRESSED_AS_TEXT'::text as c) as txt) as decompressed_serialized;

\set ECHO all
