-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set ECHO errors

TRUNCATE TABLE compressed;
CREATE TABLE tmp AS SELECT * FROM :"DATA_IN";
SELECT ts_compress_table(:'DATA_IN'::REGCLASS, 'compressed'::REGCLASS, :'COMPRESSION_INFO'::_timescaledb_catalog.hypertable_compression[]);
--compression truncates the DATA_IN table, restore the data
INSERT INTO :"DATA_IN" SELECT * FROM tmp;
DROP TABLE tmp;

--test that decompression gives same result in forward direction
WITH original AS (
  SELECT * FROM :DATA_OUT
),
decompressed AS (
  SELECT * FROM (SELECT :DECOMPRESS_FORWARD_CMD FROM compressed ORDER BY device) AS q
)
SELECT 'Number of rows different between original and decompressed forward (expect 0)', count(*)
FROM original
FULL OUTER JOIN decompressed ON (original.device = decompressed.device AND original.time = decompressed.t)
WHERE (original.*) IS DISTINCT FROM (decompressed.*);

WITH original AS (
  SELECT * FROM :DATA_OUT AS q
),
decompressed AS (
  SELECT * FROM (SELECT :DECOMPRESS_FORWARD_CMD FROM compressed) AS q
)
SELECT *
FROM original
FULL OUTER JOIN decompressed ON (original.device = decompressed.device AND original.time = decompressed.t)
WHERE (original.*) IS DISTINCT FROM (decompressed.*);

-- test that decompress_table works
CREATE TABLE decompressed_table AS SELECT * FROM :DATA_OUT LIMIT 0;

SELECT ts_decompress_table('compressed'::REGCLASS, 'decompressed_table'::REGCLASS);

WITH original AS (
  SELECT * FROM :DATA_OUT
),
decompressed AS (
  SELECT * FROM decompressed_table
)
SELECT 'Number of rows different between original and decompress_table (expect 0)', count(*)
FROM original
FULL OUTER JOIN decompressed ON (original.device = decompressed.device AND original.time = decompressed.time)
WHERE (original.*) IS DISTINCT FROM (decompressed.*);

WITH original AS (
  SELECT * FROM :DATA_OUT
),
decompressed AS (
  SELECT * FROM decompressed_table
)
SELECT *
FROM original
FULL OUTER JOIN decompressed ON (original.device = decompressed.device AND original.time = decompressed.time)
WHERE (original.*) IS DISTINCT FROM (decompressed.*);

DROP TABLE decompressed_table;

\set ECHO all
