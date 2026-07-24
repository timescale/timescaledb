-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- test REPACK on hypertable with comrpessed and uncompressed chunks
CREATE TABLE repack_comp(time timestamptz, dev int, val float);
SELECT create_hypertable('repack_comp', 'time', chunk_time_interval => interval '1 day');
ALTER TABLE repack_comp SET (timescaledb.compress, timescaledb.compress_segmentby = 'dev');

INSERT INTO repack_comp
SELECT t, dev, 1.0
FROM generate_series('2020-01-01'::timestamptz, '2020-01-04', '1 hour') t,
     generate_series(1, 3) dev;

-- Compress the first two chunks, leaving the rest as rowstore
SET client_min_messages TO ERROR;
SELECT count(compress_chunk(c)) AS compressed_chunks
FROM (SELECT show_chunks('repack_comp') c ORDER BY 1 LIMIT 2) s;
RESET client_min_messages;

SELECT count(*) AS rows_before FROM repack_comp;

-- Plain REPACK over the mix of compressed and uncompressed chunks
REPACK repack_comp;
SELECT count(*) AS rows_after_plain FROM repack_comp;

-- REPACK ... USING INDEX over the same mix
REPACK repack_comp USING INDEX repack_comp_time_idx;
SELECT count(*) AS rows_after_index FROM repack_comp;

-- Data is still readable after decompression
SELECT dev, count(*) FROM repack_comp GROUP BY dev ORDER BY dev;

DROP TABLE repack_comp;
