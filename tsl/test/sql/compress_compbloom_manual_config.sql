-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

---------------------------------------------------------------------
-- Manual config change between chunk compressions
---------------------------------------------------------------------
CREATE TABLE mixed_avail_manual(ts timestamptz, a int, b int, c int, seg int);
SELECT create_hypertable('mixed_avail_manual', by_range('ts', interval '1 day'));

-- Initial config: bloom(a,b)
ALTER TABLE mixed_avail_manual SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'seg',
    timescaledb.compress_orderby = 'ts',
    timescaledb.compress_index = 'bloom(a,b)'
);

INSERT INTO mixed_avail_manual
SELECT ts, (i % 10), (i % 5), (i % 3), 1
FROM generate_series('2024-01-01'::timestamptz, '2024-01-03', interval '1 hour') ts,
     generate_series(1, 100) i;

-- Compress first chunk with bloom(a,b)
SELECT compress_chunk(c) FROM show_chunks('mixed_avail_manual') c LIMIT 1;

SELECT COUNT(*) FROM mixed_avail_manual WHERE a = 1 AND b = 2;
SELECT COUNT(*) FROM mixed_avail_manual WHERE a = 1 AND c = 2;
SELECT COUNT(*) FROM mixed_avail_manual WHERE b = 1 AND c = 2;

-- Before config changes
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT * FROM mixed_avail_manual WHERE a = 1 AND b = 2
ORDER BY 1,2,3,4;

EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT * FROM mixed_avail_manual WHERE a = 1 AND c = 2
ORDER BY 1,2,3,4;

EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT * FROM mixed_avail_manual WHERE b = 1 AND c = 2
ORDER BY 1,2,3,4;

-- Change config: bloom(a,c)
ALTER TABLE mixed_avail_manual SET (
    timescaledb.compress_index = 'bloom(a,c)'
);

-- Compress second chunk with bloom(a,c)
SELECT compress_chunk(c) FROM show_chunks('mixed_avail_manual') c OFFSET 1 LIMIT 1;

-- Change config: bloom(b,c)
ALTER TABLE mixed_avail_manual SET (
    timescaledb.compress_index = 'bloom(b,c)'
);

-- Compress third chunk with bloom(b,c)
SELECT compress_chunk(c) FROM show_chunks('mixed_avail_manual') c OFFSET 2;

SELECT COUNT(*) FROM mixed_avail_manual WHERE a = 1 AND b = 2;
SELECT COUNT(*) FROM mixed_avail_manual WHERE a = 1 AND c = 2;
SELECT COUNT(*) FROM mixed_avail_manual WHERE b = 1 AND c = 2;

-- After config changes
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT * FROM mixed_avail_manual WHERE a = 1 AND b = 2
ORDER BY 1,2,3,4;

EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT * FROM mixed_avail_manual WHERE a = 1 AND c = 2
ORDER BY 1,2,3,4;

EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT * FROM mixed_avail_manual WHERE b = 1 AND c = 2
ORDER BY 1,2,3,4;

DROP TABLE IF EXISTS mixed_avail_manual CASCADE;
