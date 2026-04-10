-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

---------------------------------------------------------------------
-- Index added after partial compression
---------------------------------------------------------------------

CREATE TABLE mixed_avail_add(ts timestamptz, a int, b int, seg int);
SELECT create_hypertable('mixed_avail_add', by_range('ts', interval '1 day'));

ALTER TABLE mixed_avail_add SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'seg',
    timescaledb.compress_orderby = 'ts'
);

-- Insert and compress first 2 chunks (no composite bloom)
INSERT INTO mixed_avail_add
SELECT ts, (i % 10), (i % 5), 1
FROM generate_series('2024-01-01'::timestamptz, '2024-01-03', interval '1 hour') ts,
     generate_series(1, 100) i;

SELECT compress_chunk(c) FROM show_chunks('mixed_avail_add') c LIMIT 2;

-- Before index add
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT * FROM mixed_avail_add WHERE a = 1 AND b = 2
ORDER BY 1,2,3,4;

-- Create index
CREATE INDEX idx_ab ON mixed_avail_add (a, b);

-- Compress last chunk (will have composite bloom from new index)
SELECT compress_chunk(c) FROM show_chunks('mixed_avail_add') c OFFSET 2;

-- Query should work on all chunks
SELECT COUNT(*) FROM mixed_avail_add WHERE a = 1 AND b = 2;

-- After index add
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT * FROM mixed_avail_add WHERE a = 1 AND b = 2
ORDER BY 1,2,3,4;

DROP TABLE IF EXISTS mixed_avail_add CASCADE;
