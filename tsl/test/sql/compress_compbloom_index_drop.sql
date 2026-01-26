-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

---------------------------------------------------------------------
-- Some chunks don't have a composite bloom index after index change
---------------------------------------------------------------------

CREATE TABLE mixed_avail_drop(ts timestamptz, a int, b int, seg int);
SELECT create_hypertable('mixed_avail_drop', by_range('ts', interval '1 day'));

-- Create index
CREATE INDEX idx_ab ON mixed_avail_drop (a, b);

ALTER TABLE mixed_avail_drop SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'seg',
    timescaledb.compress_orderby = 'ts'
);

-- Insert data for 3 days (3 chunks)
INSERT INTO mixed_avail_drop
SELECT ts, (i % 10), (i % 5), 1
FROM generate_series('2024-01-01'::timestamptz, '2024-01-03', interval '1 hour') ts,
     generate_series(1, 100) i;

-- Compress first 2 chunks (will have composite bloom from index)
SELECT compress_chunk(c) FROM show_chunks('mixed_avail_drop') c LIMIT 2;

-- Before index drop
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT * FROM mixed_avail_drop WHERE a = 1 AND b = 2
ORDER BY 1,2,3,4;

-- Drop index
DROP INDEX idx_ab;

-- Compress last chunk (no composite bloom, index is gone)
SELECT compress_chunk(c) FROM show_chunks('mixed_avail_drop') c OFFSET 2;

-- Query should work on all chunks
SELECT COUNT(*) FROM mixed_avail_drop WHERE a = 1 AND b = 2;

-- After index drop
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT * FROM mixed_avail_drop WHERE a = 1 AND b = 2
ORDER BY 1,2,3,4;

DROP TABLE IF EXISTS mixed_avail_drop CASCADE;
