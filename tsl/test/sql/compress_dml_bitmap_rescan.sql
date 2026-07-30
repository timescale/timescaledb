-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Check the proper handling of Bitmap Heap Scan under compressed DML.

DROP TABLE IF EXISTS partial_bhs_delete_returning_crash CASCADE;
CREATE TABLE partial_bhs_delete_returning_crash (id int NOT NULL, ts timestamptz NOT NULL);
SELECT create_hypertable('partial_bhs_delete_returning_crash', 'ts');
ALTER TABLE partial_bhs_delete_returning_crash SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'id'
);

-- compressed batch outside the DELETE qual
INSERT INTO partial_bhs_delete_returning_crash
SELECT 1, '2026-04-15 00:00'::timestamptz + (g * interval '5 min')
FROM generate_series(0, 11) g;
SELECT compress_chunk(c) FROM show_chunks('partial_bhs_delete_returning_crash') c;

-- uncompressed rows inside the DELETE qual, in the same chunk
INSERT INTO partial_bhs_delete_returning_crash
SELECT 1, '2026-04-15 06:00'::timestamptz + (g * interval '5 min')
FROM generate_series(0, 11) g;

-- used to crash
SET enable_indexscan = off;
SET enable_seqscan = off;

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF)
DELETE FROM partial_bhs_delete_returning_crash
WHERE id = 1 AND ts >= '2026-04-15 06:00' AND ts <  '2026-04-15 07:00'
RETURNING id, ts
;

SELECT COUNT(*) FROM partial_bhs_delete_returning_crash
WHERE id = 1 AND ts >= '2026-04-15 06:00' AND ts <  '2026-04-15 07:00'
;
