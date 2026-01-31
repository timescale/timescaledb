-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test that bulk decompression is automatically disabled for small limits.

EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT time, device_id, v0 FROM metrics_compressed
WHERE device_id = 1
ORDER BY time
LIMIT 5;

EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT time, device_id, v0 FROM metrics_compressed
WHERE device_id = 1
ORDER BY time
OFFSET 5;

EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT time, device_id, v0 FROM metrics_compressed
WHERE device_id = 1
ORDER BY time
LIMIT 0;

EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT time, device_id, v0 FROM metrics_compressed
WHERE device_id = 1
ORDER BY time
OFFSET 0;

EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT time, device_id, v0 FROM metrics_compressed
WHERE device_id = 1
ORDER BY time
LIMIT 10000;

EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT time, device_id, v0 FROM metrics_compressed
WHERE device_id = 1
ORDER BY time
OFFSET 10000;

EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT time, device_id, v0 FROM metrics_compressed
WHERE device_id = 1
ORDER BY time
LIMIT 70 OFFSET 70;

EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT time, device_id, v0 FROM metrics_compressed
WHERE device_id = 1
ORDER BY time
LIMIT 1 OFFSET 1;

EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT time, device_id, v0 FROM metrics_compressed
WHERE device_id = 1
ORDER BY v0
LIMIT 1 OFFSET 1;


EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT time, device_id, v0 FROM metrics_compressed
WHERE device_id = 1
ORDER BY time
LIMIT 1::numeric;

EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT time, device_id, v0 FROM metrics_compressed
WHERE device_id = 1
ORDER BY time
LIMIT 1.1;

EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT time, device_id, v0 FROM metrics_compressed
WHERE device_id = 1
ORDER BY time
LIMIT 1.1::float;


\set ON_ERROR_STOP 0

EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT time, device_id, v0 FROM metrics_compressed
WHERE device_id = 1
ORDER BY time
LIMIT -1;

EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT time, device_id, v0 FROM metrics_compressed
WHERE device_id = 1
ORDER BY time
OFFSET -1;

EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT time, device_id, v0 FROM metrics_compressed
WHERE device_id = 1
ORDER BY time
LIMIT 'nan'::numeric;

\set ON_ERROR_STOP 1
