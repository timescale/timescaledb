-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Tests for parameterized ChunkAppend in ordered mode crashing when creating
-- a MergeAppend because the latter doesn't support parameterization in
-- Postgres. We cover several code paths where this can happen (space
-- partitioning and partially compressed chunks).

\set PREFIX 'EXPLAIN (costs off)'

SET max_parallel_workers_per_gather = 0;
SET enable_material = off;
SET enable_seqscan = off;
SET enable_hashjoin = off;
SET enable_mergejoin = off;

:PREFIX
SELECT m.time, m.device_id, m.v0
FROM devices d
JOIN metrics_compressed m ON m.device_id = d.device_id
WHERE m.time < now()
ORDER BY m.time;


:PREFIX
SELECT m.time, m.device_id, m.v0
FROM devices d
JOIN metrics_space_compressed m ON m.device_id = d.device_id
WHERE m.time < now()
ORDER BY m.time;

RESET enable_material;
RESET enable_seqscan;
RESET enable_hashjoin;
RESET enable_mergejoin;
RESET max_parallel_workers_per_gather;
