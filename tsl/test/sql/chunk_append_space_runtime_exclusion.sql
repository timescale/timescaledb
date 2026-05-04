-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test runtime chunk exclusion in an ordered ChunkAppend with space
-- partitioning. When some time slices have multiple space partitions
-- (producing MergeAppend children) and others have a single partition
-- (producing direct scan children), the ChunkAppend must handle both
-- child types during runtime exclusion.

\set PREFIX 'EXPLAIN (COSTS OFF)'
\set PREFIX_ANALYZE 'EXPLAIN (COSTS OFF, ANALYZE, TIMING OFF, BUFFERS OFF, SUMMARY OFF)'

SET max_parallel_workers_per_gather = 0;
SET enable_material = off;
SET enable_seqscan = off;
SET enable_hashjoin = off;
SET enable_mergejoin = off;

CREATE TABLE space_mixed(time timestamptz NOT NULL, device_id int NOT NULL, value float);
SELECT create_hypertable('space_mixed', 'time', 'device_id',
    number_partitions => 2,
    chunk_time_interval => '5 days'::interval);

-- Row counts and LIMIT below are chosen so EXPLAIN ANALYZE per-loop
-- averages are integers; otherwise PG versions differ in how they
-- format half-integer values.
-- Time slice 1: both space partitions have data (produces MergeAppend).
INSERT INTO space_mixed VALUES
  ('2024-01-01 01:00', 1, 1.0), ('2024-01-01 02:00', 1, 2.0),
  ('2024-01-02 01:00', 3, 3.0), ('2024-01-02 02:00', 3, 4.0);

-- Time slice 2: only one space partition has data (produces direct scan).
INSERT INTO space_mixed VALUES
  ('2024-01-06 01:00', 1, 5.0), ('2024-01-06 02:00', 1, 6.0),
  ('2024-01-07 01:00', 1, 7.0), ('2024-01-07 02:00', 1, 8.0);

CREATE INDEX ON space_mixed(time);
ANALYZE space_mixed;

-- The driver table provides join parameters for runtime exclusion.
CREATE TABLE driver_times(t timestamptz PRIMARY KEY);
INSERT INTO driver_times VALUES ('2024-01-01'), ('2024-01-06');

-- Plan should show ordered ChunkAppend with mixed children:
-- MergeAppend for the first time slice, direct IndexScan for the second.
:PREFIX
SELECT d.t, m.*
FROM driver_times d,
LATERAL (SELECT * FROM space_mixed m WHERE m.time >= d.t ORDER BY m.time LIMIT 4) m;

-- Execute to trigger runtime exclusion. The MergeAppend child is
-- skipped by do_runtime_exclusion (scan == NULL path), while the
-- direct scan child participates in constraint exclusion.
:PREFIX_ANALYZE
SELECT d.t, m.*
FROM driver_times d,
LATERAL (SELECT * FROM space_mixed m WHERE m.time >= d.t ORDER BY m.time LIMIT 4) m;

SELECT d.t, m.*
FROM driver_times d,
LATERAL (SELECT * FROM space_mixed m WHERE m.time >= d.t ORDER BY m.time LIMIT 4) m;

DROP TABLE space_mixed;
DROP TABLE driver_times;

RESET max_parallel_workers_per_gather;
RESET enable_material;
RESET enable_seqscan;
RESET enable_hashjoin;
RESET enable_mergejoin;
