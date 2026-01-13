-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set PREFIX 'EXPLAIN (analyze, buffers off, costs off, timing off, summary off)'
\set PREFIX_NO_ANALYZE 'EXPLAIN (verbose, buffers off, costs off)'
SET parallel_leader_participation TO off;
SET min_parallel_table_scan_size TO '0';
SET enable_incremental_sort TO off;
-- Test lateral query for partial chunk #8848

:PREFIX
SELECT *
FROM metrics subq_1,
    LATERAL (
        SELECT *
        FROM (
            SELECT subq_1.*
            FROM metrics_compressed) AS subq_2
            RIGHT JOIN sensors ON FALSE) AS lat;

-- with parallel

SET max_parallel_workers_per_gather = 4;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;

:PREFIX_NO_ANALYZE
SELECT *
FROM metrics subq_1,
    LATERAL (
        SELECT *
        FROM (
            SELECT subq_1.*
            FROM metrics_compressed) AS subq_2
            RIGHT JOIN sensors ON FALSE) AS lat;
--  now run the query to catch any coredump
\o /dev/null
SELECT *
FROM metrics subq_1,
    LATERAL (
        SELECT *
        FROM (
            SELECT subq_1.*
            FROM metrics_compressed) AS subq_2
            RIGHT JOIN sensors ON FALSE) AS lat;
\o
