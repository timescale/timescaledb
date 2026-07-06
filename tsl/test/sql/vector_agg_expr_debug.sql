-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Debug-only tests for CSE (common subexpression elimination) in vectorized
-- aggregation. Verifies that EXPLAIN VERBOSE shows the correct set of cached
-- subexpressions. Requires assert-enabled (debug) builds because the
-- "Cached Subexpressions" EXPLAIN output is guarded by #ifndef NDEBUG.

\c :TEST_DBNAME :ROLE_SUPERUSER

set timescaledb.enable_columnarindexscan = off;

create table cse_explain(ts int, i int, x text, b bool, v float4)
    with (tsdb.hypertable, tsdb.compress, tsdb.compress_orderby = 'ts',
        tsdb.compress_segmentby = 'i, x, b',
        tsdb.partition_column = 'ts', tsdb.chunk_interval = 100000);

insert into cse_explain
    select g, g % 5, 'val' || (g % 3)::text, g % 2 = 0, g::float4
    from generate_series(1, 1000) g;

select count(compress_chunk(x)) from show_chunks('cse_explain') x;

set max_parallel_workers_per_gather = 0;
set timescaledb.debug_require_vector_agg = 'require';

-- Same expression in two aggregates: one cached subexpression.
explain (verbose, costs off)
select sum(abs(v - 500)), count(abs(v - 500)) from cse_explain;

-- Two distinct shared expressions cached simultaneously.
explain (verbose, costs off)
select sum(abs(v - 500)), count(abs(v - 500)),
       sum(length(x)), count(length(x)) from cse_explain;

-- Shared expression with segmentby grouping column.
explain (verbose, costs off)
select b, sum(abs(v - 500)), count(abs(v - 500))
    from cse_explain group by b;

-- FILTER aggregate excluded from CSE, but non-FILTER aggregates still share.
explain (verbose, costs off)
select sum(abs(v - 500)) filter (where b),
       count(abs(v - 500)),
       sum(abs(v - 500))
    from cse_explain;

-- No common subexpressions: no "Cached Subexpressions" line.
explain (verbose, costs off)
select sum(v), count(i) from cse_explain;

-- Two shared sibling subexpressions in a single expression argument.
explain (verbose, costs off)
select sum(abs(v - 500) + length(x)), sum(abs(v - 500) - length(x))
    from cse_explain;

-- Three aggregates sharing one expression (refcount = 3).
explain (verbose, costs off)
select sum(abs(v - 500)), count(abs(v - 500)), min(abs(v - 500))
    from cse_explain;

-- Cached boolean subexpression consumed as function argument.
explain (verbose, costs off)
select sum((b = (i > 0))::int), count(b = (i > 0)) from cse_explain;

reset timescaledb.debug_require_vector_agg;

drop table cse_explain;
