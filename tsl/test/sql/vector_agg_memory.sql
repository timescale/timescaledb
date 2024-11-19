-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

-- Helper function that returns the amount of memory currently allocated in a
-- given memory context.
create or replace function ts_debug_allocated_bytes(text = 'PortalContext') returns bigint
    as :MODULE_PATHNAME, 'ts_debug_allocated_bytes'
    language c strict volatile;


create table mvagg (t int, s0 int, s1 int);
select create_hypertable('mvagg', 't', chunk_time_interval => pow(10, 9)::int);
insert into mvagg select generate_series(1, 2 * pow(10, 6)::int) t, 1 s0, 1 s1;

-- Need two segmentbys to prevent compressed index scans to force hash aggregation.
-- Otherwise we might get GroupAggregate with Sort which uses linear memory in
-- the number of compressed batches.
alter table mvagg set (timescaledb.compress, timescaledb.compress_segmentby='s0, s1');
-- Need to inflate the estimated cardinalities of segmentby columns, again to
-- force the hash aggregation.
insert into mvagg select -1 - x t, -x s0, -x s1 from generate_series(1, 1000) x;
-- Need two chunks for chunkwise aggregation.
insert into mvagg select -1 t, 1 s0, 1 s1;

select count(compress_chunk(x)) from show_chunks('mvagg') x;

-- We are going to log memory usage as a function of number of aggregated elements
-- here.
create table log(n int, bytes int, a bigint, b bigint, c bigint, d bigint, e bigint);


-- First, ensure that the underlying decompression has constant memory usage.
explain (costs off) select distinct on (s0, s1) ts_debug_allocated_bytes() bytes,
        s0, s1, t
    from mvagg where t >= -1 and t < 1000000 order by s0, s1, t desc;

truncate log;
\set ECHO none
select
format('insert into log
    select distinct on (s0, s1) %1$s,
        ts_debug_allocated_bytes() bytes,
        0 a, 0 b, 0 c, 0 d, 0 e
    from mvagg where t >= -1 and t < %1$s
    order by s0, s1, t desc',
    pow(10, generate_series(1, 7)))
\gexec
\set ECHO all

select * from log where (
    -- Ideally the memory usage should be constant, but we have to allow for
    -- small spurious changes to make this test more robust.
    select regr_slope(bytes, n) > 1/65536 from log
);

-- Test the vectorized aggregation with grouping by segmentby with various number
-- of input row. We expect approximately constant memory usage.
truncate log;
set max_parallel_workers_per_gather = 0;
set timescaledb.debug_require_vector_agg = 'require';
-- Despite the tweaks above, we are unable to force the HashAggregation, because
-- the unsorted DecompressChunk paths for aggregation are not created properly
-- (see issue #6836). Limit the memory consumed by tuplesort.
set work_mem = '64kB';

explain (costs off) select ts_debug_allocated_bytes() bytes,
        count(*) a, count(t) b, sum(t) c, avg(t) d, stddev(t) e
            from mvagg where t >= -1 and t < 1000000 group by s1;

\set ECHO none
select
format('insert into log
    select %1$s,
        ts_debug_allocated_bytes() bytes,
        count(*) a, count(t) b, sum(t) c, avg(t) d, stddev(t) e
    from mvagg where t >= -1 and t < %1$s group by s1',
    pow(10, generate_series(1, 7)))
\gexec
\set ECHO all

reset timescaledb.debug_require_vector_agg;
reset max_parallel_workers_per_gather;
reset work_mem;

select * from log where (
    -- For aggregation by segmentby, memory usage should be constant regardless
    -- of the number of tuples. Still, we have to allow for small variations
    -- that can be caused by other reasons. Currently the major increase is
    -- caused by tuplesort, because we are unable to force hash aggregation due
    -- to unrelated planning bugs.
    select regr_slope(bytes, n) > 0.05 from log
);

reset timescaledb.debug_require_vector_agg;
