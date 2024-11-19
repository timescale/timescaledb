-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

-- Helper function that returns the amount of memory currently allocated in a
-- given memory context.
create or replace function ts_debug_allocated_bytes(text = 'PortalContext') returns bigint
    as :MODULE_PATHNAME, 'ts_debug_allocated_bytes'
    language c strict volatile;


create table mvagg (t int, s int);
select create_hypertable('mvagg', 't', chunk_time_interval => pow(10, 9)::int);
insert into mvagg select -1 t, 1 s; -- need two chunks for chunkwise aggregation
insert into mvagg select generate_series(1, 2 * pow(10, 6)::int) t, 1 s;
alter table mvagg set (timescaledb.compress, timescaledb.compress_segmentby='s');
select count(compress_chunk(x)) from show_chunks('mvagg') x;

-- We are going to log memory usage as a function of number of aggregated elements
-- here.
create table log(n int, bytes int, a bigint, b bigint, c bigint, d bigint, e bigint);


-- Run the vectorized aggregation with grouping by segmentby with various number
-- of input row. We expect approximately constant memory usage.
set timescaledb.debug_require_vector_agg = 'require';

\set ECHO none
select
format('insert into log
    select %1$s,
        ts_debug_allocated_bytes() bytes,
        count(*) a, count(t) b, sum(t) c, avg(t) d, stddev(t) e
            from mvagg where t < %1$s group by s',
    pow(10, generate_series(1, 7)))
\gexec
\set ECHO all

reset timescaledb.debug_require_vector_agg;

select * from log;

select regr_slope(bytes, n), regr_intercept(bytes, n),
    avg(bytes / n),
    regr_slope(bytes, n) / avg(bytes / n),
    regr_slope(bytes, n) / regr_intercept(bytes, n)::float,
    regr_slope(bytes, n) / regr_intercept(bytes, n)::float > 0.05
from log;

select * from log where (
    -- For aggregation by segmentby, memory usage should be constant regardless
    -- of the number of tuples. Still, we have to allow for small variations
    -- that can be caused by other reasons.
    select regr_slope(bytes, n) > 1/65536 from log
);

reset timescaledb.debug_require_vector_agg;
