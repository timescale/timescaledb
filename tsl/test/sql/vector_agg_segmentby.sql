-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set CHUNKS 2::int
\set CHUNK_ROWS 100000::int
\set GROUPING_CARDINALITY 10::int

create table svagg(t int, f int, s int);
select create_hypertable('svagg', 's', chunk_time_interval => :GROUPING_CARDINALITY / :CHUNKS);

insert into svagg select s * 10000::int + t, (s + t) % 7::int, s from
    generate_series(1::int, :CHUNK_ROWS * :CHUNKS / :GROUPING_CARDINALITY) t,
    -- (values (1), (10)) s(s)
    generate_series(0::int, :GROUPING_CARDINALITY - 1::int) s(s)
;

alter table svagg set (timescaledb.compress, timescaledb.compress_orderby = 't',
    timescaledb.compress_segmentby = 's');

select count(compress_chunk(x)) from show_chunks('svagg') x;

analyze svagg;

set max_parallel_workers_per_gather = 0;


-- Check that the debug GUC actually works.
\set ON_ERROR_STOP 0

set timescaledb.debug_require_vector_agg = 'require';
set timescaledb.enable_vectorized_aggregation to off;
select sum(t) from svagg;

set timescaledb.debug_require_vector_agg = 'forbid';
set timescaledb.enable_vectorized_aggregation to off;
select sum(t) from svagg;

set timescaledb.debug_require_vector_agg = 'forbid';
set timescaledb.enable_vectorized_aggregation to on;
select sum(t) from svagg;

set timescaledb.debug_require_vector_agg = 'require';
set timescaledb.enable_vectorized_aggregation to on;
select sum(t) from svagg;

set timescaledb.debug_require_vector_agg = 'allow';
set timescaledb.enable_vectorized_aggregation to on;
select sum(t) from svagg;

\set ON_ERROR_STOP 1


set timescaledb.debug_require_vector_agg = 'require';
---- Uncomment to generate reference
--set timescaledb.debug_require_vector_agg = 'forbid';
--set timescaledb.enable_vectorized_aggregation to off;

select s, sum(t), count(*) from svagg where f >= 0         group by s order by s;
select s, sum(t), count(*) from svagg where f = 0          group by s order by s;
select s, sum(t), count(*) from svagg where f in (0, 1)    group by s order by s;
select s, sum(t), count(*) from svagg where f in (0, 1, 3) group by s order by s;
select s, sum(t), count(*) from svagg where f > 10         group by s order by s;

select s, sum(t), count(*) from svagg group by s order by s;

-- another example that we used not to vectorize because of the projection.
explain (costs off)
select sum(t), s, count(*) from svagg group by s order by s;

select sum(t) from svagg group by s order by 1;


drop table svagg;
