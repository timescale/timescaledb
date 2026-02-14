-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Expressions in vectorized aggregation.

\c :TEST_DBNAME :ROLE_SUPERUSER

\pset null $

create function always_null(x int4) returns int4 as $$ select null::int4 $$
language sql strict immutable parallel safe;

create table aggexpr(ts int, i int, x text, b bool, v float4) with (tsdb.hypertable,
    tsdb.compress, tsdb.compress_orderby = 'ts', tsdb.compress_segmentby = 'i, x, b',
    tsdb.partition_column = 'ts', tsdb.chunk_interval = 10);

insert into aggexpr select 1, null, null, null, generate_series(1, 1499);

insert into aggexpr select 2, 2, '2', false, generate_series(1, 1493);

-- Batch divisible by 64 to test proper padding.
insert into aggexpr select 3, case when x % 3 = 1 then 3 else 0 end,
    case when x % 3 = 2 then '3' else '0' end,
    case when x % 3 = 0 then true else false end
from generate_series(1, 960) x;

select count(compress_chunk(x)) from show_chunks('aggexpr') x;

alter table aggexpr set (tsdb.compress_segmentby = '');

insert into aggexpr select 11, null, null, null, generate_series(1, 1489);

insert into aggexpr select 12, 12, '12', false, generate_series(1, 1487);

insert into aggexpr select 13, case when x % 2 = 0 then 13 else null end,
    case when x % 2 = 1 then '13' else null end, x % 3 = 0, x from generate_series(1, 1483) x;

insert into aggexpr select 14, case when x % 2 = 0 then 14 else null end,
    case when x % 2 = 0 then '14' || x::text else null end, x % 3 = 1, x from generate_series(1, 1481) x;

select count(compress_chunk(x)) from show_chunks('aggexpr') x;

vacuum full analyze aggexpr;

-- The batch sorted merge has very close costs for one query, and prevents
-- vectorized aggregation, so we have to get it out of the way.
set timescaledb.enable_decompression_sorted_merge to off;


-- Some functions we are not able to vectorize at the moment.
set timescaledb.debug_require_vector_agg = 'forbid';

-- Volatile expression
select sum((b and random() < 0.0)::int) from aggexpr;

-- Non-strict expression
select sum(length(format('%s', x))) from aggexpr;

-- No columnar representation for the expression (numeric)
select sum(factorial(i % 2)) from aggexpr;

reset timescaledb.debug_require_vector_agg;


-- Test some functions that are vectorizable.
set timescaledb.debug_require_vector_agg = 'require';
-- /* Uncomment to generate reference. */ set timescaledb.debug_require_vector_agg = 'forbid'; set timescaledb.enable_vectorized_aggregation to off;


select always_null(i) from aggexpr group by 1;

select
    format('select %s%s from aggexpr%s%s%s;',
            grouping || ', ',
            function,
            ' where ' || condition,
            ' group by ' || grouping,
            format(' order by %s, ', function) || grouping || ' limit 10')
from
    unnest(array[
        'count(*)'
        , 'count(i)'
        , 'count(x)'
        , 'count(b)'
        , 'sum((i = 12)::int)'
        , 'sum(abs(v - 500))'
        ]) with ordinality as function(function, n),
    unnest(array[
        null
        , 'b'
        , 'not b'
        , 'length(x) = 1'
        , 'length(x) < 0'
        , 'i % 2 = 0'
        ]) with ordinality as condition(condition, n),
    unnest(array[
        null
        , 'length(x)'
        , 'lower(x)'
        , 'i % 2'
        , 'ts'
        , 'b'
        , 'v - 501 > 0'
        ]) with ordinality as grouping(grouping, n)
order by grouping.n, condition.n, function.n
\gexec

reset timescaledb.debug_require_vector_agg;
reset timescaledb.enable_vectorized_aggregation;

reset timescaledb.enable_decompression_sorted_merge;
