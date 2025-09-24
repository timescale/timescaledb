-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Expressions in vectorized aggregation.

\pset null $


create table aggexpr(ts int, i int, x text, b bool, v float4) with (tsdb.hypertable,
    tsdb.compress, tsdb.compress_orderby = 'ts', tsdb.compress_segmentby = 'i, x, b',
    tsdb.partition_column = 'ts', tsdb.chunk_interval = 10);

insert into aggexpr select 1, null, null, null, generate_series(1, 1499);

insert into aggexpr select 2, 2, '2', false, generate_series(1, 1493);

select count(compress_chunk(x)) from show_chunks('aggexpr') x;

alter table aggexpr set (tsdb.compress_segmentby = '');

insert into aggexpr select 11, null, null, null, generate_series(1, 1489);

insert into aggexpr select 12, 12, '12', false, generate_series(1, 1487);

insert into aggexpr select 13, case when x % 2 = 0 then 13 else null end,
    case when x % 2 = 1 then '13' else null end, x % 2 = 0, x from generate_series(1, 1483) x;

insert into aggexpr select 14, case when x % 2 = 0 then 14 else null end,
    case when x % 2 = 0 then '14' || x::text else null end, x % 2 = 1, x from generate_series(1, 1481) x;

select count(compress_chunk(x)) from show_chunks('aggexpr') x;

vacuum full analyze aggexpr;


-- FIXME
--set timescaledb.debug_require_vector_agg = 'require';
---- Uncomment to generate reference
--set timescaledb.debug_require_vector_agg = 'forbid'; set timescaledb.enable_vectorized_aggregation to off;

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
        ]) function,
    unnest(array[
        null
        , 'b'
        , 'not b'
        , 'length(x) = 1'
        , 'length(x) < 0'
        , 'i % 2 = 0'
        ]) with ordinality as condition(condition, n),
    unnest(array[
        null,
        'length(x)',
        'i % 2',
        'ts'
        ]) with ordinality as grouping(grouping, n)
\gexec

reset timescaledb.debug_require_vector_agg;
