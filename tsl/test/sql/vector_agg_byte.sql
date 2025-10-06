-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Tests for grouping by single-byte data types. Only bool for now, because char
-- lacks a vectorized representation.

create table groupbyte(ts int, b bool, x text, i int)
    with (tsdb.hypertable, tsdb.partition_column = 'ts', tsdb.chunk_interval = '10',
        tsdb.compress, tsdb.compress_segmentby = 'b', tsdb.compress_orderby = ts);

insert into groupbyte select 1,
    case when x % 11 = 0 then null else x % 3 = 0 end,
    case when x % 13 = 0 then null else 'tag' || (x % 7) end,
    x
from generate_series(1, 1000) x;

select count(compress_chunk(x)) from show_chunks('groupbyte') x;

alter table groupbyte set (tsdb.compress_segmentby = '');

insert into groupbyte select 11,
    case when x % 11 = 0 then null else x % 3 = 0 end,
    case when x % 13 = 0 then null else 'tag' || (x % 7) end,
    x
from generate_series(1, 1000) x;

select count(compress_chunk(x)) from show_chunks('groupbyte') x;

set timescaledb.debug_require_vector_agg = 'require';
-- Uncomment to generate reference.
--set timescaledb.enable_vectorized_aggregation to off; set timescaledb.debug_require_vector_agg = 'allow';

select
    format('select %s%s from groupbyte%s%s;',
            grouping || ', ',
            function,
            ' group by ' || grouping,
            ' order by ' || function || ', ' || grouping || ' limit 10')
from
    unnest(array[
        'count(*)'
        , 'count(b)'
        , 'sum(i)'
    ]) function,
    unnest(array[
        null
        , 'b'
        , 'b, x'
        , 'x, b'
    ]) with ordinality as grouping(grouping, n)
order by function, grouping.n
\gexec

reset timescaledb.debug_require_vector_agg;
