-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Check that the vectorized aggregation works properly in the GroupAggregate
-- mode.

create table groupagg(t int, s text, value int);
select create_hypertable('groupagg', 't', chunk_time_interval => 10000);

insert into groupagg
select
    xfast * 100 + xslow,
    case when xfast = 13 then null else xfast end,
    xfast * 7 + xslow * 3
from generate_series(10, 99) xfast,
    generate_series(1, 10000) xslow
;

alter table groupagg set (timescaledb.compress, timescaledb.compress_segmentby = '',
    timescaledb.compress_orderby = 's');
select count(compress_chunk(x)) from show_chunks('groupagg') x;

set enable_hashagg to off;
set timescaledb.debug_require_vector_agg to 'allow';
select s, sum(value) from groupagg group by s order by s limit 10;


reset timescaledb.debug_require_vector_agg;
select count(decompress_chunk(x)) from show_chunks('groupagg') x;
alter table groupagg set (timescaledb.compress, timescaledb.compress_segmentby = '',
    timescaledb.compress_orderby = 's nulls first');
select count(compress_chunk(x)) from show_chunks('groupagg') x;

set timescaledb.debug_require_vector_agg to 'require';
select s , sum(value) from groupagg group by s  order by s  nulls first limit 10;


reset enable_hashagg;
reset timescaledb.debug_require_vector_agg;
