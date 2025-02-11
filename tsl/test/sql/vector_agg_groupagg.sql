-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Check that the vectorized aggregation works properly in the GroupAggregate
-- mode.

\pset null $

set max_parallel_workers_per_gather = 0;
set enable_hashagg to off;

--set timescaledb.enable_vectorized_aggregation to off;
--set timescaledb.debug_require_vector_agg to 'forbid';

create table groupagg(t int, s int, value int);
select create_hypertable('groupagg', 't', chunk_time_interval => 10000);

insert into groupagg
select
    xslow * 10,
    case when xfast = 13 then null else xfast end,
    xfast * 7 + xslow * 3
from generate_series(10, 99) xfast,
    generate_series(1, 1487) xslow
;

alter table groupagg set (timescaledb.compress, timescaledb.compress_segmentby = '',
    timescaledb.compress_orderby = 's');
select count(compress_chunk(x)) from show_chunks('groupagg') x;
vacuum analyze groupagg;


set timescaledb.debug_require_vector_agg to 'require';
select s, sum(value) from groupagg group by s order by s limit 10;

-- The hash grouping policies do not support the GroupAggregate mode in the
-- reverse order.
set timescaledb.debug_require_vector_agg to 'forbid';
select s, sum(value) from groupagg group by s order by s desc limit 10;
reset timescaledb.debug_require_vector_agg;

-- Also test the NULLS FIRST order.
select count(decompress_chunk(x)) from show_chunks('groupagg') x;
alter table groupagg set (timescaledb.compress, timescaledb.compress_segmentby = '',
    timescaledb.compress_orderby = 's nulls first');
select count(compress_chunk(x)) from show_chunks('groupagg') x;
vacuum analyze groupagg;

set timescaledb.debug_require_vector_agg to 'require';
select s, sum(value) from groupagg group by s order by s nulls first limit 10;
reset timescaledb.debug_require_vector_agg;


-- More tests for dictionary encoding. This is not vectorized at the moment.
set timescaledb.debug_require_vector_agg to 'forbid';
create table text_table(ts int);
select create_hypertable('text_table', 'ts', chunk_time_interval => 3);
alter table text_table set (timescaledb.compress);

insert into text_table select 0 /*, default */ from generate_series(1, 1000) x;
select count(compress_chunk(x)) from show_chunks('text_table') x;

alter table text_table add column a text default 'default';
alter table text_table set (timescaledb.compress,
    timescaledb.compress_segmentby = '', timescaledb.compress_orderby = 'a');

insert into text_table select 1, '' from generate_series(1, 1000) x;
insert into text_table select 2, 'same' from generate_series(1, 1000) x;
insert into text_table select 3, 'different' || x from generate_series(1, 1000) x;
insert into text_table select 4, case when x % 2 = 0 then null else 'same-with-nulls' end from generate_series(1, 1000) x;
insert into text_table select 5, case when x % 2 = 0 then null else 'different-with-nulls' || x end from generate_series(1, 1000) x;

select count(compress_chunk(x)) from show_chunks('text_table') x;
vacuum analyze text_table;


-- Test the GroupAggregate by a text column. Not vectorized for now.
select a, count(*) from text_table group by a order by a limit 10;

-- The hash grouping policies do not support the GroupAggregate mode in the
-- reverse order.
select a, count(*) from text_table group by a order by a desc limit 10;


-- with NULLS FIRST
select count(decompress_chunk(x)) from show_chunks('text_table') x;
alter table text_table set (timescaledb.compress,
    timescaledb.compress_segmentby = '', timescaledb.compress_orderby = 'a nulls first');
select count(compress_chunk(x)) from show_chunks('text_table') x;

select a, count(*) from text_table group by a order by a nulls first limit 10;


-- TODO verify that this works with the serialized hash grouping strategy
select ts, a, count(*) from text_table group by ts, a order by ts, a limit 10;
select a, ts, count(*) from text_table group by a, ts order by a desc, ts desc limit 10;

reset max_parallel_workers_per_gather;
reset timescaledb.debug_require_vector_agg;
reset enable_hashagg;
