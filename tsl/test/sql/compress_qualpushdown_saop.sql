-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Planning tests for compressed chunk table filter pushdown with scalar array
-- operations.
\c :TEST_DBNAME :ROLE_SUPERUSER

-- helper function: float -> pseudorandom float [-0.5..0.5]
create or replace function mix(x anyelement) returns float8 as $$
    select hashfloat8(x::float8) / pow(2, 32)
$$ language sql;

-- a lower() function that is stable
create function stable_lower(x text) returns text as 'lower' language internal stable;

-- a lower() function that is volatile
create function volatile_lower(x text) returns text as 'lower' language internal volatile;

set max_parallel_workers_per_gather = 0;

create table saop(ts int, segmentby text, with_minmax text, with_bloom text);

select create_hypertable('saop', 'ts', chunk_time_interval => 50001);

alter table saop set (timescaledb.compress,
    timescaledb.compress_segmentby = 'segmentby',
    timescaledb.compress_orderby = 'with_minmax, ts');

insert into saop
select ts,
    ts % 23 segmentby,
    ts % 29 with_minmax,
    (mix(ts % 1483) * 1483)::int::text with_bloom
from generate_series(1, 100000) ts;

create index on saop(with_bloom);

select count(compress_chunk(x)) from show_chunks('saop') x;

vacuum full analyze saop;


explain (analyze, buffers off, costs off, timing off, summary off)
select * from saop where segmentby = any(array['1', '10']);

explain (analyze, buffers off, costs off, timing off, summary off)
select * from saop where '1' = any(array[segmentby, segmentby]);

explain (analyze, buffers off, costs off, timing off, summary off)
select * from saop where with_minmax = any(array['1', '10']);

explain (analyze, buffers off, costs off, timing off, summary off)
select * from saop where with_bloom = any(array['1', '10']);

set timescaledb.enable_sparse_index_bloom to off;

explain (analyze, buffers off, costs off, timing off, summary off)
select * from saop where with_bloom = any(array['1', '10']);

reset timescaledb.enable_sparse_index_bloom;

explain (analyze, buffers off, costs off, timing off, summary off)
select * from saop where with_bloom = any(array['1', '10']::varchar(255)[]);

select * from (
    select 3 priority, 'C' "COLLATION"
    union all (select 2, collname from pg_collation where collname ilike 'en_us%' order by collencoding, collname limit 1)
    union all (select 1, collname from pg_collation where collname ilike 'en_us_utf%8%' order by collencoding, collname limit 1)
) c
order by priority limit 1 \gset

explain (analyze, buffers off, costs off, timing off, summary off)
select * from saop where (with_bloom collate :"COLLATION") = any(array['1', '10']::varchar(255)[]);

explain (analyze, buffers off, costs off, timing off, summary off)
select * from saop where with_bloom = all(array['1', '10']);

explain (analyze, buffers off, costs off, timing off, summary off)
select * from saop where with_minmax < any(array['1', '10']);

explain (analyze, buffers off, costs off, timing off, summary off)
select * from saop where with_minmax < all(array['1', '10']);

explain (analyze, buffers off, costs off, timing off, summary off)
select * from saop where with_bloom = any(array[segmentby, '10']);

explain (analyze, buffers off, costs off, timing off, summary off)
select * from saop where with_bloom = any(array[segmentby, null]);

explain (analyze, buffers off, costs off, timing off, summary off)
select * from saop where with_bloom = any(array[segmentby, null, with_minmax]);

explain (analyze, buffers off, costs off, timing off, summary off)
select * from saop where with_bloom = any(array[stable_lower(segmentby), stable_lower('1')]);

explain (analyze, buffers off, costs off, timing off, summary off)
select * from saop where with_bloom = any(array[stable_lower(segmentby), stable_lower('1'), volatile_lower('10')]);

explain (analyze, buffers off, costs off, timing off, summary off)
select * from saop where with_bloom = any(array[]::text[]);

explain (analyze, buffers off, costs off, timing off, summary off)
select * from saop where with_bloom = any(null::text[]);

explain (analyze, buffers off, costs off, timing off, summary off)
select * from saop where with_bloom = any(array[null, null]);

explain (analyze, buffers off, costs off, timing off, summary off)
select * from saop where segmentby = any(array[with_bloom, with_minmax]);

explain (analyze, buffers off, costs off, timing off, summary off)
select * from saop where segmentby = all(array[with_bloom, with_minmax]);


-- If the arguments of an operator can pushed down but require recheck, combining
-- them is wrong.
explain (analyze, buffers off, costs off, timing off, summary off)
select * from saop where (with_bloom = '1') = (with_minmax = '1');

explain (analyze, buffers off, costs off, timing off, summary off)
select * from saop where (with_bloom = any(array['1', '10'])) = (with_minmax = any(array['1', '10']));

explain (analyze, buffers off, costs off, timing off, summary off)
select * from saop where (segmentby = '1') = (segmentby = '2');

explain (analyze, buffers off, costs off, timing off, summary off)
select * from saop where (segmentby = any(array['1', '2'])) = (segmentby = any(array['3', '4']));

explain (analyze, buffers off, costs off, timing off, summary off)
select * from saop where with_bloom = any(case when with_minmax = '1' then array['1'] else array['2'] end);

explain (analyze, buffers off, costs off, timing off, summary off)
select * from saop where with_bloom = any(case when segmentby = '1' then array['1'] else array['2'] end);


-- Partial pushdown of AND scalar array operation.
explain (analyze, buffers off, costs off, timing off, summary off)
select * from saop where with_bloom = all(array[with_minmax, with_minmax]);

explain (analyze, buffers off, costs off, timing off, summary off)
select * from saop where with_bloom = all(array['1', with_minmax]);

explain (analyze, buffers off, costs off, timing off, summary off)
select * from saop where segmentby = '1' and with_bloom = all(array['1', with_minmax]);

explain (analyze, buffers off, costs off, timing off, summary off)
select * from saop where segmentby = '1' or with_bloom = all(array['1', with_minmax]);


-- Partial pushdown with volatile functions.
explain (analyze, buffers off, costs off, timing off, summary off)
select * from saop where with_bloom = any(array[stable_lower(segmentby), volatile_lower(segmentby)]);

explain (analyze, buffers off, costs off, timing off, summary off)
select * from saop where with_bloom = stable_lower(segmentby) or with_bloom = volatile_lower(segmentby);

explain (analyze, buffers off, costs off, timing off, summary off)
select * from saop where with_bloom = all(array[stable_lower(segmentby), volatile_lower(segmentby)]);

explain (analyze, buffers off, costs off, timing off, summary off)
select * from saop where segmentby = '1' or with_bloom = all(array[stable_lower('1'), volatile_lower('1')]);

explain (analyze, buffers off, costs off, timing off, summary off)
select * from saop where segmentby = '1' or (with_bloom = stable_lower('1') and with_bloom = volatile_lower('1'));


-- Some joins.
explain (analyze, buffers off, costs off, timing off, summary off)
with arrays as (select array[segmentby] a from saop group by segmentby order by segmentby limit 10)
select * from saop, arrays where segmentby = any(a);

explain (analyze, buffers off, costs off, timing off, summary off)
with arrays as (select array[segmentby] a from saop group by segmentby order by segmentby limit 10)
select * from saop, arrays where with_minmax = any(a);

explain (analyze, buffers off, costs off, timing off, summary off)
with arrays as (select array[segmentby] a from saop group by segmentby order by segmentby limit 10)
select * from saop, arrays where with_bloom = any(a);


-- Array parameter of prepared statements.
prepare array_param as select * from saop where with_bloom = any($1);
set timescaledb.enable_chunk_append to off;

-- Generic plans.
set plan_cache_mode = force_generic_plan;

explain (analyze, buffers off, costs off, timing off, summary off)
execute array_param(array['1', '10']);

explain (analyze, buffers off, costs off, timing off, summary off)
execute array_param(array[]::text[]);

explain (analyze, costs off, timing off, summary off)
execute array_param(null::text[]);

-- Custom plans.
set plan_cache_mode = force_custom_plan;

explain (analyze, buffers off, costs off, timing off, summary off)
execute array_param(array['1', '10']);

explain (analyze, buffers off, costs off, timing off, summary off)
execute array_param(array[]::text[]);

explain (analyze, costs off, timing off, summary off)
execute array_param(null::text[]);

reset timescaledb.enable_chunk_append;
