-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

create table bloom(x int, value text, u uuid, ts timestamp);
select create_hypertable('bloom', 'x');

insert into bloom
select x, md5(x::text),
    case when x = 7134 then '90ec9e8e-4501-4232-9d03-6d7cf6132815'
        else '6c1d0998-05f3-452c-abd3-45afe72bbcab'::uuid end,
    '2021-01-01'::timestamp + (interval '1 hour') * x
from generate_series(1, 10000) x;

create index on bloom using brin(value text_bloom_ops);
create index on bloom using brin(u uuid_bloom_ops);
create index on bloom using brin(ts timestamp_minmax_ops);

alter table bloom set (timescaledb.compress,
    timescaledb.compress_segmentby = '',
    timescaledb.compress_orderby = 'x');
select count(compress_chunk(x)) from show_chunks('bloom') x;

select schema_name || '.' || table_name chunk from _timescaledb_catalog.chunk
    where id = (select compressed_chunk_id from _timescaledb_catalog.chunk
        where hypertable_id = (select id from _timescaledb_catalog.hypertable
            where table_name = 'bloom') limit 1)
\gset

\d+ :chunk

explain (analyze, verbose, costs off, timing off, summary off)
select count(*) from bloom where value = md5(7248::text);

select count(*) from bloom where value = md5(7248::text);


-- The join condition is not pushed down to the compressed scan for some reason.
set enable_mergejoin to off;
set enable_hashjoin to off;

explain (analyze, verbose, costs off, timing off, summary off)
with query(value) as materialized (values (md5(3516::text)), (md5(9347::text)),
    (md5(5773::text)))
select count(*) from bloom natural join query;
;

with query(value) as materialized (values (md5(3516::text)), (md5(9347::text)),
    (md5(5773::text)))
select count(*) from bloom natural join query;
;

reset enable_mergejoin;
reset enable_hashjoin;


-- Stable expression that yields null
set timescaledb.enable_chunk_append to off;

explain (analyze, verbose, costs off, timing off, summary off)
select count(*) from bloom where value =
    case when now() < '1970-01-01' then md5(2345::text) else null end
;

reset timescaledb.enable_chunk_append;


-- Stable expression that yields not null
explain (analyze, verbose, costs off, timing off, summary off)
select count(*) from bloom where value =
    case when now() < '1970-01-01' then md5(2345::text) else md5(5837::text) end
;


-- Stable expression on minmax index
explain (analyze, verbose, costs off, timing off, summary off)
select count(*) from bloom where x <
    case when now() < '1970-01-01' then 1 else 1000 end
;


-- Parameter on minmax index
set timescaledb.enable_chunk_append to off;
set plan_cache_mode to 'force_generic_plan';
prepare p as
select count(*) from bloom where x < $1;

explain (analyze, verbose, costs off, timing off, summary off)
execute p(1000);

deallocate p;


-- Parameter on bloom index
prepare p as
select count(*) from bloom where value = $1;

explain (analyze, verbose, costs off, timing off, summary off)
execute p(md5('2345'));

-- Null parameter on bloom index
explain (analyze, verbose, costs off, timing off, summary off)
execute p(null);

reset timescaledb.enable_chunk_append;

deallocate p;


-- Function of parameter on bloom index
prepare p as
select count(*) from bloom where value = md5($1);

explain (analyze, verbose, costs off, timing off, summary off)
execute p('2345');

deallocate p;

reset plan_cache_mode;
reset timescaledb.enable_chunk_append;


-- Scalar array operations are not yet supported
explain (analyze, verbose, costs off, timing off, summary off)
select count(*) from bloom where x < any(array[1000, 2000]::int[]);

explain (analyze, verbose, costs off, timing off, summary off)
select count(*) from bloom where value = any(array[md5('1000'), md5('2000')]);


-- UUID uses bloom
explain (analyze, verbose, costs off, timing off, summary off)
select count(*) from bloom where u = '90ec9e8e-4501-4232-9d03-6d7cf6132815';

explain (analyze, verbose, costs off, timing off, summary off)
select count(*) from bloom where u = '6c1d0998-05f3-452c-abd3-45afe72bbcab';

explain (analyze, verbose, costs off, timing off, summary off)
select count(*) from bloom where u = '6c1d0998-05f3-452c-abd3-45afe72bbcac';


-- Timestamp uses minmax
explain (analyze, verbose, costs off, timing off, summary off)
select count(*) from bloom where ts between '2021-01-07' and '2021-01-14';


-- Test some corner cases
create table corner(ts int, s text, c text);
select create_hypertable('corner', 'ts');
alter table corner set (timescaledb.compress, timescaledb.compress_segmentby = 's',
    timescaledb.compress_orderby = 'ts');

-- This is to test detoasting the second argument of "bloom filter contains"
-- function.
insert into corner values (1, repeat('long', 100), 'short');

-- This is to test the null bloom filter.
insert into corner values(2, 'normal', null);

-- This is to test the match and also the short varlena header in the bloom
-- filter.
insert into corner values(3, 'match', 'match');

-- This is to test the long varlena header in the bloom filter.
insert into corner select 4, 'longheader', generate_series(1, 1000)::text;

create index on corner(c);

select count(compress_chunk(x)) from show_chunks('corner') x;

explain (analyze, verbose, costs off, timing off, summary off)
select * from corner where c = 'short';

-- Cross-type equality operator.
explain (analyze, verbose, costs off, timing off, summary off)
select * from corner where c = 'short'::name;

-- Comparison with segmentby.
explain (analyze, verbose, costs off, timing off, summary off)
select * from corner where c = s;

-- Can push down only some parts of the expression but not the others, so the
-- pushdown shouldn't work in this case.
explain (analyze, verbose, costs off, timing off, summary off)
select * from corner where c = s or c = random()::text;

explain (analyze, verbose, costs off, timing off, summary off)
select * from corner where c = 'test'
    or c = case when now() > '1970-01-01' then 'test2' else random()::text end
;


-- Cleanup
drop table bloom;
drop table corner;


