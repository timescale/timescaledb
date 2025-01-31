-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

create table bloom(ts int, value text);
select create_hypertable('bloom', 'ts');
insert into bloom select x, md5(x::text) from generate_series(1, 10000) x;
create index on bloom(value);
alter table bloom set (timescaledb.compress,
    timescaledb.compress_segmentby = '',
    timescaledb.compress_orderby = 'ts');
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
select count(*) from bloom where ts <
    case when now() < '1970-01-01' then 1 else 1000 end
;


-- Parameter on minmax index
set plan_cache_mode to 'force_generic_plan';
prepare p as
select count(*) from bloom where ts < $1;

explain (analyze, verbose, costs off, timing off, summary off)
execute p(1000);

deallocate p;


-- Parameter on bloom index
prepare p as
select count(*) from bloom where value = $1;

explain (analyze, verbose, costs off, timing off, summary off)
execute p(md5('2345'));

deallocate p;


-- Function of parameter on bloom index
prepare p as
select count(*) from bloom where value = md5($1);

explain (analyze, verbose, costs off, timing off, summary off)
execute p('2345');

deallocate p;

reset plan_cache_mode;
