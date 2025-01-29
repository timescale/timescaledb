-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

create table bloom(ts int, value text);
select create_hypertable('bloom', 'ts');
insert into bloom select x, md5(x::text) from generate_series(1, 10000) x;
create index on bloom(value);
alter table bloom set (timescaledb.compress,
    timescaledb.compress_segmentby = '',
    timescaledb.compress_orderby = 'ts');
select count(compress_chunk(x)) from show_chunks('bloom') x;

explain (analyze, verbose, costs off, timing off, summary off)
select count(*) from bloom where value = md5(7248::text);
