-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE VIEW settings AS SELECT * FROM _timescaledb_catalog.compression_settings ORDER BY upper(relid::text) COLLATE "C";

-- Test configurable sparse indexes settings

create table test_settings(x int, value text, u uuid, ts timestamp);
select create_hypertable('test_settings', 'x');

-- defaults
alter table test_settings set (timescaledb.compress);
select * from settings;

-- no custom sparse indexes
alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'x');
select * from settings;

-- one sparse index
alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = 'bloom("u")');
select * from settings;

alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = 'minmax("ts")');
select * from settings;

--multi column
alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = 'bloom("u"), bloom("ts")');
select * from settings;

alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = 'bloom(u), bloom(ts)');
select * from settings;

--test errors
\set ON_ERROR_STOP 0
-- invalid syntax
alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = 'bloom("u"), bloom(count(u))');

alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = '"u", minmax("u")');

alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = 'foo("u")');

alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = 'bloom(public.u)');

alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = 'select count(*)');

alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = 'bloom (ts); select count(*)');

alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = ';');

alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = ' ');

-- same column
alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = 'bloom("u"), minmax("u")');

-- duplicate column
alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = 'bloom("u"), bloom("u")');

-- invalid column
alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = 'bloom("u"), minmax("foo")');

-- no orderby
alter table test_settings reset (timescaledb.compress_orderby);
alter table test_settings set (timescaledb.compress,
    timescaledb.compress_index = 'bloom("u"), minmax("x")');

-- guc disabled
set timescaledb.enable_sparse_index_bloom to false;

alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = 'bloom("u")');

reset timescaledb.enable_sparse_index_bloom;
\set ON_ERROR_STOP 1

-- empty sparse index
alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'ts',
    timescaledb.compress_index = '');
select * from settings;

-- change orderby setting (sparse index changes)
alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'u');
select * from settings;

-- same column as orderby should succeed since minmax
alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'ts',
    timescaledb.compress_index = 'bloom("value"), minmax("ts")');
select * from settings;

-- change orderby setting (sparse index doesn't change)
alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'u');
select * from settings;

alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = 'minmax("x")');

alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'u');
select * from settings;

-- same column as orderby should fail since bloom
\set ON_ERROR_STOP 0
alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = 'bloom("x"), minmax("ts")');
\set ON_ERROR_STOP 1

drop table test_settings;

-- Test configurable sparse indexes functionality
create table test_sparse_index(x int, value text, u uuid, ts timestamp, data jsonb);
select create_hypertable('test_sparse_index', 'x');
create index gin_jsonb_idx on test_sparse_index using gin (data);
insert into test_sparse_index
select x, md5(x::text),
    case when x = 7134 then '90ec9e8e-4501-4232-9d03-6d7cf6132815'
        else '6c1d0998-05f3-452c-abd3-45afe72bbcab'::uuid end,
    '2021-01-01'::timestamp + (interval '1 hour') * x,
    jsonb_build_object('id', x, 'tag', case when x % 2 = 0 then 'even' else 'odd' end, 'active', x % 10 = 0)
from generate_series(1, 10000) x;

alter table test_sparse_index set (timescaledb.compress,
    timescaledb.compress_segmentby = '',
    timescaledb.compress_orderby = 'x');
select count(compress_chunk(x)) from show_chunks('test_sparse_index') x;
alter table test_sparse_index set (timescaledb.compress,
    timescaledb.compress_segmentby = '',
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = 'bloom("u"),minmax("ts")');
select * from settings;
select count(compress_chunk(decompress_chunk(x))) from show_chunks('test_sparse_index') x;
vacuum full analyze test_sparse_index;

select schema_name || '.' || table_name chunk from _timescaledb_catalog.chunk
    where id = (select compressed_chunk_id from _timescaledb_catalog.chunk
        where hypertable_id = (select id from _timescaledb_catalog.hypertable
            where table_name = 'test_sparse_index') limit 1)
\gset

\d+ :chunk

-- UUID uses bloom
explain (analyze, verbose, costs off, timing off, summary off)
select count(*) from test_sparse_index where u = '90ec9e8e-4501-4232-9d03-6d7cf6132815';

-- Timestamp uses minmax
explain (analyze, verbose, costs off, timing off, summary off)
select count(*) from test_sparse_index where ts between '2021-01-07' and '2021-01-14';

drop table test_sparse_index;

-- Test configurable sparse index in CREATE TABLE .. WITH
create table test_sparse_index(x int, value text, u uuid, ts timestamp) with (
    tsdb.hypertable,
    tsdb.partition_column='x',
    tsdb.order_by='x',
    tsdb.index='bloom("u"),minmax("ts")');
select * from settings;

insert into test_sparse_index
select x, md5(x::text),
    case when x = 7134 then '90ec9e8e-4501-4232-9d03-6d7cf6132815'
        else '6c1d0998-05f3-452c-abd3-45afe72bbcab'::uuid end,
    '2021-01-01'::timestamp + (interval '1 hour') * x
from generate_series(1, 10000) x;

select count(compress_chunk(x)) from show_chunks('test_sparse_index') x;
vacuum full analyze test_sparse_index;

select schema_name || '.' || table_name chunk from _timescaledb_catalog.chunk
    where id = (select compressed_chunk_id from _timescaledb_catalog.chunk
        where hypertable_id = (select id from _timescaledb_catalog.hypertable
            where table_name = 'test_sparse_index') limit 1)
\gset

\d+ :chunk

-- these tests show that despite column having multiple sparse indexes, the appropriate one is selected by the planner
-- UUID uses bloom
explain (analyze, verbose, costs off, timing off, summary off)
select count(*) from test_sparse_index where u = '90ec9e8e-4501-4232-9d03-6d7cf6132815';

-- Timestamp uses minmax
explain (analyze, verbose, costs off, timing off, summary off)
select count(*) from test_sparse_index where ts between '2021-01-07' and '2021-01-14';

-- Test rename column
-- change a non-sparse index column
alter table test_sparse_index rename value to value_new;
-- change sparse index column
alter table test_sparse_index rename ts to ts_new;
select * from settings;

select schema_name || '.' || table_name chunk from _timescaledb_catalog.chunk
    where id = (select compressed_chunk_id from _timescaledb_catalog.chunk
        where hypertable_id = (select id from _timescaledb_catalog.hypertable
            where table_name = 'test_sparse_index') limit 1)
\gset

\d+ :chunk

-- Test same minmax and orderby column
alter table test_sparse_index set (timescaledb.compress,
    timescaledb.compress_segmentby = '',
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = 'minmax("x")');

select count(compress_chunk(decompress_chunk(x))) from show_chunks('test_sparse_index') x;

select schema_name || '.' || table_name chunk from _timescaledb_catalog.chunk
    where id = (select compressed_chunk_id from _timescaledb_catalog.chunk
        where hypertable_id = (select id from _timescaledb_catalog.hypertable
            where table_name = 'test_sparse_index') limit 1)
\gset

\d+ :chunk
select * from settings;

-- Test auto sparse index
create index ii on test_sparse_index(value_new);
alter table test_sparse_index reset (timescaledb.compress_index);
alter table test_sparse_index set (timescaledb.compress,
    timescaledb.compress_segmentby = '',
    timescaledb.compress_orderby = 'x');

select count(compress_chunk(decompress_chunk(x))) from show_chunks('test_sparse_index') x;

select schema_name || '.' || table_name chunk from _timescaledb_catalog.chunk
    where id = (select compressed_chunk_id from _timescaledb_catalog.chunk
        where hypertable_id = (select id from _timescaledb_catalog.hypertable
            where table_name = 'test_sparse_index') limit 1)
\gset

\d+ :chunk
select * from settings;

-- Test empty sparse index
alter table test_sparse_index set (timescaledb.compress,
    timescaledb.compress_segmentby = '',
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = '');

select count(compress_chunk(decompress_chunk(x))) from show_chunks('test_sparse_index') x;
vacuum full analyze test_sparse_index;

select schema_name || '.' || table_name chunk from _timescaledb_catalog.chunk
    where id = (select compressed_chunk_id from _timescaledb_catalog.chunk
        where hypertable_id = (select id from _timescaledb_catalog.hypertable
            where table_name = 'test_sparse_index') limit 1)
\gset

\d+ :chunk
select * from settings;

-- Test default orderby without sparse index
set timescaledb.enable_sparse_index_bloom to false;
alter table test_sparse_index reset (timescaledb.compress_segmentby,
    timescaledb.compress_orderby,
    timescaledb.compress_index);
select count(compress_chunk(decompress_chunk(x))) from show_chunks('test_sparse_index') x;
vacuum full analyze test_sparse_index;

select schema_name || '.' || table_name chunk from _timescaledb_catalog.chunk
    where id = (select compressed_chunk_id from _timescaledb_catalog.chunk
        where hypertable_id = (select id from _timescaledb_catalog.hypertable
            where table_name = 'test_sparse_index') limit 1)
\gset

\d+ :chunk
select * from settings;
reset timescaledb.enable_sparse_index_bloom;
drop table test_sparse_index;
