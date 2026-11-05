-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE VIEW settings AS SELECT * FROM _timescaledb_catalog.compression_settings ORDER BY upper(relid::text) COLLATE "C";

-- Test configurable sparse indexes settings

create table test_settings(
  x int, value text, u uuid, ts timestamp, d real, b bigint, age int, gender text, city text,
  long1_01234567890123456789 text, long2_01234567890123456789 text, long3_01234567890123456789 text);
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

-- valid composite bloom filter cases:
-- two columns
alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = 'bloom(u,ts)');

alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = 'bloom("u","ts")');

alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = 'bloom("u",ts)');

-- three columns
alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = 'bloom(u,ts,value)');

alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = 'bloom("u","ts","value")');

alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = 'bloom("u",ts,"value")');

alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = 'bloom(long1_01234567890123456789,long2_01234567890123456789,long3_01234567890123456789)');

-- more than 3 columns
alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = 'bloom(u,ts,value,d)');

alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = 'bloom(u,ts,value,d,b)');

-- partial overlaps
alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = 'bloom(u,value),bloom(u,ts)');

alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = 'bloom(u),bloom(u,ts)');

alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = 'bloom(u,value),bloom(u,ts,value)');

-- invalid composite bloom filter cases:
\set ON_ERROR_STOP 0

-- nine column composite bloom is not supported
alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = 'bloom(u,ts,"value",d,b,x,age,gender,city)');

-- duplicate column
alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = 'bloom(u,ts,u)');

-- duplicate blooms in different orders
-- 2 cols
alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = 'bloom(u,ts),bloom(ts,u)');

-- 3 cols
alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = 'bloom(u,ts,x),bloom(u,x,ts)');

alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = 'bloom(ts,x,u),bloom(ts,u,x)');

alter table test_settings set (timescaledb.compress,
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = 'bloom(x,ts,u),bloom(x,u,ts)');

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
select count(compress_chunk(x, recompress:=true)) from show_chunks('test_sparse_index') x;
vacuum full analyze test_sparse_index;

select cs.compress_relid::regclass chunk from _timescaledb_catalog.chunk ch
    join _timescaledb_catalog.compression_settings cs
        on cs.relid = ch.relid
    where ch.hypertable_id = (select id from _timescaledb_catalog.hypertable
        where table_name = 'test_sparse_index') limit 1
\gset

select * from test.show_columns_ext(:'chunk'::regclass);

-- UUID uses bloom
explain (analyze, verbose, buffers off, costs off, timing off, summary off)
select count(*) from test_sparse_index where u = '90ec9e8e-4501-4232-9d03-6d7cf6132815';

-- Timestamp uses minmax
explain (analyze, verbose, buffers off, costs off, timing off, summary off)
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

select cs.compress_relid::regclass chunk from _timescaledb_catalog.chunk ch
    join _timescaledb_catalog.compression_settings cs
        on cs.relid = ch.relid
    where ch.hypertable_id = (select id from _timescaledb_catalog.hypertable
        where table_name = 'test_sparse_index') limit 1
\gset

select * from test.show_columns_ext(:'chunk'::regclass);

-- these tests show that despite column having multiple sparse indexes, the appropriate one is selected by the planner
-- UUID uses bloom
explain (analyze, verbose, buffers off, costs off, timing off, summary off)
select count(*) from test_sparse_index where u = '90ec9e8e-4501-4232-9d03-6d7cf6132815';

-- Timestamp uses minmax
explain (analyze, verbose, buffers off, costs off, timing off, summary off)
select count(*) from test_sparse_index where ts between '2021-01-07' and '2021-01-14';


-- Test recompression when the bloom filter index is disabled by a GUC
set timescaledb.enable_sparse_index_bloom to off;

select count(compress_chunk(decompress_chunk(x))) from show_chunks('test_sparse_index') x;

reset timescaledb.enable_sparse_index_bloom;


-- Test rename column
-- change a non-sparse index column
alter table test_sparse_index rename value to value_new;
-- change sparse index column
alter table test_sparse_index rename ts to ts_new;
select * from settings;

select cs.compress_relid::regclass chunk from _timescaledb_catalog.chunk ch
    join _timescaledb_catalog.compression_settings cs
        on cs.relid = ch.relid
    where ch.hypertable_id = (select id from _timescaledb_catalog.hypertable
        where table_name = 'test_sparse_index') limit 1
\gset

select * from test.show_columns_ext(:'chunk'::regclass);

-- Test same minmax and orderby column
alter table test_sparse_index set (timescaledb.compress,
    timescaledb.compress_segmentby = '',
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = 'minmax("x")');

select count(compress_chunk(x, recompress:=true)) from show_chunks('test_sparse_index') x;

select cs.compress_relid::regclass chunk from _timescaledb_catalog.chunk ch
    join _timescaledb_catalog.compression_settings cs
        on cs.relid = ch.relid
    where ch.hypertable_id = (select id from _timescaledb_catalog.hypertable
        where table_name = 'test_sparse_index') limit 1
\gset

select * from test.show_columns_ext(:'chunk'::regclass);
select * from settings;

-- Test auto sparse index
create index ii on test_sparse_index(value_new);
alter table test_sparse_index reset (timescaledb.compress_index);
alter table test_sparse_index set (timescaledb.compress,
    timescaledb.compress_segmentby = '',
    timescaledb.compress_orderby = 'x');

select count(compress_chunk(decompress_chunk(x))) from show_chunks('test_sparse_index') x;

select cs.compress_relid::regclass chunk from _timescaledb_catalog.chunk ch
    join _timescaledb_catalog.compression_settings cs
        on cs.relid = ch.relid
    where ch.hypertable_id = (select id from _timescaledb_catalog.hypertable
        where table_name = 'test_sparse_index') limit 1
\gset

select * from test.show_columns_ext(:'chunk'::regclass);
select * from settings;

-- Test empty sparse index
alter table test_sparse_index set (timescaledb.compress,
    timescaledb.compress_segmentby = '',
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = '');

select count(compress_chunk(x, recompress:=true)) from show_chunks('test_sparse_index') x;
vacuum full analyze test_sparse_index;

select cs.compress_relid::regclass chunk from _timescaledb_catalog.chunk ch
    join _timescaledb_catalog.compression_settings cs
        on cs.relid = ch.relid
    where ch.hypertable_id = (select id from _timescaledb_catalog.hypertable
        where table_name = 'test_sparse_index') limit 1
\gset

select * from test.show_columns_ext(:'chunk'::regclass);
select * from settings;

-- Test default orderby without sparse index
set timescaledb.enable_sparse_index_bloom to false;
alter table test_sparse_index reset (timescaledb.compress_segmentby,
    timescaledb.compress_orderby,
    timescaledb.compress_index);
select count(compress_chunk(decompress_chunk(x))) from show_chunks('test_sparse_index') x;
vacuum full analyze test_sparse_index;

select cs.compress_relid::regclass chunk from _timescaledb_catalog.chunk ch
    join _timescaledb_catalog.compression_settings cs
        on cs.relid = ch.relid
    where ch.hypertable_id = (select id from _timescaledb_catalog.hypertable
        where table_name = 'test_sparse_index') limit 1
\gset

select * from test.show_columns_ext(:'chunk'::regclass);
select * from settings;
reset timescaledb.enable_sparse_index_bloom;
drop table test_sparse_index;

-- Test drop column with sparse indexes
create table test_drop_sparse(x int, value text, u uuid, ts timestamp, extra text);
select create_hypertable('test_drop_sparse', 'x');

insert into test_drop_sparse
select x, md5(x::text),
    '6c1d0998-05f3-452c-abd3-45afe72bbcab'::uuid,
    '2021-01-01'::timestamp + (interval '1 hour') * x,
    'extra'
from generate_series(1, 1000) x;

-- Configure with bloom on u, minmax on ts, composite bloom on (u, extra)
alter table test_drop_sparse set (timescaledb.compress,
    timescaledb.compress_segmentby = '',
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = 'bloom("u"), minmax("ts"), bloom("u","extra")');
select * from settings;

select count(compress_chunk(x)) from show_chunks('test_drop_sparse') x;

select cs.compress_relid::regclass chunk from _timescaledb_catalog.chunk ch
    join _timescaledb_catalog.compression_settings cs
        on cs.relid = ch.relid
    where ch.hypertable_id = (select id from _timescaledb_catalog.hypertable
        where table_name = 'test_drop_sparse') limit 1
\gset

-- Show columns before drop
select * from test.show_columns_ext(:'chunk'::regclass);

-- Drop column with minmax sparse index
alter table test_drop_sparse drop column ts;
select * from settings;

select * from test.show_columns_ext(:'chunk'::regclass);

-- Verify data is still queryable
select count(*) from test_drop_sparse;

-- Drop column with bloom sparse index (also part of composite bloom)
alter table test_drop_sparse drop column u;
select * from settings;

select * from test.show_columns_ext(:'chunk'::regclass);

-- Verify data is still queryable
select count(*) from test_drop_sparse;

-- Drop column with only composite bloom participation
drop table test_drop_sparse;

create table test_drop_sparse2(x int, a text, b text, c text);
select create_hypertable('test_drop_sparse2', 'x');

insert into test_drop_sparse2
select x, 'a', 'b', 'c' from generate_series(1, 1000) x;

alter table test_drop_sparse2 set (timescaledb.compress,
    timescaledb.compress_segmentby = '',
    timescaledb.compress_orderby = 'x',
    timescaledb.compress_index = 'bloom("a","b","c")');
select * from settings;

select count(compress_chunk(x)) from show_chunks('test_drop_sparse2') x;

select cs.compress_relid::regclass chunk from _timescaledb_catalog.chunk ch
    join _timescaledb_catalog.compression_settings cs
        on cs.relid = ch.relid
    where ch.hypertable_id = (select id from _timescaledb_catalog.hypertable
        where table_name = 'test_drop_sparse2') limit 1
\gset

select * from test.show_columns_ext(:'chunk'::regclass);

-- Drop one column from the composite bloom
alter table test_drop_sparse2 drop column b;
select * from settings;

select * from test.show_columns_ext(:'chunk'::regclass);

select count(*) from test_drop_sparse2;

drop table test_drop_sparse2;

-- Test orderby sparse index with default orderby and auto_sparse_indexes off
set timescaledb.auto_sparse_indexes = false;
create table test_orderby_default_noguc(x int, value float);
select create_hypertable('test_orderby_default_noguc', 'x');
-- No explicit orderby — triggers compression_settings_set_defaults path
alter table test_orderby_default_noguc set (timescaledb.compress);
insert into test_orderby_default_noguc select i, random() from generate_series(1, 10000) i;
select count(compress_chunk(c)) from show_chunks('test_orderby_default_noguc') c;
select * from settings;

reset timescaledb.auto_sparse_indexes;
drop table test_orderby_default_noguc;

-- Rename a column to a name that matches a sparse index type token
create table test_rename_token(ts int not null, minmax int not null);
select create_hypertable('test_rename_token', 'ts', chunk_time_interval => 100);
alter table test_rename_token set (timescaledb.compress, timescaledb.compress_orderby = 'ts');
insert into test_rename_token select 1,1;
select compress_chunk(show_chunks('test_rename_token'));
select * from settings;
-- renaming to a type token must not corrupt the orderby minmax index
alter table test_rename_token rename column minmax to bloom;
select * from settings;
alter table test_rename_token rename column bloom to firstlast;
select * from settings;
-- renaming an indexed column to a type token keeps the index intact
alter table test_rename_token rename column ts to minmax;
select * from settings;

\set ON_ERROR_STOP 0
alter table test_rename_token rename column minmax to _ts_meta_v2_first_minmax;
alter table test_rename_token rename column minmax to _ts_meta_count;
\set ON_ERROR_STOP 1

drop table test_rename_token;

-- Test DML and queries on chunks compressed with an empty sparse index
-- setting. The empty setting is stored as a {"source": "config"} object
-- without columns, which must be skipped when resolving sparse index
-- columns to attribute numbers (#10414).
create table test_empty_sparse(id int, day date not null, value int, primary key (id, day));
select create_hypertable('test_empty_sparse', 'day', chunk_time_interval => interval '30 days');
insert into test_empty_sparse select i, '2025-01-01'::date + i, i from generate_series(0, 9) i;
alter table test_empty_sparse set (timescaledb.compress,
    timescaledb.compress_orderby = 'day desc',
    timescaledb.compress_index = '');
select count(compress_chunk(c)) from show_chunks('test_empty_sparse') c;
select relid::regclass, index from settings where relid = 'test_empty_sparse'::regclass;

set timescaledb.enable_sparse_index_bloom to on;
set timescaledb.enable_composite_bloom_indexes to on;
set timescaledb.enable_dml_bloom_filter to on;
set timescaledb.enable_dml_decompression_tuple_filtering to on;

-- SELECT with equality predicates on two columns
select * from test_empty_sparse where id = 1 and day = '2025-01-02';

-- UPDATE and DELETE with an equality predicate
update test_empty_sparse set value = 100 where day = '2025-01-03';
delete from test_empty_sparse where day = '2025-01-05';

-- INSERT ON CONFLICT on the unique index
insert into test_empty_sparse values (1, '2025-01-02', 42) on conflict (id, day) do nothing;
insert into test_empty_sparse values (2, '2025-01-03', 42) on conflict (id, day) do update set value = excluded.value;

select * from test_empty_sparse order by id;

reset timescaledb.enable_sparse_index_bloom;
reset timescaledb.enable_composite_bloom_indexes;
reset timescaledb.enable_dml_bloom_filter;
reset timescaledb.enable_dml_decompression_tuple_filtering;
drop table test_empty_sparse;
