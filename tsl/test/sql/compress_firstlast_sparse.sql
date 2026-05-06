-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE VIEW settings AS SELECT * FROM _timescaledb_catalog.compression_settings ORDER BY upper(relid::text) COLLATE "C";

-- basic configuration
create table test_firstlast(ts timestamptz, value int, tag text);
select create_hypertable('test_firstlast', 'ts');

alter table test_firstlast set (timescaledb.compress,
    timescaledb.compress_orderby = 'ts',
    timescaledb.compress_index = 'firstlast("value")');
select * from settings;

alter table test_firstlast set (timescaledb.compress,
    timescaledb.compress_orderby = 'ts',
    timescaledb.compress_index = 'firstlast("tag")');
select * from settings;

-- firstlast alongside bloom
alter table test_firstlast set (timescaledb.compress,
    timescaledb.compress_orderby = 'ts',
    timescaledb.compress_index = 'firstlast("value"), bloom("tag")');
select * from settings;

drop table test_firstlast;

-- metadata column values
create table fl_basic(ts int, value int);
select create_hypertable('fl_basic', 'ts', chunk_time_interval => 10000);
insert into fl_basic select x, x * 10 from generate_series(1, 5) x;

alter table fl_basic set (timescaledb.compress,
    timescaledb.compress_orderby = 'ts',
    timescaledb.compress_index = 'firstlast("value")');
select count(compress_chunk(x)) from show_chunks('fl_basic') x;

select ch.schema_name || '.' || ch.table_name as compressed_chunk
from _timescaledb_catalog.chunk ch
inner join _timescaledb_catalog.chunk uc on ch.id = uc.compressed_chunk_id
where uc.hypertable_id = (select id from _timescaledb_catalog.hypertable where table_name = 'fl_basic')
\gset

-- sorted by ts ASC: values are [10,20,30,40,50]
select _ts_meta_v2_first_value, _ts_meta_v2_last_value
from :compressed_chunk;

drop table fl_basic;

-- all NULLs
create table fl_nulls(ts int, value int);
select create_hypertable('fl_nulls', 'ts', chunk_time_interval => 10000);
insert into fl_nulls select x, null from generate_series(1, 5) x;

alter table fl_nulls set (timescaledb.compress,
    timescaledb.compress_orderby = 'ts',
    timescaledb.compress_index = 'firstlast("value")');
select count(compress_chunk(x)) from show_chunks('fl_nulls') x;

select ch.schema_name || '.' || ch.table_name as compressed_chunk
from _timescaledb_catalog.chunk ch
inner join _timescaledb_catalog.chunk uc on ch.id = uc.compressed_chunk_id
where uc.hypertable_id = (select id from _timescaledb_catalog.hypertable where table_name = 'fl_nulls')
\gset

select _ts_meta_v2_first_value, _ts_meta_v2_last_value,
       _ts_meta_v2_first_value is null as first_is_null,
       _ts_meta_v2_last_value is null as last_is_null
from :compressed_chunk;

drop table fl_nulls;

-- mixed NULLs
create table fl_mixed(ts int, value int);
select create_hypertable('fl_mixed', 'ts', chunk_time_interval => 10000);
insert into fl_mixed values (1, 10), (2, null), (3, 30), (4, null), (5, 50);

alter table fl_mixed set (timescaledb.compress,
    timescaledb.compress_orderby = 'ts',
    timescaledb.compress_index = 'firstlast("value")');
select count(compress_chunk(x)) from show_chunks('fl_mixed') x;

select ch.schema_name || '.' || ch.table_name as compressed_chunk
from _timescaledb_catalog.chunk ch
inner join _timescaledb_catalog.chunk uc on ch.id = uc.compressed_chunk_id
where uc.hypertable_id = (select id from _timescaledb_catalog.hypertable where table_name = 'fl_mixed')
\gset

-- first row (1,10), last row (5,50)
select _ts_meta_v2_first_value, _ts_meta_v2_last_value
from :compressed_chunk;

drop table fl_mixed;

-- first row is NULL
create table fl_firstnull(ts int, value int);
select create_hypertable('fl_firstnull', 'ts', chunk_time_interval => 10000);
insert into fl_firstnull values (1, null), (2, 20), (3, 30);

alter table fl_firstnull set (timescaledb.compress,
    timescaledb.compress_orderby = 'ts',
    timescaledb.compress_index = 'firstlast("value")');
select count(compress_chunk(x)) from show_chunks('fl_firstnull') x;

select ch.schema_name || '.' || ch.table_name as compressed_chunk
from _timescaledb_catalog.chunk ch
inner join _timescaledb_catalog.chunk uc on ch.id = uc.compressed_chunk_id
where uc.hypertable_id = (select id from _timescaledb_catalog.hypertable where table_name = 'fl_firstnull')
\gset

select _ts_meta_v2_first_value, _ts_meta_v2_last_value,
       _ts_meta_v2_first_value is null as first_is_null
from :compressed_chunk;

drop table fl_firstnull;

-- last row is NULL
create table fl_lastnull(ts int, value int);
select create_hypertable('fl_lastnull', 'ts', chunk_time_interval => 10000);
insert into fl_lastnull values (1, 10), (2, 20), (3, null);

alter table fl_lastnull set (timescaledb.compress,
    timescaledb.compress_orderby = 'ts',
    timescaledb.compress_index = 'firstlast("value")');
select count(compress_chunk(x)) from show_chunks('fl_lastnull') x;

select ch.schema_name || '.' || ch.table_name as compressed_chunk
from _timescaledb_catalog.chunk ch
inner join _timescaledb_catalog.chunk uc on ch.id = uc.compressed_chunk_id
where uc.hypertable_id = (select id from _timescaledb_catalog.hypertable where table_name = 'fl_lastnull')
\gset

select _ts_meta_v2_first_value, _ts_meta_v2_last_value,
       _ts_meta_v2_last_value is null as last_is_null
from :compressed_chunk;

drop table fl_lastnull;

-- single row batch
create table fl_single(ts int, value int);
select create_hypertable('fl_single', 'ts', chunk_time_interval => 10000);
insert into fl_single values (1, 42);

alter table fl_single set (timescaledb.compress,
    timescaledb.compress_orderby = 'ts',
    timescaledb.compress_index = 'firstlast("value")');
select count(compress_chunk(x)) from show_chunks('fl_single') x;

select ch.schema_name || '.' || ch.table_name as compressed_chunk
from _timescaledb_catalog.chunk ch
inner join _timescaledb_catalog.chunk uc on ch.id = uc.compressed_chunk_id
where uc.hypertable_id = (select id from _timescaledb_catalog.hypertable where table_name = 'fl_single')
\gset

select _ts_meta_v2_first_value, _ts_meta_v2_last_value
from :compressed_chunk;

drop table fl_single;

-- multiple orderby columns
create table fl_multi(a int, b int, value text);
select create_hypertable('fl_multi', 'a', chunk_time_interval => 10000);
insert into fl_multi values (1, 5, 'x'), (2, 3, 'w'), (1, 8, 'y'), (3, 2, 'v'), (2, 1, 'z');

alter table fl_multi set (timescaledb.compress,
    timescaledb.compress_orderby = 'a, b',
    timescaledb.compress_index = 'firstlast("value")');
select count(compress_chunk(x)) from show_chunks('fl_multi') x;

select ch.schema_name || '.' || ch.table_name as compressed_chunk
from _timescaledb_catalog.chunk ch
inner join _timescaledb_catalog.chunk uc on ch.id = uc.compressed_chunk_id
where uc.hypertable_id = (select id from _timescaledb_catalog.hypertable where table_name = 'fl_multi')
\gset

-- sorted: (1,5,x),(1,8,y),(2,1,z),(2,3,w),(3,2,v)
select _ts_meta_v2_first_value, _ts_meta_v2_last_value
from :compressed_chunk;

drop table fl_multi;

-- recompression after insert
create table fl_recompress(ts int, value int);
select create_hypertable('fl_recompress', 'ts', chunk_time_interval => 10000);
insert into fl_recompress select x, x * 10 from generate_series(1, 5) x;

alter table fl_recompress set (timescaledb.compress,
    timescaledb.compress_orderby = 'ts',
    timescaledb.compress_index = 'firstlast("value")');
select count(compress_chunk(x)) from show_chunks('fl_recompress') x;

insert into fl_recompress values (6, 600);
select count(compress_chunk(x, recompress:=true)) from show_chunks('fl_recompress') x;

select ch.schema_name || '.' || ch.table_name as compressed_chunk
from _timescaledb_catalog.chunk ch
inner join _timescaledb_catalog.chunk uc on ch.id = uc.compressed_chunk_id
where uc.hypertable_id = (select id from _timescaledb_catalog.hypertable where table_name = 'fl_recompress')
\gset

select _ts_meta_v2_first_value, _ts_meta_v2_last_value
from :compressed_chunk;

drop table fl_recompress;

-- error cases
\set ON_ERROR_STOP 0

create table fl_errors(ts int, value int, tag text);
select create_hypertable('fl_errors', 'ts', chunk_time_interval => 10000);

-- duplicate column
alter table fl_errors set (timescaledb.compress,
    timescaledb.compress_orderby = 'ts',
    timescaledb.compress_index = 'firstlast("value"), firstlast("value")');

-- invalid column
alter table fl_errors set (timescaledb.compress,
    timescaledb.compress_orderby = 'ts',
    timescaledb.compress_index = 'firstlast("nonexistent")');

-- multi-column not supported
alter table fl_errors set (timescaledb.compress,
    timescaledb.compress_orderby = 'ts',
    timescaledb.compress_index = 'firstlast("value", "tag")');

\set ON_ERROR_STOP 1

drop table fl_errors;

drop view settings;
