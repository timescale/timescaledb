-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- disable hash pushdown so we don't include the hash values in the plans
-- which makes the test stable. the hash pushdown is tested separately
set timescaledb.enable_bloom1_hash_pushdown = false;

CREATE VIEW settings AS SELECT * FROM _timescaledb_catalog.compression_settings ORDER BY upper(relid::text) COLLATE "C";
CREATE VIEW metacols AS select relname,attname,count(*) from pg_attribute a, pg_class c where c.oid=a.attrelid and attname like '%_ts_meta%' and relname like '%chunk' GROUP BY 1,2 ORDER BY relname::text COLLATE "C", attname::text COLLATE "C";
CREATE VIEW compressedcols AS select relname,attname,c.oid as reloid,attnum from pg_attribute a, pg_class c where c.oid=a.attrelid and relname like '%compress_hyper_%' order by c.oid asc, a.attnum asc;

create table sparse(
    ts int,
    o bigint,
    value float,
    boo bigint,
    big1 bigint,
    big2 bigint,
    sby bigint,
    small1 smallint,
    small2 int2,
    num numeric,
    nowts timestamptz,
    hello bytea);
select create_hypertable('sparse', 'ts', create_default_indexes=>false);
insert into sparse select x, x, x, x, x, x, x, x%4, x%4, x::numeric, now()::timestamptz, md5(x::text)::bytea from generate_series(1, 10000) x;
alter table sparse set (
    timescaledb.compress,
    timescaledb.order_by='o',
    timescaledb.segment_by='sby',
    timescaledb.compress_index = 'bloom(big1),bloom(big2),bloom(value),bloom(value,big1,big2),bloom(o,big2),bloom(big1,big2),bloom(boo,big1),bloom(small1,small2),bloom(num,nowts),bloom(num,hello)');

select count(compress_chunk(x)) from show_chunks('sparse') x;
vacuum analyze sparse;

-- smoke tests
select min(big1), max(big2) from sparse where value < 100 and value > 10;
select relname,attname from metacols order by 1,2;
select index from settings where relid = (select oid from pg_class where relname = 'sparse') and index is not null group by 1 order by 1;
select distinct(attname) from compressedcols where relname = 'sparse' order by 1 asc;

-- show plan
explain (buffers off, costs off) select * from sparse where value = 1;
explain (buffers off, costs off) select * from sparse where o = 1 and big2 = 1;
explain (buffers off, costs off) select * from sparse where num = 1 and hello = md5('1')::bytea;
explain (buffers off, costs off) select * from sparse where small1 = 1 and small2 = 1;
explain (buffers off, costs off) select * from sparse where num = 1 and nowts = now()::timestamptz;
explain (buffers off, costs off) select * from sparse where o in (1,2) and big2 = 1;
explain (buffers off, costs off) select * from sparse where o = 1 and big2 in (1,2);
explain (buffers off, costs off) select * from sparse where o in (1,2) and big2 in (1,2);

-- segmentby = bloom
explain (buffers off, costs off) select * from sparse where big1 = sby and value = 1;
explain (buffers off, costs off) select * from sparse where o = sby and big2 = sby;
explain (buffers off, costs off) select * from sparse where o = 1 and big2 = sby;
explain (buffers off, costs off) select * from sparse where o = sby and big2 = 1;

-- create a sister table where the same composite blooms should be auto created
create table sparse_sister as select * from sparse where ts < -1;
select create_hypertable('sparse_sister', 'ts', create_default_indexes=>false);
alter table sparse_sister set (
    timescaledb.compress,
    timescaledb.order_by='o',
    timescaledb.segment_by='sby');
insert into sparse_sister select * from sparse;

create index on sparse_sister(value,big1,big2);
create index on sparse_sister(o,big2);
create index on sparse_sister(big1,big2);
create index on sparse_sister(boo,big1);
create index on sparse_sister(small1,small2);
create index on sparse_sister(num,nowts);
create index on sparse_sister(num,hello);

select count(compress_chunk(x)) from show_chunks('sparse_sister') x;
vacuum analyze sparse_sister;

-- smoke tests
select min(big1), max(big2) from sparse_sister where value < 100 and value > 10;
select relname,attname from metacols order by 1,2;
select index from settings where relid = (select oid from pg_class where relname = 'sparse_sister') and index is not null group by 1 order by 1;
select distinct(attname) from compressedcols where relname = 'sparse_sister' order by 1 asc;

-- show plan
explain (buffers off, costs off) select * from sparse_sister where value = 1;
explain (buffers off, costs off) select * from sparse_sister where o = 1 and big2 = 1;
explain (buffers off, costs off) select * from sparse_sister where num = 1 and hello = md5('1')::bytea;
explain (buffers off, costs off) select * from sparse_sister where small1 = 1 and small2 = 1;
explain (buffers off, costs off) select * from sparse_sister where num = 1 and nowts = now()::timestamptz;

-- segmentby = bloom
explain (buffers off, costs off) select * from sparse_sister where big1 = sby and value = 1;
explain (buffers off, costs off) select * from sparse_sister where o = sby and big2 = sby;
explain (buffers off, costs off) select * from sparse_sister where o = 1 and big2 = sby;
explain (buffers off, costs off) select * from sparse_sister where o = sby and big2 = 1;

-- renaming a column that participates in a composite bloom filter
alter table sparse rename big2 to xxl;
select relname,attname from metacols order by 1,2;
select index from settings where relid = (select oid from pg_class where relname = 'sparse') and index is not null group by 1 order by 1;
select distinct(attname) from compressedcols where relname = 'sparse' order by 1 asc;

-- dropping a column that participates in a composite bloom filter
alter table sparse drop column xxl;
select relname,attname from metacols order by 1,2;
select index from settings where relid = (select oid from pg_class where relname = 'sparse') and index is not null group by 1 order by 1;
select distinct(attname) from compressedcols where relname = 'sparse' order by 1 asc;

-- droppping an orderby column should fail
\set ON_ERROR_STOP 0
alter table sparse drop column o;

-- a segmentby column cannot be part of composite key
alter table sparse set (
    timescaledb.compress,
    timescaledb.order_by='o',
    timescaledb.segment_by='sby',
    timescaledb.compress_index = 'bloom(big1),bloom(value),bloom(boo,big1),bloom(sby,value)');

-- a segmentby column cannot be part of single col bloom filter
alter table sparse set (
    timescaledb.compress,
    timescaledb.order_by='o',
    timescaledb.segment_by='sby',
    timescaledb.compress_index = 'bloom(big1),bloom(value),bloom(boo,big1),bloom(sby)');

-- an orderby column cannot be part of a single col bloom filter
alter table sparse set (
    timescaledb.compress,
    timescaledb.order_by='o',
    timescaledb.segment_by='sby',
    timescaledb.compress_index = 'bloom(big1),bloom(value),bloom(boo,big1),bloom(o)');

-- an orderby column cannot be part of a single col bloom filter
alter table sparse set (
    timescaledb.compress,
    timescaledb.order_by='o',
    timescaledb.segment_by='sby',
    timescaledb.compress_index = 'bloom(big1),bloom(value),bloom(boo,big1),bloom(boo,o),bloom(o)');

-- an orderby column cannot be part of a single col bloom filter
alter table sparse set (
    timescaledb.compress,
    timescaledb.order_by='o',
    timescaledb.segment_by='sby',
    timescaledb.compress_index = 'bloom(big1),bloom(value),bloom(boo,big1),bloom(o),bloom(o,boo)');

\set ON_ERROR_STOP 1

-- an orderby column _can_ be part of a composite bloom filter
alter table sparse set (
    timescaledb.compress,
    timescaledb.order_by='o',
    timescaledb.segment_by='sby',
    timescaledb.compress_index = 'bloom(big1),bloom(value),bloom(boo,big1),bloom(o,boo)');

DROP TABLE IF EXISTS sparse CASCADE;
DROP TABLE IF EXISTS sparse_sister CASCADE;
