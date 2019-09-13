-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SET timescaledb.enable_transparent_decompression to OFF;

\ir include/rand_generator.sql

--test_collation ---
--basic test with count
create table foo (a integer, b integer, c integer, d integer);
select table_name from create_hypertable('foo', 'a', chunk_time_interval=> 10);

insert into foo values( 3 , 16 , 20, 11);
insert into foo values( 10 , 10 , 20, 120);
insert into foo values( 20 , 11 , 20, 13);
insert into foo values( 30 , 12 , 20, 14);

alter table foo set (timescaledb.compress, timescaledb.compress_segmentby = 'a,b', timescaledb.compress_orderby = 'c desc, d asc nulls last');
select id, schema_name, table_name, compressed, compressed_hypertable_id from
_timescaledb_catalog.hypertable order by id;
select * from _timescaledb_catalog.hypertable_compression order by hypertable_id, attname;

-- TEST2 compress-chunk for the chunks created earlier --
select compress_chunk( '_timescaledb_internal._hyper_1_2_chunk');
select tgname , tgtype, tgenabled , relname
from pg_trigger t, pg_class rel 
where t.tgrelid = rel.oid and rel.relname like '_hyper_1_2_chunk' order by tgname;
\x
select * from timescaledb_information.compressed_chunk_size;
\x
select compress_chunk( '_timescaledb_internal._hyper_1_1_chunk');
\x
select * from _timescaledb_catalog.compression_chunk_size
order by chunk_id;
\x
select  ch1.id, ch1.schema_name, ch1.table_name ,  ch2.table_name as compress_table
from
_timescaledb_catalog.chunk ch1, _timescaledb_catalog.chunk ch2
where ch1.compressed_chunk_id = ch2.id;

\set ON_ERROR_STOP 0
--cannot recompress the chunk the second time around
select compress_chunk( '_timescaledb_internal._hyper_1_2_chunk');

--TEST2a try DML on a compressed chunk 
insert into foo values( 11 , 10 , 20, 120);
update foo set b =20 where a = 10;
delete from foo where a = 10;

--TEST2b decompress the chunk and try DML
select decompress_chunk( '_timescaledb_internal._hyper_1_2_chunk');
insert into foo values( 11 , 10 , 20, 120);
update foo set b =20 where a = 10;
select * from _timescaledb_internal._hyper_1_2_chunk order by a;
delete from foo where a = 10;
select * from _timescaledb_internal._hyper_1_2_chunk order by a;

-- TEST3 check if compress data from views is accurate
CREATE TABLE conditions (
      time        TIMESTAMPTZ       NOT NULL,
      location    TEXT              NOT NULL,
      location2    char(10)              NOT NULL,
      temperature DOUBLE PRECISION  NULL,
      humidity    DOUBLE PRECISION  NULL
    );
select create_hypertable( 'conditions', 'time', chunk_time_interval=> '31days'::interval);
alter table conditions set (timescaledb.compress, timescaledb.compress_segmentby = 'location', timescaledb.compress_orderby = 'time');
insert into conditions
select generate_series('2018-12-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '1 day'), 'POR', 'klick', 55, 75;
insert into conditions
select generate_series('2018-12-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '1 day'), 'NYC', 'klick', 55, 75;

select hypertable_id, attname, compression_algorithm_id , al.name
from _timescaledb_catalog.hypertable_compression hc,
     _timescaledb_catalog.hypertable ht,
      _timescaledb_catalog.compression_algorithm al
where ht.id = hc.hypertable_id and ht.table_name like 'conditions' and al.id = hc.compression_algorithm_id
ORDER BY hypertable_id, attname;

select attname, attstorage, typname from pg_attribute at, pg_class cl , pg_type ty
where cl.oid = at.attrelid and  at.attnum > 0
and cl.relname = '_compressed_hypertable_4'
and atttypid = ty.oid
order by at.attnum;

SELECT ch1.schema_name|| '.' || ch1.table_name as "CHUNK_NAME", ch1.id "CHUNK_ID"
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht where ch1.hypertable_id = ht.id and ht.table_name like 'conditions'
ORDER BY ch1.id
LIMIT 1 \gset

SELECT count(*) from :CHUNK_NAME;
SELECT count(*) as "ORIGINAL_CHUNK_COUNT" from :CHUNK_NAME \gset

select tableoid::regclass, count(*) from conditions group by tableoid order by tableoid;

select  compress_chunk(ch1.schema_name|| '.' || ch1.table_name)
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht where ch1.hypertable_id = ht.id and ht.table_name like 'conditions' ORDER BY ch1.id limit 1;

--test that only one chunk was affected
--note tables with 0 rows will not show up in here.
select tableoid::regclass, count(*) from conditions group by tableoid order by tableoid;

select  compress_chunk(ch1.schema_name|| '.' || ch1.table_name)
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht where ch1.hypertable_id = ht.id and ht.table_name like 'conditions' and ch1.compressed_chunk_id IS NULL;

select tableoid::regclass, count(*) from conditions group by tableoid order by tableoid;

select  compressed.schema_name|| '.' || compressed.table_name as "COMPRESSED_CHUNK_NAME"
from _timescaledb_catalog.chunk uncompressed, _timescaledb_catalog.chunk compressed
where uncompressed.compressed_chunk_id = compressed.id AND uncompressed.id = :'CHUNK_ID' \gset

SELECT count(*) from :CHUNK_NAME;
SELECT count(*) from :COMPRESSED_CHUNK_NAME;
SELECT sum(_ts_meta_count) from :COMPRESSED_CHUNK_NAME;
SELECT _ts_meta_sequence_num from :COMPRESSED_CHUNK_NAME;

\x
select * from timescaledb_information.compressed_chunk_size
where hypertable_name::text like 'conditions'
order by hypertable_name, chunk_name;
select * from timescaledb_information.compressed_hypertable_size
order by hypertable_name;
\x

select decompress_chunk(ch1.schema_name|| '.' || ch1.table_name)
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht where ch1.hypertable_id = ht.id and ht.table_name like 'conditions';

SELECT count(*), count(*) = :'ORIGINAL_CHUNK_COUNT' from :CHUNK_NAME;
--check that the compressed chunk is dropped
\set ON_ERROR_STOP 0
SELECT count(*) from :COMPRESSED_CHUNK_NAME;
\set ON_ERROR_STOP 1

--size information is gone too
select count(*) from timescaledb_information.compressed_chunk_size
where hypertable_name::text like 'conditions';

--make sure  compressed_chunk_id  is reset to NULL
select ch1.compressed_chunk_id IS NULL
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht where ch1.hypertable_id = ht.id and ht.table_name like 'conditions';

-- test plans get invalidated when chunks get compressed

SET timescaledb.enable_transparent_decompression TO ON;
CREATE TABLE plan_inval(time timestamptz, device_id int);
SELECT create_hypertable('plan_inval','time');
ALTER TABLE plan_inval SET (timescaledb.compress,timescaledb.compress_orderby='time desc');

-- create 2 chunks
INSERT INTO plan_inval SELECT * FROM (VALUES ('2000-01-01'::timestamptz,1), ('2000-01-07'::timestamptz,1)) v(time,device_id);
SET max_parallel_workers_per_gather to 0;
PREPARE prep_plan AS SELECT count(*) FROM plan_inval;
EXECUTE prep_plan;
EXECUTE prep_plan;
EXECUTE prep_plan;
-- get name of first chunk
SELECT tableoid::regclass AS "CHUNK_NAME" FROM plan_inval ORDER BY time LIMIT 1
\gset

SELECT compress_chunk(:'CHUNK_NAME');

EXECUTE prep_plan;
EXPLAIN (COSTS OFF) EXECUTE prep_plan;

CREATE TABLE test_collation (
      time      TIMESTAMPTZ       NOT NULL,
      device_id  TEXT   COLLATE "C"           NULL,
      device_id_2  TEXT  COLLATE "POSIX"           NULL,
      val_1 TEXT  COLLATE "C" NULL,
      val_2 TEXT COLLATE "POSIX"  NULL
    );
--we want all the data to go into 1 chunk. so use 1 year chunk interval
select create_hypertable( 'test_collation', 'time', chunk_time_interval=> '1 day'::interval);

\set ON_ERROR_STOP 0
--forbid setting collation in compression ORDER BY clause. (parse error is fine)
alter table test_collation set (timescaledb.compress, timescaledb.compress_segmentby='device_id, device_id_2', timescaledb.compress_orderby = 'val_1 COLLATE "POSIX", val2, time');
\set ON_ERROR_STOP 1
alter table test_collation set (timescaledb.compress, timescaledb.compress_segmentby='device_id, device_id_2', timescaledb.compress_orderby = 'val_1, val_2, time');

insert into test_collation
select generate_series('2018-01-01 00:00'::timestamp, '2018-01-10 00:00'::timestamp, '2 hour'), 'device_1', 'device_3', gen_rand_minstd(), gen_rand_minstd();
insert into test_collation
select generate_series('2018-01-01 00:00'::timestamp, '2018-01-10 00:00'::timestamp, '2 hour'), 'device_2', 'device_4', gen_rand_minstd(), gen_rand_minstd();
insert into test_collation
select generate_series('2018-01-01 00:00'::timestamp, '2018-01-10 00:00'::timestamp, '2 hour'), NULL, 'device_5', gen_rand_minstd(), gen_rand_minstd();

--compress 2 chunks
SELECT compress_chunk(ch1.schema_name|| '.' || ch1.table_name)
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht where ch1.hypertable_id = ht.id
and ht.table_name like 'test_collation' ORDER BY ch1.id LIMIT 2;

--segment bys are pushed down correctly
EXPLAIN (costs off) SELECT * FROM test_collation WHERE device_id < 'a';
EXPLAIN (costs off) SELECT * FROM test_collation WHERE device_id < 'a' COLLATE "POSIX";

\set ON_ERROR_STOP 0
EXPLAIN (costs off) SELECT * FROM test_collation WHERE device_id COLLATE "POSIX" < device_id_2 COLLATE "C";
SELECT device_id < device_id_2  FROM test_collation;
\set ON_ERROR_STOP 1

--segment meta on order bys pushdown
--should work
EXPLAIN (costs off) SELECT * FROM test_collation WHERE val_1 < 'a';
EXPLAIN (costs off) SELECT * FROM test_collation WHERE val_2 < 'a';
EXPLAIN (costs off) SELECT * FROM test_collation WHERE val_1 < 'a' COLLATE "C";
EXPLAIN (costs off) SELECT * FROM test_collation WHERE val_2 < 'a' COLLATE "POSIX";
--cannot pushdown when op collation does not match column's collation since min/max used different collation than what op needs
EXPLAIN (costs off) SELECT * FROM test_collation WHERE val_1 < 'a' COLLATE "POSIX";
EXPLAIN (costs off) SELECT * FROM test_collation WHERE val_2 < 'a' COLLATE "C";
