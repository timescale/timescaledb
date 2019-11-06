-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SET timescaledb.enable_transparent_decompression to OFF;

\ir include/rand_generator.sql

--test_collation ---
--basic test with count
create table foo (a integer, b integer, c integer, d integer);
select table_name from create_hypertable('foo', 'a', chunk_time_interval=> 10);
create unique index foo_uniq ON foo (a, b);

--note that the "d" order by column is all NULL
insert into foo values( 3 , 16 , 20, NULL);
insert into foo values( 10 , 10 , 20, NULL);
insert into foo values( 20 , 11 , 20, NULL);
insert into foo values( 30 , 12 , 20, NULL);

alter table foo set (timescaledb.compress, timescaledb.compress_segmentby = 'a,b', timescaledb.compress_orderby = 'c desc, d asc nulls last');

--test self-refencing updates
SET timescaledb.enable_transparent_decompression to ON;
update foo set c = 40
where  a = (SELECT max(a) FROM foo);
SET timescaledb.enable_transparent_decompression to OFF;

select id, schema_name, table_name, compressed, compressed_hypertable_id from
_timescaledb_catalog.hypertable order by id;
select * from _timescaledb_catalog.hypertable_compression order by hypertable_id, attname;

-- TEST2 compress-chunk for the chunks created earlier --
select compress_chunk( '_timescaledb_internal._hyper_1_2_chunk');
select tgname , tgtype, tgenabled , relname
from pg_trigger t, pg_class rel
where t.tgrelid = rel.oid and rel.relname like '_hyper_1_2_chunk' order by tgname;
\x
select * from timescaledb_information.compressed_chunk_stats
order by chunk_name limit 2;
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

--TEST2b try complex DML on compressed chunk
create table foo_join ( a integer, newval integer);
select table_name from create_hypertable('foo_join', 'a', chunk_time_interval=> 10);
insert into foo_join select generate_series(0,40, 10), 111;
create table foo_join2 ( a integer, newval integer);
select table_name from create_hypertable('foo_join2', 'a', chunk_time_interval=> 10);
insert into foo_join select generate_series(0,40, 10), 222;
update foo
set b = newval
from foo_join where foo.a = foo_join.a;
update foo
set b = newval
from foo_join where foo.a = foo_join.a and foo_join.a > 10;
--here the chunk gets excluded , so succeeds --
update foo
set b = newval
from foo_join where foo.a = foo_join.a and foo.a > 20;
update foo
set b = (select f1.newval from foo_join f1 left join lateral (select newval as newval2 from  foo_join2 f2 where f1.a= f2.a ) subq on true limit 1);

--upsert test --
insert into foo values(10, 12, 12, 12)
on conflict( a, b)
do update set b = excluded.b;

--TEST2c dml directly on the chunk NOTE update/deletes don't get blocked (TODO)
insert into _timescaledb_internal._hyper_1_2_chunk values(10, 12, 12, 12);
update _timescaledb_internal._hyper_1_2_chunk
set b = 12;
delete from _timescaledb_internal._hyper_1_2_chunk;

--TEST2d decompress the chunk and try DML
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
select * from timescaledb_information.compressed_chunk_stats
where hypertable_name::text like 'conditions'
order by hypertable_name, chunk_name;
select * from timescaledb_information.compressed_hypertable_stats
order by hypertable_name;
vacuum full foo;
vacuum full conditions;
-- After vacuum, table_bytes is 0, but any associated index/toast storage is not
-- completely reclaimed. Sets it at 8K (page size). So a chunk which has
-- been compressed still incurs an overhead of n * 8KB (for every index + toast table) storage on the original uncompressed chunk.
select * from timescaledb_information.hypertable
where table_name like 'foo' or table_name like 'conditions'
order by table_name;
\x

select decompress_chunk(ch1.schema_name|| '.' || ch1.table_name)
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht where ch1.hypertable_id = ht.id and ht.table_name like 'conditions';

SELECT count(*), count(*) = :'ORIGINAL_CHUNK_COUNT' from :CHUNK_NAME;
--check that the compressed chunk is dropped
\set ON_ERROR_STOP 0
SELECT count(*) from :COMPRESSED_CHUNK_NAME;
\set ON_ERROR_STOP 1

--size information is gone too
select count(*)
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht,
_timescaledb_catalog.compression_chunk_size map
where ch1.hypertable_id = ht.id and ht.table_name like 'conditions'
and map.chunk_id = ch1.id;

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
-- SELECT device_id < device_id_2  FROM test_collation;
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

--test datatypes
CREATE TABLE datatype_test(
  time timestamptz NOT NULL,
  int2_column int2,
  int4_column int4,
  int8_column int8,
  float4_column float4,
  float8_column float8,
  date_column date,
  timestamp_column timestamp,
  timestamptz_column timestamptz,
  interval_column interval,
  numeric_column numeric,
  decimal_column decimal,
  text_column text,
  char_column char
);

SELECT create_hypertable('datatype_test','time');
ALTER TABLE datatype_test SET (timescaledb.compress);
INSERT INTO datatype_test VALUES ('2000-01-01',2,4,8,4.0,8.0,'2000-01-01','2001-01-01 12:00','2001-01-01 6:00','1 week', 3.41, 4.2, 'text', 'x');

SELECT compress_chunk(ch1.schema_name|| '.' || ch1.table_name)
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht where ch1.hypertable_id = ht.id
and ht.table_name like 'datatype_test' ORDER BY ch1.id;

SELECT
  attname, alg.name
FROM _timescaledb_catalog.hypertable ht
  INNER JOIN _timescaledb_catalog.hypertable_compression htc ON ht.id=htc.hypertable_id
  INNER JOIN _timescaledb_catalog.compression_algorithm alg ON alg.id=htc.compression_algorithm_id
WHERE ht.table_name='datatype_test'
ORDER BY attname;

--try to compress a hypertable that has a continuous aggregate
CREATE TABLE metrics(time timestamptz, device_id int, v1 float, v2 float);
SELECT create_hypertable('metrics','time');

INSERT INTO metrics SELECT generate_series('2000-01-01'::timestamptz,'2000-01-10','1m'),1,0.25,0.75;

-- check expressions in view definition
CREATE VIEW cagg_expr WITH (timescaledb.continuous)
AS
SELECT
  time_bucket('1d', time) AS time,
  'Const'::text AS Const,
  4.3::numeric AS "numeric",
  first(metrics,time),
  CASE WHEN true THEN 'foo' ELSE 'bar' END,
  COALESCE(NULL,'coalesce'),
  avg(v1) + avg(v2) AS avg1,
  avg(v1+v2) AS avg2
FROM metrics
GROUP BY 1;

SET timescaledb.current_timestamp_mock = '2000-01-10';
REFRESH MATERIALIZED VIEW cagg_expr;
SELECT * FROM cagg_expr ORDER BY time LIMIT 5;

ALTER TABLE metrics set(timescaledb.compress);

-- test rescan in compress chunk dml blocker
CREATE TABLE rescan_test(id integer NOT NULL, t timestamptz NOT NULL, val double precision, PRIMARY KEY(id, t));
SELECT create_hypertable('rescan_test', 't', chunk_time_interval => interval '1 day');

-- compression
ALTER TABLE rescan_test SET (timescaledb.compress, timescaledb.compress_segmentby = 'id');

-- INSERT dummy data
INSERT INTO rescan_test SELECT 1, time, random() FROM generate_series('2000-01-01'::timestamptz, '2000-01-05'::timestamptz, '1h'::interval) g(time);


SELECT count(*) FROM rescan_test;

-- compress first chunk
SELECT compress_chunk(ch1.schema_name|| '.' || ch1.table_name)
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht where ch1.hypertable_id = ht.id
and ht.table_name like 'rescan_test' ORDER BY ch1.id LIMIT 1;

-- count should be equal to count before compression
SELECT count(*) FROM rescan_test;

-- single row update is fine
UPDATE rescan_test SET val = val + 1 WHERE rescan_test.id = 1 AND rescan_test.t = '2000-01-03 00:00:00+00';

-- multi row update via WHERE is fine
UPDATE rescan_test SET val = val + 1 WHERE rescan_test.id = 1 AND rescan_test.t > '2000-01-03 00:00:00+00';

-- single row update with FROM is allowed if no compressed chunks are hit
UPDATE rescan_test SET val = tmp.val
FROM (SELECT x.id, x.t, x.val FROM unnest(array[(1, '2000-01-03 00:00:00+00', 2.045)]::rescan_test[]) AS x) AS tmp
WHERE rescan_test.id = tmp.id AND rescan_test.t = tmp.t AND rescan_test.t >= '2000-01-03';

-- single row update with FROM is blocked
\set ON_ERROR_STOP 0
UPDATE rescan_test SET val = tmp.val
FROM (SELECT x.id, x.t, x.val FROM unnest(array[(1, '2000-01-03 00:00:00+00', 2.045)]::rescan_test[]) AS x) AS tmp
WHERE rescan_test.id = tmp.id AND rescan_test.t = tmp.t;

-- bulk row update with FROM is blocked
UPDATE rescan_test SET val = tmp.val
FROM (SELECT x.id, x.t, x.val FROM unnest(array[(1, '2000-01-03 00:00:00+00', 2.045), (1, '2000-01-03 01:00:00+00', 8.045)]::rescan_test[]) AS x) AS tmp
WHERE rescan_test.id = tmp.id AND rescan_test.t = tmp.t;
\set ON_ERROR_STOP 1


