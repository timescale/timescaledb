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

select id, schema_name, table_name, compression_state as compressed, compressed_hypertable_id from
_timescaledb_catalog.hypertable order by id;
select * from _timescaledb_catalog.hypertable_compression order by hypertable_id, attname;
select * from timescaledb_information.compression_settings ORDER BY hypertable_name;

-- TEST2 compress-chunk for the chunks created earlier --
select compress_chunk( '_timescaledb_internal._hyper_1_2_chunk');
select tgname , tgtype, tgenabled , relname
from pg_trigger t, pg_class rel
where t.tgrelid = rel.oid and rel.relname like '_hyper_1_2_chunk' order by tgname;
\x
select * from chunk_compression_stats('foo')
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
BEGIN;
insert into foo values( 11 , 10 , 20, 120);
ROLLBACK;
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
SELECT * from foo ORDER BY a,b;

--TEST2c Do DML directly on the chunk.
insert into _timescaledb_internal._hyper_1_2_chunk values(10, 12, 12, 12)
on conflict( a, b)
do update set b = excluded.b + 12;
SELECT * from foo ORDER BY a,b;

update _timescaledb_internal._hyper_1_2_chunk
set b = 12;
delete from _timescaledb_internal._hyper_1_2_chunk;

--TEST2d decompress the chunk and try DML
select decompress_chunk( '_timescaledb_internal._hyper_1_2_chunk');
insert into foo values( 11 , 10 , 20, 120);
update foo set b =20 where a = 10;
select * from _timescaledb_internal._hyper_1_2_chunk order by a,b;
delete from foo where a = 10;
select * from _timescaledb_internal._hyper_1_2_chunk order by a,b;

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
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht where ch1.hypertable_id = ht.id and ht.table_name like 'conditions' and ch1.status & 1 = 0;

select tableoid::regclass, count(*) from conditions group by tableoid order by tableoid;

select  compressed.schema_name|| '.' || compressed.table_name as "COMPRESSED_CHUNK_NAME"
from _timescaledb_catalog.chunk uncompressed, _timescaledb_catalog.chunk compressed
where uncompressed.compressed_chunk_id = compressed.id AND uncompressed.id = :'CHUNK_ID' \gset

SELECT count(*) from :CHUNK_NAME;
SELECT count(*) from :COMPRESSED_CHUNK_NAME;
SELECT sum(_ts_meta_count) from :COMPRESSED_CHUNK_NAME;
SELECT location, _ts_meta_sequence_num from :COMPRESSED_CHUNK_NAME ORDER BY 1,2;

\x
SELECT chunk_id, numrows_pre_compression, numrows_post_compression
FROM _timescaledb_catalog.chunk srcch,
      _timescaledb_catalog.compression_chunk_size map,
     _timescaledb_catalog.hypertable srcht
WHERE map.chunk_id = srcch.id and srcht.id = srcch.hypertable_id
       and srcht.table_name like 'conditions'
order by chunk_id;

select * from chunk_compression_stats('conditions')
order by chunk_name;
select * from hypertable_compression_stats('foo');
select * from hypertable_compression_stats('conditions');
vacuum full foo;
vacuum full conditions;
-- After vacuum, table_bytes is 0, but any associated index/toast storage is not
-- completely reclaimed. Sets it at 8K (page size). So a chunk which has
-- been compressed still incurs an overhead of n * 8KB (for every index + toast table) storage on the original uncompressed chunk.
select pg_size_pretty(table_bytes), pg_size_pretty(index_bytes),
pg_size_pretty(toast_bytes), pg_size_pretty(total_bytes)
from hypertable_detailed_size('conditions');
select * from timescaledb_information.hypertables
where hypertable_name like 'foo' or hypertable_name like 'conditions'
order by hypertable_name;
\x

SELECT decompress_chunk(ch1.schema_name|| '.' || ch1.table_name) AS chunk
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht
WHERE ch1.hypertable_id = ht.id and ht.table_name LIKE 'conditions'
ORDER BY chunk;

SELECT count(*), count(*) = :'ORIGINAL_CHUNK_COUNT' from :CHUNK_NAME;
--check that the compressed chunk is empty
SELECT count(*) from :COMPRESSED_CHUNK_NAME;

--size information is gone too
select count(*)
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht,
_timescaledb_catalog.compression_chunk_size map
where ch1.hypertable_id = ht.id and ht.table_name like 'conditions'
and map.chunk_id = ch1.id;

--make sure  compressed_chunk_id  is reset to NULL
select ch1.status & 1 = 0
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
CREATE OR REPLACE PROCEDURE reindex_compressed_hypertable(hypertable REGCLASS)
AS $$
DECLARE
  hyper_id int;
BEGIN
  SELECT h.compressed_hypertable_id
  INTO hyper_id
  FROM _timescaledb_catalog.hypertable h
  WHERE h.table_name = hypertable::name;
  EXECUTE format('REINDEX TABLE _timescaledb_internal._compressed_hypertable_%s',
    hyper_id);
END $$ LANGUAGE plpgsql;
-- reindexing compressed hypertable to update statistics
CALL reindex_compressed_hypertable('test_collation');

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

--TEST try to compress a hypertable that has a continuous aggregate
CREATE TABLE metrics(time timestamptz, device_id int, v1 float, v2 float);
SELECT create_hypertable('metrics','time');

INSERT INTO metrics SELECT generate_series('2000-01-01'::timestamptz,'2000-01-10','1m'),1,0.25,0.75;

-- check expressions in view definition
CREATE MATERIALIZED VIEW cagg_expr WITH (timescaledb.continuous)
AS
SELECT
  time_bucket('1d', time) AS time,
  'Const'::text AS Const,
  4.3::numeric AS "numeric",
  first(metrics,time),
  CASE WHEN true THEN 'foo' ELSE 'bar' END,
  COALESCE(NULL,'coalesce'),
  avg(v1) + avg(v2) AS avg1,
  avg(v1+v2) AS avg2,
  count(*) AS cnt
FROM metrics
GROUP BY 1 WITH NO DATA;

CALL refresh_continuous_aggregate('cagg_expr', NULL, NULL);
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


-- Test FK constraint drop and recreate during compression and decompression on a chunk

CREATE TABLE meta (device_id INT PRIMARY KEY);
CREATE TABLE hyper(
    time INT NOT NULL,
    device_id INT REFERENCES meta(device_id) ON DELETE CASCADE ON UPDATE CASCADE,
    val INT);
SELECT * FROM create_hypertable('hyper', 'time', chunk_time_interval => 10);
ALTER TABLE hyper SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'time',
    timescaledb.compress_segmentby = 'device_id');
INSERT INTO meta VALUES (1), (2), (3), (4), (5);
INSERT INTO hyper VALUES (1, 1, 1), (2, 2, 1), (3, 3, 1), (10, 3, 2), (11, 4, 2), (11, 5, 2);

SELECT ch1.table_name AS "CHUNK_NAME", ch1.schema_name|| '.' || ch1.table_name AS "CHUNK_FULL_NAME"
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht
WHERE ch1.hypertable_id = ht.id AND ht.table_name LIKE 'hyper'
ORDER BY ch1.id LIMIT 1 \gset

SELECT constraint_schema, constraint_name, table_schema, table_name, constraint_type
FROM information_schema.table_constraints
WHERE table_name = :'CHUNK_NAME' AND constraint_type = 'FOREIGN KEY'
ORDER BY constraint_name;

SELECT compress_chunk(:'CHUNK_FULL_NAME');

SELECT constraint_schema, constraint_name, table_schema, table_name, constraint_type
FROM information_schema.table_constraints
WHERE table_name = :'CHUNK_NAME' AND constraint_type = 'FOREIGN KEY'
ORDER BY constraint_name;

-- Delete data from compressed chunk directly fails
\set ON_ERROR_STOP 0
DELETE FROM hyper WHERE device_id = 3;
\set ON_ERROR_STOP 0

-- Delete data from FK-referenced table deletes data from compressed chunk
SELECT * FROM hyper ORDER BY time, device_id;
DELETE FROM meta WHERE device_id = 3;
SELECT * FROM hyper ORDER BY time, device_id;

SELECT decompress_chunk(:'CHUNK_FULL_NAME');

SELECT constraint_schema, constraint_name, table_schema, table_name, constraint_type
FROM information_schema.table_constraints
WHERE table_name = :'CHUNK_NAME' AND constraint_type = 'FOREIGN KEY'
ORDER BY constraint_name;

-- create hypertable with 2 chunks
CREATE TABLE ht5(time TIMESTAMPTZ NOT NULL);
SELECT create_hypertable('ht5','time');
INSERT INTO ht5 SELECT '2000-01-01'::TIMESTAMPTZ;
INSERT INTO ht5 SELECT '2001-01-01'::TIMESTAMPTZ;

-- compressed chunk stats should not show dropped chunks
ALTER TABLE ht5 SET (timescaledb.compress);
SELECT compress_chunk(i) FROM show_chunks('ht5') i;
SELECT drop_chunks('ht5', newer_than => '2000-01-01'::TIMESTAMPTZ);
select chunk_name from chunk_compression_stats('ht5')
order by chunk_name;

-- Test enabling compression for a table with compound foreign key
-- (Issue https://github.com/timescale/timescaledb/issues/2000)
CREATE TABLE table2(col1 INT, col2 int, primary key (col1,col2));
CREATE TABLE table1(col1 INT NOT NULL, col2 INT);
ALTER TABLE table1 ADD CONSTRAINT fk_table1 FOREIGN KEY (col1,col2) REFERENCES table2(col1,col2);
SELECT create_hypertable('table1','col1', chunk_time_interval => 10);
-- Trying to list an incomplete set of fields of the compound key (should fail with a nice message)
ALTER TABLE table1 SET (timescaledb.compress, timescaledb.compress_segmentby = 'col1');
-- Listing all fields of the compound key should succeed:
ALTER TABLE table1 SET (timescaledb.compress, timescaledb.compress_segmentby = 'col1,col2');
SELECT * FROM timescaledb_information.compression_settings ORDER BY hypertable_name;

-- test delete/update on non-compressed tables involving hypertables with compression
CREATE TABLE uncompressed_ht (
  time timestamptz NOT NULL,
  value double precision,
  series_id integer
);

SELECT table_name FROM create_hypertable ('uncompressed_ht', 'time');

INSERT INTO uncompressed_ht
  VALUES ('2020-04-20 01:01', 100, 1), ('2020-05-20 01:01', 100, 1), ('2020-04-20 01:01', 200, 2);

CREATE TABLE compressed_ht (
  time timestamptz NOT NULL,
  value double precision,
  series_id integer
);

SELECT table_name FROM create_hypertable ('compressed_ht', 'time');

ALTER TABLE compressed_ht SET (timescaledb.compress);

INSERT INTO compressed_ht
  VALUES ('2020-04-20 01:01', 100, 1), ('2020-05-20 01:01', 100, 1);

SELECT compress_chunk (ch1.schema_name || '.' || ch1.table_name)
FROM _timescaledb_catalog.chunk ch1,
  _timescaledb_catalog.hypertable ht
WHERE ch1.hypertable_id = ht.id
  AND ht.table_name LIKE 'compressed_ht'
ORDER BY ch1.id;

BEGIN;
WITH compressed AS (
  SELECT series_id
  FROM compressed_ht
  WHERE time >= '2020-04-17 17:14:24.161989+00'
)
DELETE FROM uncompressed_ht
WHERE series_id IN (SELECT series_id FROM compressed);
ROLLBACK;

-- test delete inside CTE
WITH compressed AS (
  DELETE FROM compressed_ht RETURNING series_id
)
SELECT * FROM uncompressed_ht
WHERE series_id IN (SELECT series_id FROM compressed);

-- test update inside CTE is blocked
WITH compressed AS (
  UPDATE compressed_ht SET value = 0.2 RETURNING *
)
SELECT * FROM uncompressed_ht
WHERE series_id IN (SELECT series_id FROM compressed);

DROP TABLE compressed_ht;
DROP TABLE uncompressed_ht;

-- Test that pg_stats and pg_class stats for uncompressed chunks are correctly updated after compression.
-- Note that approximate_row_count pulls from pg_class
CREATE TABLE stattest(time TIMESTAMPTZ NOT NULL, c1 int);
SELECT create_hypertable('stattest', 'time');
INSERT INTO stattest SELECT '2020/02/20 01:00'::TIMESTAMPTZ + ('1 hour'::interval * v), 250 * v FROM generate_series(0,25) v;
SELECT table_name INTO TEMPORARY temptable FROM _timescaledb_catalog.chunk WHERE hypertable_id = (SELECT id FROM _timescaledb_catalog.hypertable WHERE table_name = 'stattest');
\set statchunk '(select table_name from temptable)'
SELECT * FROM pg_stats WHERE tablename = :statchunk;

ALTER TABLE stattest SET (timescaledb.compress);
SELECT approximate_row_count('stattest');
SELECT compress_chunk(c) FROM show_chunks('stattest') c;
SELECT approximate_row_count('stattest');
-- Uncompressed chunk table is empty since we just compressed the chunk and moved everything to compressed chunk table.
-- reltuples is initially -1 on PG14 before VACUUM/ANALYZE was run
SELECT relpages, CASE WHEN reltuples > 0 THEN reltuples ELSE 0 END as reltuples FROM pg_class WHERE relname = :statchunk;
SELECT histogram_bounds FROM pg_stats WHERE tablename = :statchunk AND attname = 'c1';

SELECT compch.table_name  as "STAT_COMP_CHUNK_NAME"
FROM _timescaledb_catalog.hypertable ht, _timescaledb_catalog.chunk ch
       , _timescaledb_catalog.chunk compch
  WHERE ht.table_name = 'stattest' AND ch.hypertable_id = ht.id
        AND compch.id = ch.compressed_chunk_id AND ch.compressed_chunk_id > 0  \gset

-- reltuples is initially -1 on PG14 before VACUUM/ANALYZE was run
SELECT relpages, CASE WHEN reltuples > 0 THEN reltuples ELSE 0 END as reltuples FROM pg_class WHERE relname = :'STAT_COMP_CHUNK_NAME';

-- Now verify stats are updated on compressed chunk table when we analyze the hypertable.
ANALYZE stattest;
SELECT histogram_bounds FROM pg_stats WHERE tablename = :statchunk AND attname = 'c1';
-- Unfortunately, the stats on the hypertable won't find any rows to sample from the chunk
SELECT histogram_bounds FROM pg_stats WHERE tablename = 'stattest' AND attname = 'c1';
SELECT relpages, reltuples FROM pg_class WHERE relname = :statchunk;

-- verify that corresponding compressed chunk table stats is updated as well.
SELECT relpages, reltuples FROM pg_class WHERE relname = :'STAT_COMP_CHUNK_NAME';

-- Verify partial chunk stats are handled correctly when analyzing
-- for both uncompressed and compressed chunk tables
INSERT INTO stattest SELECT '2020/02/20 01:00'::TIMESTAMPTZ + ('1 hour'::interval * v), 250 * v FROM generate_series(25,50) v;
ANALYZE stattest;
SELECT histogram_bounds FROM pg_stats WHERE tablename = :statchunk AND attname = 'c1';
-- Hypertable will now see the histogram bounds since we have data in the uncompressed chunk table.
SELECT histogram_bounds FROM pg_stats WHERE tablename = 'stattest' AND attname = 'c1';
-- verify that corresponding uncompressed chunk table stats is updated as well.
SELECT relpages, reltuples FROM pg_class WHERE relname = :statchunk;
-- verify that corresponding compressed chunk table stats have not changed since
-- we didn't compress anything new.
SELECT relpages, reltuples FROM pg_class WHERE relname = :'STAT_COMP_CHUNK_NAME';


-- Verify that decompressing the chunk restores autoanalyze to the hypertable's setting
SELECT reloptions FROM pg_class WHERE relname = :statchunk;
SELECT decompress_chunk(c) FROM show_chunks('stattest') c;
SELECT reloptions FROM pg_class WHERE relname = :statchunk;
SELECT compress_chunk(c) FROM show_chunks('stattest') c;
SELECT reloptions FROM pg_class WHERE relname = :statchunk;
ALTER TABLE stattest SET (autovacuum_enabled = false);
SELECT decompress_chunk(c) FROM show_chunks('stattest') c;
SELECT reloptions FROM pg_class WHERE relname = :statchunk;

-- Verify that even a global analyze works as well, changing message scope here
-- to hide WARNINGs for skipped tables
SET client_min_messages TO ERROR;
ANALYZE;
SET client_min_messages TO NOTICE;
SELECT histogram_bounds FROM pg_stats WHERE tablename = :statchunk and attname = 'c1';
SELECT relpages, reltuples FROM pg_class WHERE relname = :statchunk;

DROP TABLE stattest;

--- Test that analyze on compression internal table updates stats on original chunks
CREATE TABLE stattest2(time TIMESTAMPTZ NOT NULL, c1 int, c2 int);
SELECT create_hypertable('stattest2', 'time', chunk_time_interval=>'1 day'::interval);
ALTER TABLE stattest2 SET (timescaledb.compress, timescaledb.compress_segmentby='c1');
INSERT INTO stattest2 SELECT '2020/06/20 01:00'::TIMESTAMPTZ ,1 , generate_series(1, 200, 1);
INSERT INTO stattest2 SELECT '2020/07/20 01:00'::TIMESTAMPTZ ,1 , generate_series(1, 200, 1);

SELECT  compress_chunk(ch1.schema_name|| '.' || ch1.table_name)
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht
WHERE ch1.hypertable_id = ht.id and ht.table_name like 'stattest2'
 ORDER BY ch1.id limit 1;

-- reltuples is initially -1 on PG14 before VACUUM/ANALYZE has been run
SELECT relname, CASE WHEN reltuples > 0 THEN reltuples ELSE 0 END AS reltuples, relpages, relallvisible FROM pg_class
 WHERE relname in ( SELECT ch.table_name FROM
                   _timescaledb_catalog.chunk ch, _timescaledb_catalog.hypertable ht
  WHERE ht.table_name = 'stattest2' AND ch.hypertable_id = ht.id )
order by relname;

\c :TEST_DBNAME :ROLE_SUPERUSER

--overwrite pg_class stats for the compressed chunk.
UPDATE pg_class
SET reltuples = 0, relpages = 0
 WHERE relname in ( SELECT ch.table_name FROM
    _timescaledb_catalog.chunk ch,
    _timescaledb_catalog.hypertable ht
  WHERE ht.table_name = 'stattest2' AND ch.hypertable_id = ht.id
        AND ch.compressed_chunk_id > 0 );
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

-- reltuples is initially -1 on PG14 before VACUUM/ANALYZE has been run
SELECT relname, CASE WHEN reltuples > 0 THEN reltuples ELSE 0 END AS reltuples, relpages, relallvisible FROM pg_class
 WHERE relname in ( SELECT ch.table_name FROM
                   _timescaledb_catalog.chunk ch, _timescaledb_catalog.hypertable ht
  WHERE ht.table_name = 'stattest2' AND ch.hypertable_id = ht.id )
order by relname;

SELECT '_timescaledb_internal.' || compht.table_name as "STAT_COMP_TABLE",
             compht.table_name  as "STAT_COMP_TABLE_NAME"
FROM _timescaledb_catalog.hypertable ht, _timescaledb_catalog.hypertable compht
WHERE ht.table_name = 'stattest2' AND ht.compressed_hypertable_id = compht.id \gset

--analyze the compressed table, will update stats for the raw table.
ANALYZE :STAT_COMP_TABLE;

-- reltuples is initially -1 on PG14 before VACUUM/ANALYZE has been run
SELECT relname, CASE WHEN reltuples > 0 THEN reltuples ELSE 0 END AS reltuples, relpages, relallvisible FROM pg_class
 WHERE relname in ( SELECT ch.table_name FROM
                   _timescaledb_catalog.chunk ch, _timescaledb_catalog.hypertable ht
  WHERE ht.table_name = 'stattest2' AND ch.hypertable_id = ht.id )
ORDER BY relname;

SELECT relname, reltuples, relpages, relallvisible FROM pg_class
 WHERE relname in ( SELECT ch.table_name FROM
                   _timescaledb_catalog.chunk ch, _timescaledb_catalog.hypertable ht
  WHERE ht.table_name = :'STAT_COMP_TABLE_NAME' AND ch.hypertable_id = ht.id )
ORDER BY relname;

--analyze on stattest2 should not overwrite
ANALYZE stattest2;
SELECT relname, reltuples, relpages, relallvisible FROM pg_class
 WHERE relname in ( SELECT ch.table_name FROM
                   _timescaledb_catalog.chunk ch, _timescaledb_catalog.hypertable ht
  WHERE ht.table_name = 'stattest2' AND ch.hypertable_id = ht.id )
ORDER BY relname;

SELECT relname, reltuples, relpages, relallvisible FROM pg_class
 WHERE relname in ( SELECT ch.table_name FROM
                   _timescaledb_catalog.chunk ch, _timescaledb_catalog.hypertable ht
  WHERE ht.table_name = :'STAT_COMP_TABLE_NAME' AND ch.hypertable_id = ht.id )
ORDER BY relname;

-- analyze on compressed hypertable should restore stats

-- Test approximate_row_count() with compressed hypertable
--
CREATE TABLE approx_count(time timestamptz not null, device int, temp float);
SELECT create_hypertable('approx_count', 'time');
INSERT INTO approx_count SELECT t, (abs(timestamp_hash(t::timestamp)) % 10) + 1, random()*80
FROM generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-04 1:00', '1 hour') t;
SELECT count(*) FROM approx_count;
ALTER TABLE approx_count SET (timescaledb.compress, timescaledb.compress_segmentby='device', timescaledb.compress_orderby = 'time DESC');
SELECT approximate_row_count('approx_count');
ANALYZE approx_count;
SELECT approximate_row_count('approx_count');
DROP TABLE approx_count;

--TEST drop_chunks from a compressed hypertable (that has caggs defined).
-- chunk metadata is still retained. verify correct status for chunk
SELECT count(*)
FROM (SELECT compress_chunk(ch) FROM show_chunks('metrics') ch ) q;
SELECT drop_chunks('metrics', older_than=>'1 day'::interval);
SELECT
   c.table_name as chunk_name,
   c.status as chunk_status, c.dropped, c.compressed_chunk_id as comp_id
FROM _timescaledb_catalog.hypertable h, _timescaledb_catalog.chunk c
WHERE h.id = c.hypertable_id and h.table_name = 'metrics'
ORDER BY 1;
SELECT "time", cnt  FROM cagg_expr ORDER BY time LIMIT 5;

--now reload data into the dropped chunks region, then compress
-- then verify chunk status/dropped column
INSERT INTO metrics SELECT generate_series('2000-01-01'::timestamptz,'2000-01-10','1m'),1,0.25,0.75;
SELECT count(*)
FROM (SELECT compress_chunk(ch) FROM show_chunks('metrics') ch) q;
SELECT
   c.table_name as chunk_name,
   c.status as chunk_status, c.dropped, c.compressed_chunk_id as comp_id
FROM _timescaledb_catalog.hypertable h, _timescaledb_catalog.chunk c
WHERE h.id = c.hypertable_id and h.table_name = 'metrics'
ORDER BY 1;

SELECT count(*) FROM metrics;

-- test sequence number is local to segment by
CREATE TABLE local_seq(time timestamptz, device int);
SELECT table_name FROM create_hypertable('local_seq','time');
ALTER TABLE local_seq SET(timescaledb.compress,timescaledb.compress_segmentby='device');

INSERT INTO local_seq SELECT '2000-01-01',1 FROM generate_series(1,3000);
INSERT INTO local_seq SELECT '2000-01-01',2 FROM generate_series(1,3500);
INSERT INTO local_seq SELECT '2000-01-01',3 FROM generate_series(1,3000);
INSERT INTO local_seq SELECT '2000-01-01',4 FROM generate_series(1,3000);
INSERT INTO local_seq SELECT '2000-01-01', generate_series(5,8);

SELECT compress_chunk(c) FROM show_chunks('local_seq') c;

SELECT
	format('%s.%s',chunk.schema_name,chunk.table_name) AS "COMP_CHUNK"
FROM _timescaledb_catalog.hypertable ht
  INNER JOIN _timescaledb_catalog.hypertable ht_comp ON ht_comp.id = ht.compressed_hypertable_id
  INNER JOIN _timescaledb_catalog.chunk ON chunk.hypertable_id = ht_comp.id
WHERE ht.table_name = 'local_seq' \gset

SELECT device, _ts_meta_sequence_num, _ts_meta_count FROM :COMP_CHUNK ORDER BY 1,2;

-- github issue 4872
-- If subplan of ConstraintAwareAppend is TidRangeScan, then SELECT on
-- hypertable fails with error "invalid child of chunk append: Node (26)"
CREATE TABLE tidrangescan_test(time timestamptz, device_id int, v1 float, v2 float);
SELECT create_hypertable('tidrangescan_test','time');
INSERT INTO tidrangescan_test SELECT generate_series('2000-01-01'::timestamptz,'2000-01-10','1m'),1,0.25,0.75;

CREATE MATERIALIZED VIEW tidrangescan_expr WITH (timescaledb.continuous)
 AS
 SELECT
   time_bucket('1d', time) AS time,
   'Const'::text AS Const,
   4.3::numeric AS "numeric",
   first(tidrangescan_test,time),
   CASE WHEN true THEN 'foo' ELSE 'bar' END,
   COALESCE(NULL,'coalesce'),
   avg(v1) + avg(v2) AS avg1,
   avg(v1+v2) AS avg2,
   count(*) AS cnt
 FROM tidrangescan_test
 WHERE ctid < '(1,1)'::tid GROUP BY 1 WITH NO DATA;

CALL refresh_continuous_aggregate('tidrangescan_expr', NULL, NULL);
SET timescaledb.enable_chunk_append to off;
SET enable_indexscan to off;
SELECT time, const, numeric,first, avg1, avg2 FROM tidrangescan_expr ORDER BY time LIMIT 5;
RESET timescaledb.enable_chunk_append;
RESET enable_indexscan;


-- Test the number of allocated parallel workers for decompression

-- Test that a parallel plan is generated
-- with different number of parallel workers
CREATE TABLE f_sensor_data(
      time timestamptz NOT NULL,
      sensor_id integer NOT NULL,
      cpu double precision NULL,
      temperature double precision NULL
    );

SELECT FROM create_hypertable('f_sensor_data','time');
SELECT set_chunk_time_interval('f_sensor_data', INTERVAL '1 year');

-- Create one chunk manually to ensure, all data is inserted into one chunk
SELECT * FROM _timescaledb_functions.create_chunk('f_sensor_data',' {"time": [181900977000000, 515024000000000]}');

INSERT INTO f_sensor_data
SELECT
    time AS time,
    sensor_id,
    100.0,
    36.6
FROM
    generate_series('1980-01-01 00:00'::timestamp, '1980-02-28 12:00', INTERVAL '1 day') AS g1(time),
    generate_series(1, 1700, 1 ) AS g2(sensor_id)
ORDER BY
    time;

ALTER TABLE f_sensor_data SET (timescaledb.compress, timescaledb.compress_segmentby='sensor_id' ,timescaledb.compress_orderby = 'time DESC');

SELECT compress_chunk(i) FROM show_chunks('f_sensor_data') i;
CALL reindex_compressed_hypertable('f_sensor_data');

-- Encourage use of parallel plans
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size TO '0';

\set explain 'EXPLAIN (VERBOSE, COSTS OFF)'

SHOW min_parallel_table_scan_size;
SHOW max_parallel_workers;
SHOW max_parallel_workers_per_gather;

SET max_parallel_workers_per_gather = 4;
SHOW max_parallel_workers_per_gather;

-- We disable enable_parallel_append here to ensure
-- that we create the same query plan in all PG 14.X versions
SET enable_parallel_append = false;

:explain
SELECT sum(cpu) FROM f_sensor_data;

-- Encourage use of Index Scan

SET enable_seqscan = false;
SET enable_indexscan = true;
SET min_parallel_index_scan_size = 0;
SET min_parallel_table_scan_size = 0;

CREATE INDEX ON f_sensor_data (time, sensor_id);
:explain
SELECT * FROM f_sensor_data WHERE sensor_id > 100;

RESET enable_parallel_append;

-- Test for partially compressed chunks

INSERT INTO f_sensor_data
SELECT
    time AS time,
    sensor_id,
    100.0,
    36.6
FROM
    generate_series('1980-01-01 00:00'::timestamp, '1980-01-30 12:00', INTERVAL '1 day') AS g1(time),
    generate_series(1700, 1800, 1 ) AS g2(sensor_id)
ORDER BY
    time;

:explain
SELECT sum(cpu) FROM f_sensor_data;

:explain
SELECT * FROM f_sensor_data WHERE sensor_id > 100;


-- Test non-partial paths below append are not executed multiple times
CREATE TABLE ts_device_table(time INTEGER, device INTEGER, location INTEGER, value INTEGER);
CREATE UNIQUE INDEX device_time_idx on ts_device_table(time, device);
SELECT create_hypertable('ts_device_table', 'time', chunk_time_interval => 1000);
INSERT INTO ts_device_table SELECT generate_series(0,999,1), 1, 100, 20;
ALTER TABLE ts_device_table set(timescaledb.compress, timescaledb.compress_segmentby='location', timescaledb.compress_orderby='time');
SELECT compress_chunk(i) AS chunk_name FROM show_chunks('ts_device_table') i \gset

SELECT count(*) FROM ts_device_table;
SELECT count(*) FROM :chunk_name;

INSERT INTO ts_device_table VALUES (1, 1, 100, 100) ON CONFLICT DO NOTHING;

SELECT count(*) FROM :chunk_name;

SET parallel_setup_cost TO '0';
SET parallel_tuple_cost TO '0';
SET min_parallel_table_scan_size TO '8';
SET min_parallel_index_scan_size TO '8';
SET random_page_cost TO '0';

SELECT count(*) FROM :chunk_name;

ANALYZE :chunk_name;

SELECT count(*) FROM :chunk_name;


-- Test that parallel plans are chosen even if partial and small chunks are involved
RESET min_parallel_index_scan_size;
RESET min_parallel_table_scan_size;

CREATE TABLE ht_metrics_partially_compressed(time timestamptz, device int, value float);
SELECT create_hypertable('ht_metrics_partially_compressed','time',create_default_indexes:=false);
ALTER TABLE ht_metrics_partially_compressed SET (timescaledb.compress, timescaledb.compress_segmentby='device');

INSERT INTO ht_metrics_partially_compressed
SELECT time, device, device * 0.1 FROM
   generate_series('2020-01-01'::timestamptz,'2020-01-02'::timestamptz, INTERVAL '1 m') g(time),
   LATERAL (SELECT generate_series(1,2) AS device) g2;

SELECT compress_chunk(c) FROM show_chunks('ht_metrics_partially_compressed') c;

INSERT INTO ht_metrics_partially_compressed VALUES ('2020-01-01'::timestamptz, 1, 0.1);

:explain
SELECT * FROM ht_metrics_partially_compressed ORDER BY time DESC, device LIMIT 1;
