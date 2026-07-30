-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Shared helper views used across the composite bloom filter tests.
CREATE VIEW settings AS SELECT * FROM _timescaledb_catalog.compression_settings ORDER BY upper(relid::text) COLLATE "C";
CREATE VIEW metacols AS select h.table_name as relname, a.attname, count(*) from _timescaledb_catalog.compression_settings cs join _timescaledb_catalog.chunk ch on cs.relid = ch.relid join _timescaledb_catalog.hypertable h on ch.hypertable_id = h.id join pg_attribute a on a.attrelid = cs.compress_relid where a.attname like '%_ts_meta%' GROUP BY 1,2 ORDER BY h.table_name COLLATE "C", a.attname::text COLLATE "C";
CREATE VIEW compressedcols AS select h.table_name as relname, a.attname, cs.compress_relid as reloid, a.attnum from _timescaledb_catalog.compression_settings cs join _timescaledb_catalog.chunk ch on cs.relid = ch.relid join _timescaledb_catalog.hypertable h on ch.hypertable_id = h.id join pg_attribute a on a.attrelid = cs.compress_relid and not a.attisdropped order by cs.compress_relid asc, a.attnum asc;

-------------------------------------------------------------------
-- Basics
-------------------------------------------------------------------

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

-------------------------------------------------------------------
-- Config
-------------------------------------------------------------------

CREATE TABLE t(a int, b int, c int, d int, e int, f int, g int, h int, i int);
SELECT create_hypertable('t', 'a');


-- Should succeed (8 columns max)
ALTER TABLE t SET (timescaledb.compress, timescaledb.compress_orderby = 'b', timescaledb.compress_index = 'bloom("a","b","c","d","e","f","g","h")');

select relid,compress_relid,segmentby,orderby,orderby_desc,orderby_nullsfirst,index from settings
where relid = 't'::regclass and index is not null order by 1,2;

-- Should fail (9 columns)
\set ON_ERROR_STOP 0
ALTER TABLE t SET (timescaledb.compress, timescaledb.compress_orderby = 'b', timescaledb.compress_index = 'bloom("a","b","c","d","e","f","g","h","i")');
\set ON_ERROR_STOP 1

-- The ordering of bloom columns in the composite bloom index should be determined by the order of the columns in the CREATE TABLE statement
ALTER TABLE t SET (timescaledb.compress, timescaledb.compress_orderby = 'b', timescaledb.compress_index = 'bloom("h","g","f","e","d","c","b")');
select relid,compress_relid,segmentby,orderby,orderby_desc,orderby_nullsfirst,index from settings
where relid = 't'::regclass and index is not null order by 1,2;

-- Creating two composite bloom indexes with the same columns in different orders should fail
\set ON_ERROR_STOP 0
ALTER TABLE t SET (timescaledb.compress, timescaledb.compress_orderby = 'b', timescaledb.compress_index = 'bloom("h","g","f"),bloom("f","g","h")');
\set ON_ERROR_STOP 1

-- Creating a composite bloom index with the same columns should fail
\set ON_ERROR_STOP 0
ALTER TABLE t SET (timescaledb.compress_index = 'bloom("a","b","a")');
ALTER TABLE t SET (timescaledb.compress_index = 'bloom("g","g")');
\set ON_ERROR_STOP 1

DROP TABLE t CASCADE;

-- Creating a composite bloom index based on a primary key, and create the same manually again
CREATE TABLE u(a int, b int, c int, d int, PRIMARY KEY (a, b, c));
SELECT create_hypertable('u', 'a');
ALTER TABLE u SET (timescaledb.compress, timescaledb.compress_orderby = 'b');
select index from settings where relid = 'u'::regclass and index is not null order by 1;
INSERT INTO u VALUES (1, 2, 3, 4), (5, 6, 7, 8);

select count(compress_chunk(x)) from show_chunks('u') x;
select relid,compress_relid,segmentby,orderby,orderby_desc,orderby_nullsfirst,index from settings where index is not null order by 1,2;

-- Check the auto generated compressed columns
select relname,attname from compressedcols order by 1,2;

ALTER TABLE u SET (timescaledb.compress_index = 'bloom("a","b","c")');
select relid,compress_relid,segmentby,orderby,orderby_desc,orderby_nullsfirst,index from settings where index is not null order by 1,2;

-- Also in a different order
ALTER TABLE u SET (timescaledb.compress_index = 'bloom("c","b","a")');
select relid,compress_relid,segmentby,orderby,orderby_desc,orderby_nullsfirst,index from settings where index is not null order by 1,2;

DROP TABLE u CASCADE;

-- Long column names tests
CREATE TABLE v(a_01234567890123456789 int, b_01234567890123456789 int, c_01234567890123456789 int, d_01234567890123456789 int, e_01234567890123456789 int, f_01234567890123456789 int, g_01234567890123456789 int, h_01234567890123456789 int, i_01234567890123456789 int, primary key (a_01234567890123456789, b_01234567890123456789, c_01234567890123456789, d_01234567890123456789, e_01234567890123456789));
SELECT create_hypertable('v', 'a_01234567890123456789');

ALTER TABLE v SET (timescaledb.compress, timescaledb.compress_orderby = 'b_01234567890123456789');
select relid,compress_relid,segmentby,orderby,orderby_desc,orderby_nullsfirst,index from settings where index is not null order by 1,2;

INSERT INTO v VALUES (1, 2, 3, 4, 5, 6, 7, 8, 9);
select count(compress_chunk(x)) from show_chunks('v') x;

-- Check the auto generated composite bloom index configuration and column names
select relid,compress_relid,segmentby,orderby,orderby_desc,orderby_nullsfirst,index from settings where index is not null order by 1,2;
select relname,attname from compressedcols order by 1,2;

-- Make sure the duplicate column detection works for long column names
\set ON_ERROR_STOP 0
ALTER TABLE v SET (timescaledb.compress_index = 'bloom("a_01234567890123456789","b_01234567890123456789","c_01234567890123456789","a_01234567890123456789")');
\set ON_ERROR_STOP 1

DROP TABLE v CASCADE;

-------------------------------------------------------------------
-- Naming
-------------------------------------------------------------------

-- Testcase for issue #9578: the composite bloom filter naming
-- allowed to generate the same column name for ('a_b', 'c') and ('a', 'b_c').

CREATE TABLE t (
      ts timestamptz NOT NULL,
      a_b int,
      c   int,
      a   int,
      b_c int
);

SELECT create_hypertable('t', 'ts');

ALTER TABLE t SET (
      timescaledb.compress,
      timescaledb.compress_orderby = 'ts',
      timescaledb.compress_index = 'bloom(a_b, c), bloom(a, b_c)'
);

INSERT INTO t VALUES ('2024-01-01', 1, 2, 3, 4);

SELECT compress_chunk(c) FROM show_chunks('t') c;

-- If the below error doesn't appear, then the bugfix worked:
-- ERROR:  column "_ts_meta_v2_bloomh_a_b_c" specified more than once

-- Check the bloom column names in the compressed chunk
SELECT
      h.table_name AS relname,
      a.attname
FROM
      _timescaledb_catalog.compression_settings cs
      JOIN _timescaledb_catalog.chunk ch ON cs.relid = ch.relid
      JOIN _timescaledb_catalog.hypertable h ON ch.hypertable_id = h.id
      JOIN pg_attribute a ON a.attrelid = cs.compress_relid
WHERE
      a.attname LIKE '%_ts_meta%bloom%a_b_c%'
ORDER BY
      h.table_name COLLATE "C",
      a.attname::text COLLATE "C";

-- Verify the assumption that the zero byte separator used in the
-- bloom column names is not allowed in Postgres column names:
-- 'a' || chr(0) || 'b' = 'a\u0000b'

\set ON_ERROR_STOP 0
CREATE TABLE t2 (
      ts timestamptz NOT NULL,
      U&"a\0000b" int,
      c int,
      a int,
      U&"b\0000c" int);

CREATE TABLE t3 (
      ts timestamptz NOT NULL,
      E'a\000b' int,
      c int,
      a int,
      E'b\000c' int);

DO $$
BEGIN
  EXECUTE format('CREATE TABLE t4 (%I int)', E'a\000b');
END $$;

\set ON_ERROR_STOP 1

DROP TABLE t CASCADE;

-------------------------------------------------------------------
-- Manual config change between chunk compressions
-------------------------------------------------------------------
CREATE TABLE mixed_avail_manual(ts timestamptz, a int, b int, c int, seg int);
SELECT create_hypertable('mixed_avail_manual', by_range('ts', interval '1 day'));

-- Initial config: bloom(a,b)
ALTER TABLE mixed_avail_manual SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'seg',
    timescaledb.compress_orderby = 'ts',
    timescaledb.compress_index = 'bloom(a,b)'
);

INSERT INTO mixed_avail_manual
SELECT ts, (i % 10), (i % 5), (i % 3), 1
FROM generate_series('2024-01-01'::timestamptz, '2024-01-03', interval '1 hour') ts,
     generate_series(1, 100) i;

-- Compress first chunk with bloom(a,b)
SELECT compress_chunk(c) FROM show_chunks('mixed_avail_manual') c LIMIT 1;

SELECT COUNT(*) FROM mixed_avail_manual WHERE a = 1 AND b = 2;
SELECT COUNT(*) FROM mixed_avail_manual WHERE a = 1 AND c = 2;
SELECT COUNT(*) FROM mixed_avail_manual WHERE b = 1 AND c = 2;

-- Before config changes
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT * FROM mixed_avail_manual WHERE a = 1 AND b = 2
ORDER BY 1,2,3,4;

EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT * FROM mixed_avail_manual WHERE a = 1 AND c = 2
ORDER BY 1,2,3,4;

EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT * FROM mixed_avail_manual WHERE b = 1 AND c = 2
ORDER BY 1,2,3,4;

-- Change config: bloom(a,c)
ALTER TABLE mixed_avail_manual SET (
    timescaledb.compress_index = 'bloom(a,c)'
);

-- Compress second chunk with bloom(a,c)
SELECT compress_chunk(c) FROM show_chunks('mixed_avail_manual') c OFFSET 1 LIMIT 1;

-- Change config: bloom(b,c)
ALTER TABLE mixed_avail_manual SET (
    timescaledb.compress_index = 'bloom(b,c)'
);

-- Compress third chunk with bloom(b,c)
SELECT compress_chunk(c) FROM show_chunks('mixed_avail_manual') c OFFSET 2;

SELECT COUNT(*) FROM mixed_avail_manual WHERE a = 1 AND b = 2;
SELECT COUNT(*) FROM mixed_avail_manual WHERE a = 1 AND c = 2;
SELECT COUNT(*) FROM mixed_avail_manual WHERE b = 1 AND c = 2;

-- After config changes
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT * FROM mixed_avail_manual WHERE a = 1 AND b = 2
ORDER BY 1,2,3,4;

EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT * FROM mixed_avail_manual WHERE a = 1 AND c = 2
ORDER BY 1,2,3,4;

EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT * FROM mixed_avail_manual WHERE b = 1 AND c = 2
ORDER BY 1,2,3,4;

DROP TABLE IF EXISTS mixed_avail_manual CASCADE;

-------------------------------------------------------------------
-- Index added after partial compression
-------------------------------------------------------------------

CREATE TABLE mixed_avail_add(ts timestamptz, a int, b int, seg int);
SELECT create_hypertable('mixed_avail_add', by_range('ts', interval '1 day'));

ALTER TABLE mixed_avail_add SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'seg',
    timescaledb.compress_orderby = 'ts'
);

-- Insert and compress first 2 chunks (no composite bloom)
INSERT INTO mixed_avail_add
SELECT ts, (i % 10), (i % 5), 1
FROM generate_series('2024-01-01'::timestamptz, '2024-01-03', interval '1 hour') ts,
     generate_series(1, 100) i;

SELECT compress_chunk(c) FROM show_chunks('mixed_avail_add') c LIMIT 2;

-- Before index add
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT * FROM mixed_avail_add WHERE a = 1 AND b = 2
ORDER BY 1,2,3,4;

-- Create index
CREATE INDEX idx_ab ON mixed_avail_add (a, b);

-- Compress last chunk (will have composite bloom from new index)
SELECT compress_chunk(c) FROM show_chunks('mixed_avail_add') c OFFSET 2;

-- Query should work on all chunks
SELECT COUNT(*) FROM mixed_avail_add WHERE a = 1 AND b = 2;

-- After index add
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT * FROM mixed_avail_add WHERE a = 1 AND b = 2
ORDER BY 1,2,3,4;

DROP TABLE IF EXISTS mixed_avail_add CASCADE;

-------------------------------------------------------------------
-- Some chunks don't have a composite bloom index after index change
-------------------------------------------------------------------

CREATE TABLE mixed_avail_drop(ts timestamptz, a int, b int, seg int);
SELECT create_hypertable('mixed_avail_drop', by_range('ts', interval '1 day'));

-- Create index
CREATE INDEX idx_ab ON mixed_avail_drop (a, b);

ALTER TABLE mixed_avail_drop SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'seg',
    timescaledb.compress_orderby = 'ts'
);

-- Insert data for 3 days (3 chunks)
INSERT INTO mixed_avail_drop
SELECT ts, (i % 10), (i % 5), 1
FROM generate_series('2024-01-01'::timestamptz, '2024-01-03', interval '1 hour') ts,
     generate_series(1, 100) i;

-- Compress first 2 chunks (will have composite bloom from index)
SELECT compress_chunk(c) FROM show_chunks('mixed_avail_drop') c LIMIT 2;

-- Before index drop
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT * FROM mixed_avail_drop WHERE a = 1 AND b = 2
ORDER BY 1,2,3,4;

-- Drop index
DROP INDEX idx_ab;

-- Compress last chunk (no composite bloom, index is gone)
SELECT compress_chunk(c) FROM show_chunks('mixed_avail_drop') c OFFSET 2;

-- Query should work on all chunks
SELECT COUNT(*) FROM mixed_avail_drop WHERE a = 1 AND b = 2;

-- After index drop
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT * FROM mixed_avail_drop WHERE a = 1 AND b = 2
ORDER BY 1,2,3,4;

DROP TABLE IF EXISTS mixed_avail_drop CASCADE;

-------------------------------------------------------------------
-- Hash pushdown
-------------------------------------------------------------------

-- all tests here will frst run with hash pushdown disabled and then
-- enabled for comparison

create table hash_pushdown_test(a int, b int, c int, d int)
with (
    tsdb.hypertable,
    tsdb.compress,
    tsdb.partition_column = 'c',
    tsdb.segmentby = '',
    tsdb.orderby = 'd',
    tsdb.compress_index = 'bloom(a), bloom(a,b)'
);

insert into hash_pushdown_test select x, x, x, x from generate_series(1, 10000) x;
select count(compress_chunk(x)) from show_chunks('hash_pushdown_test') x;
vacuum full analyze hash_pushdown_test;

-- single bloom
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
select * from hash_pushdown_test where a = 1;

-- composite bloom
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
select * from hash_pushdown_test where a = 1 and b = 2;

-- saop with single bloom
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
select * from hash_pushdown_test where a = any(array[1,2,3]);

EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
select * from hash_pushdown_test where a IN (1,2,3);

-- saop with composite bloom
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
select * from hash_pushdown_test where a = any(array[1,2,3]) and b = any(array[4,5,6]);

EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
select * from hash_pushdown_test where a IN (1,2,3) and b IN (4,5,6);

drop table if exists hash_pushdown_test;

-------------------------------------------------------------------
-- Upsert
-------------------------------------------------------------------

-- Explain test
CREATE TABLE explain_test(ts timestamptz, device_id int, metric text, value float);
SELECT create_hypertable('explain_test', by_range('ts'));
CREATE UNIQUE INDEX idx_explain ON explain_test(device_id, metric, ts);
ALTER TABLE explain_test SET (timescaledb.compress, timescaledb.compress_segmentby ='');

INSERT INTO explain_test
SELECT '2024-01-01'::timestamptz + (i || ' minutes')::interval, i % 10, 'temp', i
FROM generate_series(1, 5000) i;

SELECT count(compress_chunk(c)) FROM show_chunks('explain_test') c;

BEGIN;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
INSERT INTO explain_test VALUES ('2024-01-01 00:05:30', 5, 'temp', 100)
ON CONFLICT (device_id, metric, ts) DO NOTHING;
ROLLBACK;

EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
INSERT INTO explain_test VALUES ('2024-01-01 00:05:30', 5, 'temp', 100)
ON CONFLICT (device_id, metric, ts)
DO UPDATE SET value = EXCLUDED.value;

DROP TABLE IF EXISTS explain_test CASCADE;

-------------------------------------------------------------------
CREATE TABLE upsert_test(ts timestamptz, uid int, event text, seg int);
SELECT create_hypertable('upsert_test', by_range('ts'));

CREATE UNIQUE INDEX idx_uid_event ON upsert_test (uid, event, ts);

ALTER TABLE upsert_test SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'seg'
);

INSERT INTO upsert_test
SELECT '2024-01-01'::timestamptz + (i || ' minutes')::interval,
       (i % 100) + 1,
       CASE WHEN i % 3 = 0 THEN 'login' WHEN i % 3 = 1 THEN 'logout' ELSE 'click' END,
       i % 5
FROM generate_series(1, 5000) i;

SELECT compress_chunk(c) FROM show_chunks('upsert_test') c;

EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
INSERT INTO upsert_test VALUES ('2024-01-01 00:03:00', 1, 'login', 1)
ON CONFLICT (uid, event, ts) DO NOTHING;

EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
INSERT INTO upsert_test VALUES ('2024-01-01 00:03:30', 50, 'login', 1)
ON CONFLICT (uid, event, ts) DO NOTHING;

DROP TABLE upsert_test CASCADE;

-------------------------------------------------------------------
CREATE TABLE upsert_temporal(
    time TIMESTAMPTZ NOT NULL,
    sensor_id INT,
    metric TEXT,
    value FLOAT
);

SELECT create_hypertable('upsert_temporal', 'time');
CREATE UNIQUE INDEX idx_unique ON upsert_temporal(sensor_id, metric, time);

ALTER TABLE upsert_temporal SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = ''
);

INSERT INTO upsert_temporal
SELECT t, (EXTRACT(HOUR FROM t)::int % 10) + 1, 'temp', 25.5
FROM generate_series('2024-01-01'::timestamptz, '2024-01-31'::timestamptz, '1 hour') t;
-- sensor_id: 1-10, time: whole hours

SELECT compress_chunk(c) FROM show_chunks('upsert_temporal') c;

EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
INSERT INTO upsert_temporal VALUES ('2024-01-15 10:00:00', 1, 'temp', 26.0)
ON CONFLICT (sensor_id, metric, time) DO NOTHING;

EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
INSERT INTO upsert_temporal VALUES ('2024-01-15 10:30:00', 5, 'temp', 26.0)
ON CONFLICT (sensor_id, metric, time) DO NOTHING;

DROP TABLE upsert_temporal CASCADE;

-------------------------------------------------------------------
CREATE TABLE upsert_demographics(
    time TIMESTAMPTZ NOT NULL,
    user_id INT,
    age SMALLINT,
    gender SMALLINT,
    value INT
);

SELECT create_hypertable('upsert_demographics', 'time');
CREATE UNIQUE INDEX idx_demo ON upsert_demographics(user_id, age, gender, time);

ALTER TABLE upsert_demographics SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = ''
);

INSERT INTO upsert_demographics
SELECT '2024-01-01'::timestamptz + (i || ' minutes')::interval,
       (i % 100) + 1,      -- user_id: 1-100
       (18 + (i % 50)),    -- age: 18-67
       (i % 2) + 1,        -- gender: 1-2
       i
FROM generate_series(1, 2000) i;

SELECT compress_chunk(c) FROM show_chunks('upsert_demographics') c;

EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
INSERT INTO upsert_demographics VALUES ('2024-01-01 00:01:00', 1, 18, 1, 200)
ON CONFLICT (user_id, age, gender, time) DO NOTHING;

EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
INSERT INTO upsert_demographics VALUES ('2024-01-01 00:01:30', 50, 30, 1, 200)
ON CONFLICT (user_id, age, gender, time) DO NOTHING;

DROP TABLE upsert_demographics CASCADE;

-------------------------------------------------------------------
CREATE TABLE explain_partial(ts timestamptz, device_id int, metric text, value float);
SELECT create_hypertable('explain_partial', by_range('ts'));
CREATE UNIQUE INDEX idx_partial ON explain_partial(device_id, metric, ts);

-- Manually configure compression with only partial bloom (device_id, metric)
-- This is a SUBSET of the conflict columns (device_id, metric, ts)
ALTER TABLE explain_partial SET (
    timescaledb.compress,
    timescaledb.order_by = 'ts',
    timescaledb.compress_segmentby = '',
    timescaledb.compress_index = 'bloom(device_id, metric)'
);

INSERT INTO explain_partial
SELECT '2024-01-01'::timestamptz + (i || ' minutes')::interval,
       i % 10, 'temp', i
FROM generate_series(1, 1000) i;
SELECT compress_chunk(c) FROM show_chunks('explain_partial') c;

BEGIN;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
INSERT INTO explain_partial VALUES ('2024-01-01 00:05:30', 5, 'temp', 100)
ON CONFLICT (device_id, metric, ts) DO NOTHING;
ROLLBACK;

EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
INSERT INTO explain_partial VALUES ('2024-01-01 00:05:00', 5, 'humidity', 100)
ON CONFLICT (device_id, metric, ts) DO NOTHING;

DROP TABLE explain_partial CASCADE;

-------------------------------------------------------------------
-- Hashed bloom filters over various column types
-------------------------------------------------------------------

create table hashes(ts int,
    x text,
    u uuid,
    d date,
    i4 int4,
    i8 int8)
with (tsdb.hypertable, tsdb.partition_column = 'ts',
    tsdb.compress, tsdb.orderby = 'ts',
    tsdb.sparse_index = 'bloom(x), bloom(u), bloom(d), bloom(i4), bloom(i8)')
;

insert into hashes
select 1,
    'test text',
    '90ec9e8e-4501-4232-9d03-6d7cf6132815',
    '2021-01-01',
    '1435',
    '30064771065'
;

select count(compress_chunk(x)) from show_chunks('hashes') x;
vacuum full analyze hashes;

select cs.compress_relid::regclass chunk from _timescaledb_catalog.chunk ch
    join _timescaledb_catalog.compression_settings cs
        on cs.relid = ch.relid
    where ch.hypertable_id = (select id from _timescaledb_catalog.hypertable
        where table_name = 'hashes') limit 1
\gset

\pset format unaligned
\pset expanded on
select * from :chunk;
\pset format aligned
\pset expanded off

DROP TABLE hashes CASCADE;

-------------------------------------------------------------------
-- Cross-type comparisons against an int2 bloom filter
-------------------------------------------------------------------

CREATE TABLE int2_crosstype(ts int NOT NULL, i2 smallint, i4 int)
WITH (tsdb.hypertable, tsdb.partition_column = 'ts',
    tsdb.compress, tsdb.compress_orderby = 'ts', tsdb.compress_index = 'bloom(i2), bloom(i2, i4)')
;

INSERT INTO int2_crosstype SELECT i, (i % 100)::smallint, i % 100 FROM generate_series(1, 5000) i;
INSERT INTO int2_crosstype SELECT i, (-1 * (i % 100))::smallint, i % 100 FROM generate_series(5001, 10000) i;
SELECT compress_chunk(c) FROM show_chunks('int2_crosstype') c;

SELECT count(*) FROM int2_crosstype WHERE i2 = 1::smallint;
SELECT count(*) FROM int2_crosstype WHERE i2 = 1::bigint;

SELECT count(*) FROM int2_crosstype WHERE i2 = -1::smallint;
SELECT count(*) FROM int2_crosstype WHERE i2 = -1::bigint;
SELECT count(*) FROM int2_crosstype WHERE i2 = 1::smallint AND i4 = 1;
SELECT count(*) FROM int2_crosstype WHERE i2 = 1::bigint AND i4 = 1;

DROP TABLE int2_crosstype;
