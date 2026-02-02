-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
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

---------------------------------------------------------------------
-- Some chunks don't have a composite bloom index after index change
---------------------------------------------------------------------

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

---------------------------------------------------------------------
-- Index added after partial compression
---------------------------------------------------------------------

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

---------------------------------------------------------------------
-- Manual config change between chunk compressions
---------------------------------------------------------------------
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
-- Upsert tests
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

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
INSERT INTO explain_test VALUES ('2024-01-01 00:05:30', 5, 'temp', 100)
ON CONFLICT (device_id, metric, ts) DO NOTHING;

DROP TABLE IF EXISTS explain_test CASCADE;
