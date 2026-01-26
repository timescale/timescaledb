-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE VIEW settings AS SELECT * FROM _timescaledb_catalog.compression_settings ORDER BY upper(relid::text) COLLATE "C";
CREATE VIEW metacols AS select attname,count(*) from pg_attribute where attname like '%_ts_meta%' GROUP BY 1 ORDER BY attname::text COLLATE "C";
CREATE VIEW compressedcols AS select relname,attname,c.oid as reloid,attnum from pg_attribute a, pg_class c where c.oid=a.attrelid and relname like '%compress_hyper_%' order by c.oid asc, a.attnum asc;

create table sparse(ts int, o bigint, value float, boo bigint, big1 bigint, big2 bigint, sby bigint);
select create_hypertable('sparse', 'ts');
insert into sparse select x, x, x, x, x, x, x from generate_series(1, 10000) x;
alter table sparse set (
    timescaledb.compress,
    timescaledb.order_by='o',
    timescaledb.segment_by='sby',
    timescaledb.compress_index = 'bloom(big1),bloom(big2),bloom(value),bloom(value,big1,big2),bloom(o,big2),bloom(big1,big2),bloom(boo,big1)');

select count(compress_chunk(x)) from show_chunks('sparse') x;
vacuum analyze sparse;

-- smoke tests
select min(big1), max(big2) from sparse where value < 100 and value > 10;
select index from settings group by 1 order by 1;
select attname from metacols;

-- show plan
explain (buffers off, costs off) select * from sparse where value = 1;
explain (buffers off, costs off) select * from sparse where o = 1 and big2 = 1;

-- segmentby = bloom
explain (buffers off, costs off) select * from sparse where big1 = sby and value = 1;
explain (buffers off, costs off) select * from sparse where o = sby and big2 = sby;
explain (buffers off, costs off) select * from sparse where o = 1 and big2 = sby;
explain (buffers off, costs off) select * from sparse where o = sby and big2 = 1;

-- renaming a column that participates in a composite bloom filter
alter table sparse rename big2 to xxl;
select attname from metacols;
select index from settings group by 1 order by 1;
select distinct(attname) from compressedcols order by 1 asc;

-- dropping a column that participates in a composite bloom filter
alter table sparse drop column xxl;
select attname from metacols;
select index from settings group by 1 order by 1;
select distinct(attname) from compressedcols order by 1 asc;

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
