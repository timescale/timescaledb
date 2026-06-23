-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

-- #9444: do not recompress when order by columns are nullable, do decompress/compress instead.
-- It is due to compression min/max metadata not handling NULLs.
-- For chunks with min/max NULL-handling metadata (i.e. with first/last metadata), this restriction can be lifted.

SET timescaledb.enable_direct_compress_insert TO OFF;
SET timescaledb.batch_sorted_merge = 'off';

CREATE TABLE t1(time int, dev int, v1 int);
-- t1 is declared NOT NULL on a hypertable as partitioning column
SELECT create_hypertable('t1', 'time', chunk_time_interval => 10000);
ALTER TABLE t1 SET (timescaledb.compress, timescaledb.compress_segmentby='dev', timescaledb.compress_orderby='v1');

-- Insert data for one segment
INSERT INTO t1 SELECT 1, 1, g FROM generate_series(1,800) g;
INSERT INTO t1 SELECT 1, 1, NULL FROM generate_series(1,200);
SELECT compress_chunk(show_chunks('t1'));

-- Remove firstlast index from order by columns
update _timescaledb_catalog.compression_settings
set index = '[{"type": "minmax", "column": "v1", "source": "orderby"}, {"type": "minmax", "column": "time", "source": "orderby"}]'
where relid = 't1'::regclass;

update _timescaledb_catalog.compression_settings
set index = '[{"type": "minmax", "column": "v1", "source": "orderby"}, {"type": "minmax", "column": "time", "source": "orderby"}]'
where compress_relid = (select compress_relid from _timescaledb_catalog.compression_settings
    where relid = (select format('%I.%I', schema_name, table_name)::regclass from _timescaledb_catalog.chunk
        where hypertable_id = (select id from _timescaledb_catalog.hypertable
            where table_name = 't1') limit 1));

-- Use minmax index on (v1, time DESC) instead
select compress_relid::text comp_chunk from _timescaledb_catalog.compression_settings
    where relid = (select format('%I.%I', schema_name, table_name)::regclass from _timescaledb_catalog.chunk
        where hypertable_id = (select id from _timescaledb_catalog.hypertable
            where table_name = 't1') limit 1)
\gset
create index t1_compressed_index_minmax on :comp_chunk (dev, _ts_meta_min_1, _ts_meta_max_1, _ts_meta_min_2 DESC, _ts_meta_max_2 DESC);

-- Now this chunk is partial
INSERT INTO t1 SELECT 1, 1, g FROM generate_series(901,1400) g;

-- Should see an info about not recompressing because of nullable order by
SET timescaledb.debug_compression_path_info TO ON;
SELECT compress_chunk(show_chunks('t1'), if_not_compressed => true);
RESET timescaledb.debug_compression_path_info;

-- This query orders NULLS LAST, so we can't have a non-NULL value after a NULL value.
-- Should return 0.
SELECT count(*) AS wrong_rows FROM (
  SELECT v1, lead(v1) OVER (ORDER BY v1) AS next FROM t1 WHERE dev=1 ORDER BY v1
) t WHERE v1 IS NULL AND next IS NOT NULL;

-- Backfill data for a new segment.
INSERT INTO t1 SELECT 1, 2, g FROM generate_series(901,1400) g;

-- We should see only new segment data (500 rows) decompressed/compressed.
-- Should also see an info about not recompressing because of nullable order by
SET client_min_messages = 'DEBUG1';
SELECT compress_chunk(show_chunks('t1'), if_not_compressed => true);
RESET client_min_messages;

-- Add data to one existing segment.
INSERT INTO t1 SELECT 1, 2, g FROM generate_series(0, 100) g;

-- We should see only participating segment data (500+101 rows) decompressed/compressed.
-- Should also see an info about not recompressing because of nullable order by
SET client_min_messages = 'DEBUG1';
-- Use a function to recompress-segmentwise
SELECT show_chunks as chunk_to_recompress FROM show_chunks('t1') LIMIT 1 \gset
SELECT _timescaledb_functions.recompress_chunk_segmentwise(:'chunk_to_recompress');
RESET client_min_messages;

-- If only some of order by columns are nullable without firstlast, we still bail out on recompress
CREATE TABLE t2(time int NOT NULL, v1 int NOT NULL, v2 int);
SELECT create_hypertable('t2', 'time', chunk_time_interval => 10000);
ALTER TABLE t2 SET (timescaledb.compress, timescaledb.compress_orderby='v1,v2');

INSERT INTO t2 SELECT 1, 1, g FROM generate_series(1,800) g;
INSERT INTO t2 SELECT 1, 1, NULL FROM generate_series(1,200);
SELECT compress_chunk(show_chunks('t2'));

-- Remove firstlast index from nullable order by column "v2"
update _timescaledb_catalog.compression_settings
set index = '[{"type": "minmax", "column": "v1", "source": "orderby"}, {"type": "firstlast", "column": "v1", "source": "orderby"}, {"type": "minmax", "column": "v2", "source": "orderby"}, {"type": "minmax", "column": "time", "source": "orderby"}, {"type": "firstlast", "column": "time", "source": "orderby"}]'
where relid = 't2'::regclass;

update _timescaledb_catalog.compression_settings
set index = '[{"type": "minmax", "column": "v1", "source": "orderby"}, {"type": "firstlast", "column": "v1", "source": "orderby"}, {"type": "minmax", "column": "v2", "source": "orderby"}, {"type": "minmax", "column": "time", "source": "orderby"}, {"type": "firstlast", "column": "time", "source": "orderby"}]'
where compress_relid = (select compress_relid from _timescaledb_catalog.compression_settings
    where relid = (select format('%I.%I', schema_name, table_name)::regclass from _timescaledb_catalog.chunk
        where hypertable_id = (select id from _timescaledb_catalog.hypertable
            where table_name = 't2') limit 1));

-- Use minmax index on (v1, v2, time DESC) instead
select compress_relid::text comp_chunk from _timescaledb_catalog.compression_settings
    where relid = (select format('%I.%I', schema_name, table_name)::regclass from _timescaledb_catalog.chunk
        where hypertable_id = (select id from _timescaledb_catalog.hypertable
            where table_name = 't2') limit 1)
\gset
create index t2_compressed_index_minmax on :comp_chunk (_ts_meta_min_1, _ts_meta_max_1, _ts_meta_min_2, _ts_meta_max_2, _ts_meta_min_3 DESC, _ts_meta_max_3 DESC);

-- Now this chunk is partial
INSERT INTO t2 SELECT 1, 1, g FROM generate_series(901,1400) g;

-- Use a function to recompress-segmentwise
-- Should see an info about not recompressing because of nullable order by
SELECT show_chunks as chunk_to_recompress FROM show_chunks('t2') LIMIT 1 \gset
SET timescaledb.debug_compression_path_info TO ON;
SELECT _timescaledb_functions.recompress_chunk_segmentwise(:'chunk_to_recompress');
RESET timescaledb.debug_compression_path_info;

-- This query orders NULLS LAST, so we can't have a non-NULL value after a NULL value.
-- Should return 0.
SELECT count(*) AS wrong_rows FROM (
  SELECT v2, lead(v2) OVER (ORDER BY v1, v2) AS next FROM t2 ORDER BY v1, v2
) t WHERE v2 IS NULL AND next IS NOT NULL;

drop table t1 cascade;
drop table t2 cascade;

-- Tests for nullable order by columns with firstlast index
------------------------------------------------------------

--- NULLS LAST: batch (1, NULL)
CREATE TABLE test1(time int, dev int, v1 int);
-- t1 is declared NOT NULL on a hypertable as partitioning column
SELECT create_hypertable('test1', 'time', chunk_time_interval => 10000);
ALTER TABLE test1 SET (timescaledb.compress, timescaledb.compress_segmentby='dev', timescaledb.compress_orderby='v1');

-- Insert data for one segment
INSERT INTO test1 SELECT 1, 1, g FROM generate_series(1,800) g;
INSERT INTO test1 SELECT 1, 1, NULL FROM generate_series(1,200);
SELECT compress_chunk(show_chunks('test1'));

-- Now this chunk is partial, this batch should match [1, NULL] batch instead of sorting after it
INSERT INTO test1 SELECT 1, 1, g FROM generate_series(901,1400) g;

-- Should not see info about nullable order by
SET timescaledb.debug_compression_path_info TO ON;
SELECT compress_chunk(show_chunks('test1'), if_not_compressed => true);
RESET timescaledb.debug_compression_path_info;

-- This query orders NULLS LAST, so we can't have a non-NULL value after a NULL value.
-- Should return 0.
SELECT count(*) AS wrong_rows FROM (
  SELECT v1, lead(v1) OVER (ORDER BY v1) AS next FROM test1 WHERE dev=1 ORDER BY v1
) t WHERE v1 IS NULL AND next IS NOT NULL;

-- All NULLs, NULLS LAST, batch (NULL, NULL)
CREATE TABLE test2(time int NOT NULL, val int);
SELECT create_hypertable('test2', 'time', chunk_time_interval => 10000);
ALTER TABLE test2 SET (timescaledb.compress, timescaledb.compress_orderby='val');

INSERT INTO test2 SELECT 1, g FROM generate_series(501,1500) g;
INSERT INTO test2 SELECT 1, NULL FROM generate_series(1,1000);
SELECT compress_chunk(show_chunks('test2'));

-- Now this chunk is partial, recompress will be applied
INSERT INTO test2 SELECT 1, g FROM generate_series(1300,1600) g;
SELECT compress_chunk(show_chunks('test2'), if_not_compressed => true);

-- Should return 0
SELECT count(*) AS wrong_rows FROM (
  SELECT val, lead(val) OVER (ORDER BY val) AS next FROM test2 ORDER BY val
) t WHERE val IS NOT NULL AND next IS NOT NULL AND val > next;

-- All NULLs, NULLS FIRST, batch (NULL, NULL)
CREATE TABLE test3(time int NOT NULL, val int);
SELECT create_hypertable('test3', 'time', chunk_time_interval => 10000);
ALTER TABLE test3 SET (timescaledb.compress, timescaledb.compress_orderby='val NULLS FIRST');

INSERT INTO test3 SELECT 1, NULL FROM generate_series(1,1000);
INSERT INTO test3 SELECT 1, g FROM generate_series(501,1500) g;
SELECT compress_chunk(show_chunks('test3'));

-- Now this chunk is partial, recompress will be applied
INSERT INTO test3 SELECT 1, g FROM generate_series(400,600) g;
SELECT compress_chunk(show_chunks('test3'), if_not_compressed => true);

-- Should return 0
SELECT count(*) AS wrong_rows FROM (
  SELECT lag(val) OVER (ORDER BY val NULLS FIRST) AS prev, val FROM test3 ORDER BY val NULLS FIRST
) t WHERE val IS NOT NULL AND prev IS NOT NULL AND val < prev;

--- NULLS FIRST: batch (NULL, 1)
CREATE TABLE test4(time int, dev int, v1 int);
-- t1 is declared NOT NULL on a hypertable as partitioning column
SELECT create_hypertable('test4', 'time', chunk_time_interval => 10000);
ALTER TABLE test4 SET (timescaledb.compress, timescaledb.compress_segmentby='dev', timescaledb.compress_orderby='v1 NULLS FIRST');

-- Insert data for one segment
INSERT INTO test4 SELECT 1, 1, NULL FROM generate_series(1,200);
INSERT INTO test4 SELECT 1, 1, g FROM generate_series(1001,1800) g;
SELECT compress_chunk(show_chunks('test4'));

-- Now this chunk is partial, recompress will be applied
-- This batch should match [NULL... 1001...1800] batch instead of going before it
INSERT INTO test4 SELECT 1, 1, g FROM generate_series(500,900) g;
SELECT compress_chunk(show_chunks('test4'), if_not_compressed => true);

-- This query orders NULLS FIRST, so we can't have a non-NULL value before a NULL value.
-- Should return 0.
SELECT count(*) AS wrong_rows FROM (
  SELECT lag(v1) OVER (ORDER BY v1 NULLS FIRST) AS prev, v1 FROM test4 WHERE dev=1 ORDER BY v1 NULLS FIRST
) t WHERE v1 IS NULL AND prev IS NOT NULL;

drop table test1 cascade;
drop table test2 cascade;
drop table test3 cascade;
drop table test4 cascade;

RESET timescaledb.enable_direct_compress_insert;
RESET timescaledb.batch_sorted_merge;
