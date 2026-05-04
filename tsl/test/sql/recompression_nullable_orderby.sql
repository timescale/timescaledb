-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- #9444: do not recompress when order by columns are nullable, do decompress/compress instead.
-- It is due to compression min/max metadata not handling NULLs.
-- When we implement chunks with min/max NULL-handling metadata, this restriction can be lifted.

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

-- If only some of order by columns are nullable we still bail out on recompress
CREATE TABLE t2(time int NOT NULL, v1 int NOT NULL, v2 int);
SELECT create_hypertable('t2', 'time', chunk_time_interval => 10000);
ALTER TABLE t2 SET (timescaledb.compress, timescaledb.compress_orderby='v1,v2');

INSERT INTO t2 SELECT 1, 1, g FROM generate_series(1,800) g;
INSERT INTO t2 SELECT 1, 1, NULL FROM generate_series(1,200);
SELECT compress_chunk(show_chunks('t2'));

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

RESET timescaledb.enable_direct_compress_insert;
RESET timescaledb.batch_sorted_merge;
