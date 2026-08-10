-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set PREFIX 'EXPLAIN (buffers off, costs off, timing off, summary off)'

-- Fix for issue #10421: do not drop MergeAppend subpaths with pathkeys not matching distinct pathkeys
-- when the compressed SkipScan plan is chosen.

-- This table will have uncompressed index on (sale_day) and compressed index on(style, sale_day)
CREATE TABLE t_10421 (
    style    text,
    sale_day date NOT NULL
) WITH (tsdb.hypertable, tsdb.partition_column = 'sale_day',
        tsdb.chunk_interval = '360 days', tsdb.segmentby = 'style');

INSERT INTO t_10421
SELECT 'S' || s, d
FROM generate_series(1, 20) s,
     generate_series('2026-01-02'::date, '2026-01-31'::date, '1 day') d;

SELECT count(compress_chunk(c)) FROM show_chunks('t_10421') c;

-- newer rows land in the same chunk, which is now partially compressed
INSERT INTO t_10421
SELECT 'S' || s, d
FROM generate_series(1, 20) s,
     generate_series('2026-02-01'::date, '2026-02-02'::date, '1 day') d;

-- steer the planner to the index-based plan shape
SET enable_seqscan = off;
SET enable_bitmapscan = off;

-- Make sure SkipScan is used on compressed part
-- and IndexScan with sort keys different from distinct keys but matching the predicate
-- is used for uncompressed part
:PREFIX
SELECT count(DISTINCT style) FROM t_10421
WHERE sale_day > '2026-01-31';

-- all 20 styles have sales after Jan 31, in the uncompressed part only
SET timescaledb.enable_compressed_skipscan = off;
SELECT count(DISTINCT style) FROM t_10421
WHERE sale_day > '2026-01-31';  -- returns 20, correct

SET timescaledb.enable_compressed_skipscan = on;  -- default setting
SELECT count(DISTINCT style) FROM t_10421
WHERE sale_day > '2026-01-31';  -- returns 20, correct

drop table t_10421 cascade;
RESET enable_seqscan;
RESET enable_bitmapscan;
