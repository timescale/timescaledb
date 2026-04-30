-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Tests for timescaledb.compression_flush_batch_on_first_orderby_change.
-- When the GUC is on, the row compressor must finish the current batch as
-- soon as the value of the first orderby column changes.

CREATE TABLE flush_orderby(seg int, ts int, val int)
    WITH (tsdb.hypertable, tsdb.partition_column = 'ts',
          tsdb.chunk_interval = 1000000);

-- 100 distinct ts values, 10 rows each. With the default batch target each
-- compressed batch covers many distinct ts values.
INSERT INTO flush_orderby
SELECT 1, t, r FROM generate_series(0, 99) t, generate_series(1, 10) r;

ALTER TABLE flush_orderby SET (tsdb.compress, tsdb.compress_segmentby = 'seg',
    tsdb.compress_orderby = 'ts');

-- Default behavior: GUC off, batches span multiple ts values.
SHOW timescaledb.compression_flush_batch_on_first_orderby_change;
SELECT count(compress_chunk(c)) FROM show_chunks('flush_orderby') c;

SELECT format('%I.%I', schema_name, table_name) AS "CCHUNK"
FROM _timescaledb_catalog.chunk WHERE compressed_chunk_id IS NULL
ORDER BY id DESC LIMIT 1 \gset

-- Without flushing on orderby change there should be at least one batch
-- covering more than one ts value.
SELECT bool_or(_ts_meta_min_1 <> _ts_meta_max_1) AS has_multi_value_batch
FROM :CCHUNK;

-- Recompress with the GUC enabled.
SELECT count(decompress_chunk(c)) FROM show_chunks('flush_orderby') c;

SET timescaledb.compression_flush_batch_on_first_orderby_change = on;
SELECT count(compress_chunk(c)) FROM show_chunks('flush_orderby') c;

SELECT format('%I.%I', schema_name, table_name) AS "CCHUNK"
FROM _timescaledb_catalog.chunk WHERE compressed_chunk_id IS NULL
ORDER BY id DESC LIMIT 1 \gset

-- Every batch must now cover exactly one ts value (min == max) and
-- the number of batches must equal the number of distinct ts values.
SELECT bool_and(_ts_meta_min_1 = _ts_meta_max_1) AS every_batch_single_value,
       count(*) AS num_batches
FROM :CCHUNK;

SELECT count(*) FROM (SELECT DISTINCT ts FROM flush_orderby) s;

-- Data round-trips cleanly.
SELECT count(*), sum(val) FROM flush_orderby;

DROP TABLE flush_orderby;
RESET timescaledb.compression_flush_batch_on_first_orderby_change;


-- Multiple segmentby groups: the per-orderby flush still applies within
-- every segment.
CREATE TABLE flush_orderby_multi(seg int, ts int, val int)
    WITH (tsdb.hypertable, tsdb.partition_column = 'ts',
          tsdb.chunk_interval = 1000000);

INSERT INTO flush_orderby_multi
SELECT s, t, r
FROM generate_series(1, 3) s, generate_series(0, 19) t, generate_series(1, 5) r;

ALTER TABLE flush_orderby_multi SET (tsdb.compress, tsdb.compress_segmentby = 'seg',
    tsdb.compress_orderby = 'ts');

SET timescaledb.compression_flush_batch_on_first_orderby_change = on;
SELECT count(compress_chunk(c)) FROM show_chunks('flush_orderby_multi') c;

SELECT format('%I.%I', schema_name, table_name) AS "CCHUNK"
FROM _timescaledb_catalog.chunk WHERE compressed_chunk_id IS NULL
ORDER BY id DESC LIMIT 1 \gset

SELECT seg, count(*) AS num_batches,
       bool_and(_ts_meta_min_1 = _ts_meta_max_1) AS every_batch_single_value
FROM :CCHUNK GROUP BY seg ORDER BY seg;

SELECT count(*), sum(val) FROM flush_orderby_multi;

DROP TABLE flush_orderby_multi;
RESET timescaledb.compression_flush_batch_on_first_orderby_change;


-- NULL values in the first orderby column. With NULLS LAST (the default)
-- the trailing run of NULLs ends up in its own batch.
CREATE TABLE flush_orderby_null(seg int, ts int, val int)
    WITH (tsdb.hypertable, tsdb.partition_column = 'val', tsdb.chunk_interval = 1000000);

INSERT INTO flush_orderby_null
SELECT 1, CASE WHEN x % 4 = 0 THEN NULL ELSE x % 5 END, x
FROM generate_series(1, 40) x;

ALTER TABLE flush_orderby_null SET (tsdb.compress, tsdb.compress_segmentby = 'seg',
    tsdb.compress_orderby = 'ts');

SET timescaledb.compression_flush_batch_on_first_orderby_change = on;
SELECT count(compress_chunk(c)) FROM show_chunks('flush_orderby_null') c;

SELECT format('%I.%I', schema_name, table_name) AS "CCHUNK"
FROM _timescaledb_catalog.chunk WHERE compressed_chunk_id IS NULL
ORDER BY id DESC LIMIT 1 \gset

SELECT _ts_meta_min_1, _ts_meta_max_1, _ts_meta_count
FROM :CCHUNK ORDER BY _ts_meta_min_1 NULLS LAST, _ts_meta_max_1;

SELECT count(*), sum(val), count(ts) FROM flush_orderby_null;

DROP TABLE flush_orderby_null;
RESET timescaledb.compression_flush_batch_on_first_orderby_change;


-- A single first-orderby value with more rows than the target batch size:
-- the batch fills up and gets flushed even though the orderby value has
-- not changed, so we get multiple batches that all share the same min/max.
CREATE TABLE flush_orderby_big(seg int, ts int, val int)
    WITH (tsdb.hypertable, tsdb.partition_column = 'val',
          tsdb.chunk_interval = 100000);

-- 2500 rows with ts = 1, then 500 rows with ts = 2.
INSERT INTO flush_orderby_big
SELECT 1, 1, x FROM generate_series(1, 2500) x;
INSERT INTO flush_orderby_big
SELECT 1, 2, x FROM generate_series(2501, 3000) x;

ALTER TABLE flush_orderby_big SET (tsdb.compress, tsdb.compress_segmentby = 'seg',
    tsdb.compress_orderby = 'ts');

SET timescaledb.compression_flush_batch_on_first_orderby_change = on;
SELECT count(compress_chunk(c)) FROM show_chunks('flush_orderby_big') c;

SELECT format('%I.%I', schema_name, table_name) AS "CCHUNK"
FROM _timescaledb_catalog.chunk WHERE compressed_chunk_id IS NULL
ORDER BY id DESC LIMIT 1 \gset

-- Every batch still covers a single ts value (min == max), but we need
-- more than one batch for ts = 1 because it exceeds the batch size limit.
SELECT _ts_meta_min_1, _ts_meta_max_1, count(*) AS num_batches,
       sum(_ts_meta_count) AS total_rows
FROM :CCHUNK GROUP BY _ts_meta_min_1, _ts_meta_max_1
ORDER BY _ts_meta_min_1;

SELECT bool_and(_ts_meta_min_1 = _ts_meta_max_1) AS every_batch_single_value
FROM :CCHUNK;

SELECT count(*), sum(val) FROM flush_orderby_big;

DROP TABLE flush_orderby_big;
RESET timescaledb.compression_flush_batch_on_first_orderby_change;


-- First orderby column is text (pass-by-reference) to exercise the
-- datumCopy path in segment_info_update.
CREATE TABLE flush_orderby_text(seg int, label text, ts int, val int)
    WITH (tsdb.hypertable, tsdb.partition_column = 'ts',
          tsdb.chunk_interval = 1000000);

INSERT INTO flush_orderby_text
SELECT 1,
       'label_' || lpad((x % 5)::text, 2, '0'),
       x,
       x
FROM generate_series(1, 50) x;

ALTER TABLE flush_orderby_text SET (tsdb.compress, tsdb.compress_segmentby = 'seg',
    tsdb.compress_orderby = 'label, ts');

SET timescaledb.compression_flush_batch_on_first_orderby_change = on;
SELECT count(compress_chunk(c)) FROM show_chunks('flush_orderby_text') c;

SELECT format('%I.%I', schema_name, table_name) AS "CCHUNK"
FROM _timescaledb_catalog.chunk WHERE compressed_chunk_id IS NULL
ORDER BY id DESC LIMIT 1 \gset

-- One batch per distinct label, with min and max equal.
SELECT _ts_meta_min_1, _ts_meta_max_1, _ts_meta_count
FROM :CCHUNK ORDER BY _ts_meta_min_1;

SELECT bool_and(_ts_meta_min_1 = _ts_meta_max_1) AS every_batch_single_value,
       count(*) AS num_batches
FROM :CCHUNK;

SELECT count(*), sum(val), count(DISTINCT label) FROM flush_orderby_text;

DROP TABLE flush_orderby_text;
RESET timescaledb.compression_flush_batch_on_first_orderby_change;
