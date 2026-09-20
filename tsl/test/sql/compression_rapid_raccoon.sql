-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- The purpose of this test is to exercise the RapidRaccoon compressor across
-- the supported datatypes and compression modalities. Ultimately this test
-- should provide a very good coverage of the RR code.
--
-- Each section fills one 1000-row chunk with data that steers the
-- compressor into one block type, first without and then with NULLs.
-- The rows are staged in the plain table `source` and copied into the
-- hypertable, so after compression we can compare the decompressed rows
-- against `source` to verify the roundtrip.

SET timescaledb.enable_rapid_raccoon_compression = ON;
CREATE TABLE t (tsx int, i2 INT2, i4 INT4, i8 INT8, d DATE, ts TIMESTAMP, tz TIMESTAMPTZ);
SELECT create_hypertable('t', 'tsx', chunk_time_interval => 1000);
ALTER TABLE t SET (timescaledb.compress, timescaledb.compress_orderby = 'tsx');

-- uncompressed reference copy of every generated row
CREATE TABLE source (LIKE t);

-- deterministic scrambler returning values in [0, r)
CREATE FUNCTION prand(i int, r int) RETURNS int
LANGUAGE SQL IMMUTABLE AS
$$ SELECT (hashint4(i) & 2147483647) % r $$;

-- RLE:
-- four constant runs of 250 values per column. a run has to reach
-- RR_RLE_CHECKPOINT (128) buffered values to switch into OP_MODE_RLE.
INSERT INTO source
SELECT i,
       (i / 250 + 1) * 100,
       (i / 250 + 1) * 100000,
       (i / 250 + 1) * 100000000000,
       '2000-01-01'::date + (i / 250) * 1000,
       '2000-01-01'::timestamp + (i / 250) * interval '1000 hours',
       '2000-01-01'::timestamptz + (i / 250) * interval '1000 hours'
FROM generate_series(0, 999) i;
INSERT INTO t SELECT * FROM source WHERE tsx BETWEEN 0 AND 999;
SELECT count(compress_chunk(ch)) FROM show_chunks('t') ch;

-- RLE with NULLs: every 10th row is NULL. the value stream drops the
-- NULLs, the runs stay constant, and the validity bitmap carries the NULLs.
INSERT INTO source
SELECT 1000 + i,
       (i / 250 + 1) * 100,
       (i / 250 + 1) * 100000,
       (i / 250 + 1) * 100000000000,
       '2000-01-01'::date + (i / 250) * 1000,
       '2000-01-01'::timestamp + (i / 250) * interval '1000 hours',
       '2000-01-01'::timestamptz + (i / 250) * interval '1000 hours'
FROM generate_series(0, 999) i;
UPDATE source SET i2 = NULL, i4 = NULL, i8 = NULL, d = NULL, ts = NULL, tz = NULL
WHERE tsx BETWEEN 1000 AND 1999 AND (tsx - 1000) % 10 = 7;
INSERT INTO t SELECT * FROM source WHERE tsx BETWEEN 1000 AND 1999;
SELECT count(compress_chunk(ch)) FROM show_chunks('t') ch;

-- DELTA RLE:
-- four arithmetic segments of 250 values per column with distinct bases
-- and steps. a segment has to hold a constant nonzero delta for
-- RR_DELTA_RLE_CHECKPOINT (32) buffered values to switch into
-- OP_MODE_DELTA_RLE.
INSERT INTO source
SELECT 2000 + i,
       (ARRAY[-15000, 15000, -10000, 10000])[seg + 1] + pos * (ARRAY[7, -7, 13, -13])[seg + 1],
       (ARRAY[-1500000, 1500000, -1000000, 1000000])[seg + 1] + pos * (ARRAY[97, -97, 131, -131])[seg + 1],
       (ARRAY[-4000000000, 4000000000, -2000000000, 2000000000])[seg + 1] + pos * (ARRAY[100003, -100003, 131071, -131071])[seg + 1],
       '2000-01-01'::date + (ARRAY[10000, 20000, 5000, 15000])[seg + 1] + pos * (ARRAY[2, -3, 5, -7])[seg + 1],
       '2000-01-01'::timestamp + (ARRAY[0, 500000, 100000, 600000])[seg + 1] * interval '1 minute'
                               + pos * (ARRAY[3, -5, 7, -11])[seg + 1] * interval '1 second',
       '2000-01-01'::timestamptz + (ARRAY[0, 700000, 200000, 800000])[seg + 1] * interval '1 minute'
                                 + pos * (ARRAY[2, -3, 5, -7])[seg + 1] * interval '1 second'
FROM (SELECT i, i / 250 AS seg, i % 250 AS pos FROM generate_series(0, 999) i) s;
INSERT INTO t SELECT * FROM source WHERE tsx BETWEEN 2000 AND 2999;
SELECT count(compress_chunk(ch)) FROM show_chunks('t') ch;

-- DELTA RLE with NULLs: the NULLs are clumped at each segment start, so
-- the value stream keeps its constant delta
INSERT INTO source
SELECT 3000 + i,
       (ARRAY[-15000, 15000, -10000, 10000])[seg + 1] + pos * (ARRAY[7, -7, 13, -13])[seg + 1],
       (ARRAY[-1500000, 1500000, -1000000, 1000000])[seg + 1] + pos * (ARRAY[97, -97, 131, -131])[seg + 1],
       (ARRAY[-4000000000, 4000000000, -2000000000, 2000000000])[seg + 1] + pos * (ARRAY[100003, -100003, 131071, -131071])[seg + 1],
       '2000-01-01'::date + (ARRAY[10000, 20000, 5000, 15000])[seg + 1] + pos * (ARRAY[2, -3, 5, -7])[seg + 1],
       '2000-01-01'::timestamp + (ARRAY[0, 500000, 100000, 600000])[seg + 1] * interval '1 minute'
                               + pos * (ARRAY[3, -5, 7, -11])[seg + 1] * interval '1 second',
       '2000-01-01'::timestamptz + (ARRAY[0, 700000, 200000, 800000])[seg + 1] * interval '1 minute'
                                 + pos * (ARRAY[2, -3, 5, -7])[seg + 1] * interval '1 second'
FROM (SELECT i, i / 250 AS seg, i % 250 AS pos FROM generate_series(0, 999) i) s;
UPDATE source SET i2 = NULL, i4 = NULL, i8 = NULL, d = NULL, ts = NULL, tz = NULL
WHERE tsx BETWEEN 3000 AND 3999 AND (tsx - 3000) % 250 < 25;
INSERT INTO t SELECT * FROM source WHERE tsx BETWEEN 3000 AND 3999;
SELECT count(compress_chunk(ch)) FROM show_chunks('t') ch;

-- FOR:
-- uniform noise over power-of-two ranges. ~900 distinct values per column
-- rule out DICT (RR_DICT_K_CEIL is 128), about half the residuals use the
-- top bit so PFOR would need more exceptions than RR_PFOR_MAX_EXCEPTIONS
-- (64), and deltas of noise are wider than the values so DFOR can't win.
INSERT INTO source
SELECT 4000 + i,
       5000 + prand(i, 8192),
       100000 + prand(i + 101, 1048576),
       10000000000 + prand(i + 211, 65536)::int8 * 65536 + prand(i + 223, 65536),
       '2000-01-01'::date + prand(i + 307, 4096),
       '2000-01-01'::timestamp + prand(i + 401, 32768) * interval '1 second',
       '2000-01-01'::timestamptz + prand(i + 503, 32768) * interval '1 second'
FROM generate_series(0, 999) i;
INSERT INTO t SELECT * FROM source WHERE tsx BETWEEN 4000 AND 4999;
SELECT count(compress_chunk(ch)) FROM show_chunks('t') ch;

-- FOR with NULLs: every 3rd row is NULL, so every 64-bit validity group
-- is mixed, the RAW validity format is kept, and the valid runs are short.
INSERT INTO source
SELECT 5000 + i,
       5000 + prand(i, 8192),
       100000 + prand(i + 101, 1048576),
       10000000000 + prand(i + 211, 65536)::int8 * 65536 + prand(i + 223, 65536),
       '2000-01-01'::date + prand(i + 307, 4096),
       '2000-01-01'::timestamp + prand(i + 401, 32768) * interval '1 second',
       '2000-01-01'::timestamptz + prand(i + 503, 32768) * interval '1 second'
FROM generate_series(0, 999) i;
UPDATE source SET i2 = NULL, i4 = NULL, i8 = NULL, d = NULL, ts = NULL, tz = NULL
WHERE tsx BETWEEN 5000 AND 5999 AND (tsx - 5000) % 3 = 0;
INSERT INTO t SELECT * FROM source WHERE tsx BETWEEN 5000 AND 5999;
SELECT count(compress_chunk(ch)) FROM show_chunks('t') ch;

-- DFOR:
-- monotone ramps with bounded jitter. the jitter breaks the constant-delta
-- check at RR_DELTA_RLE_CHECKPOINT (32), and the deltas are much narrower
-- than the values, so DFOR beats FOR. all values are distinct, so DICT
-- is out.
INSERT INTO source
SELECT 6000 + i,
       i * 30 + prand(i, 30),
       i * 65536 + prand(i + 101, 65536),
       i::int8 * 4294967296 + prand(i + 211, 65536)::int8 * 65536 + prand(i + 223, 65536),
       '2000-01-01'::date + i * 10 + prand(i + 307, 10),
       '2000-01-01'::timestamp + i * interval '1 hour' + prand(i + 401, 60) * interval '1 second',
       '2000-01-01'::timestamptz + i * interval '1 hour' + prand(i + 503, 60) * interval '1 second'
FROM generate_series(0, 999) i;
INSERT INTO t SELECT * FROM source WHERE tsx BETWEEN 6000 AND 6999;
SELECT count(compress_chunk(ch)) FROM show_chunks('t') ch;

-- DFOR with NULLs: every 10th row is NULL. a dropped value roughly doubles
-- one delta, which only costs one extra bit in the delta residual stream.
INSERT INTO source
SELECT 7000 + i,
       i * 30 + prand(i, 30),
       i * 65536 + prand(i + 101, 65536),
       i::int8 * 4294967296 + prand(i + 211, 65536)::int8 * 65536 + prand(i + 223, 65536),
       '2000-01-01'::date + i * 10 + prand(i + 307, 10),
       '2000-01-01'::timestamp + i * interval '1 hour' + prand(i + 401, 60) * interval '1 second',
       '2000-01-01'::timestamptz + i * interval '1 hour' + prand(i + 503, 60) * interval '1 second'
FROM generate_series(0, 999) i;
UPDATE source SET i2 = NULL, i4 = NULL, i8 = NULL, d = NULL, ts = NULL, tz = NULL
WHERE tsx BETWEEN 7000 AND 7999 AND (tsx - 7000) % 10 = 7;
INSERT INTO t SELECT * FROM source WHERE tsx BETWEEN 7000 AND 7999;
SELECT count(compress_chunk(ch)) FROM show_chunks('t') ch;

-- PFOR:
-- a narrow base range of 512 with large outliers on every 20th row: 12-13
-- exceptions per 256-value block, under RR_PFOR_MAX_EXCEPTIONS (64), and
-- the distinct count stays above RR_DICT_K_CEIL.
INSERT INTO source
SELECT 8000 + i,
       CASE WHEN i % 20 = 13 THEN 30000 ELSE 1000 END + prand(i, 512),
       CASE WHEN i % 20 = 13 THEN 100000000 ELSE 100000 END + prand(i + 101, 512),
       CASE WHEN i % 20 = 13 THEN 1000000000000000 ELSE 10000000000 END + prand(i + 211, 512),
       '2000-01-01'::date + CASE WHEN i % 20 = 13 THEN 25000 ELSE 0 END + prand(i + 307, 512),
       '2000-01-01'::timestamp + CASE WHEN i % 20 = 13 THEN interval '35184372089344 microseconds' ELSE interval '0' END
                               + prand(i + 401, 512) * interval '1 microsecond',
       '2000-01-01'::timestamptz + CASE WHEN i % 20 = 13 THEN interval '35184372089344 microseconds' ELSE interval '0' END
                                 + prand(i + 503, 512) * interval '1 microsecond'
FROM generate_series(0, 999) i;
INSERT INTO t SELECT * FROM source WHERE tsx BETWEEN 8000 AND 8999;
SELECT count(compress_chunk(ch)) FROM show_chunks('t') ch;

-- PFOR with NULLs: every 10th row is NULL. the NULLs never hit an outlier
-- (outliers are at 13 mod 20, NULLs at 7 mod 10), so all exceptions stay
-- in the value stream.
INSERT INTO source
SELECT 9000 + i,
       CASE WHEN i % 20 = 13 THEN 30000 ELSE 1000 END + prand(i, 512),
       CASE WHEN i % 20 = 13 THEN 100000000 ELSE 100000 END + prand(i + 101, 512),
       CASE WHEN i % 20 = 13 THEN 1000000000000000 ELSE 10000000000 END + prand(i + 211, 512),
       '2000-01-01'::date + CASE WHEN i % 20 = 13 THEN 25000 ELSE 0 END + prand(i + 307, 512),
       '2000-01-01'::timestamp + CASE WHEN i % 20 = 13 THEN interval '35184372089344 microseconds' ELSE interval '0' END
                               + prand(i + 401, 512) * interval '1 microsecond',
       '2000-01-01'::timestamptz + CASE WHEN i % 20 = 13 THEN interval '35184372089344 microseconds' ELSE interval '0' END
                                 + prand(i + 503, 512) * interval '1 microsecond'
FROM generate_series(0, 999) i;
UPDATE source SET i2 = NULL, i4 = NULL, i8 = NULL, d = NULL, ts = NULL, tz = NULL
WHERE tsx BETWEEN 9000 AND 9999 AND (tsx - 9000) % 10 = 7;
INSERT INTO t SELECT * FROM source WHERE tsx BETWEEN 9000 AND 9999;
SELECT count(compress_chunk(ch)) FROM show_chunks('t') ch;

-- DICT:
-- 60 distinct keys spread across the type range, picked in scrambled order.
-- K stays under RR_DICT_K_CEIL (128), the wide key spread makes FOR
-- expensive, and the scrambled order leaves no runs and no narrow PFOR
-- width.
INSERT INTO source
SELECT 10000 + i,
       -30000 + prand(i, 60) * 1000,
       -2000000000 + prand(i + 101, 60)::int8 * 60000000,
       -4000000000000000000 + prand(i + 211, 60)::int8 * 130000000000000000,
       '2000-01-01'::date + prand(i + 307, 60) * 400 - 12000,
       '2000-01-01'::timestamp + (prand(i + 401, 60) * 997 - 30000) * interval '1 day',
       '2000-01-01'::timestamptz + (prand(i + 503, 60) * 997 - 30000) * interval '1 day'
FROM generate_series(0, 999) i;
INSERT INTO t SELECT * FROM source WHERE tsx BETWEEN 10000 AND 10999;
SELECT count(compress_chunk(ch)) FROM show_chunks('t') ch;

-- DICT with NULLs: every 10th row is NULL. the key set and the scrambled
-- order are unchanged in the value stream.
INSERT INTO source
SELECT 11000 + i,
       -30000 + prand(i, 60) * 1000,
       -2000000000 + prand(i + 101, 60)::int8 * 60000000,
       -4000000000000000000 + prand(i + 211, 60)::int8 * 130000000000000000,
       '2000-01-01'::date + prand(i + 307, 60) * 400 - 12000,
       '2000-01-01'::timestamp + (prand(i + 401, 60) * 997 - 30000) * interval '1 day',
       '2000-01-01'::timestamptz + (prand(i + 503, 60) * 997 - 30000) * interval '1 day'
FROM generate_series(0, 999) i;
UPDATE source SET i2 = NULL, i4 = NULL, i8 = NULL, d = NULL, ts = NULL, tz = NULL
WHERE tsx BETWEEN 11000 AND 11999 AND (tsx - 11000) % 10 = 7;
INSERT INTO t SELECT * FROM source WHERE tsx BETWEEN 11000 AND 11999;
SELECT count(compress_chunk(ch)) FROM show_chunks('t') ch;

-- DFOR with zigzag deltas:
-- a triangle wave that turns mid-block: 128 rising then 128 falling
-- values per period, with jitter to break the constant-delta check at
-- RR_DELTA_RLE_CHECKPOINT (32). the strided deltas flip sign inside
-- every block, which selects zigzag encoding for the delta residuals,
-- and they stay much narrower than the values, so DFOR beats FOR.
INSERT INTO source
SELECT 12000 + i,
       1000 + tri * 200 + prand(i, 64),
       100000 + tri * 30000 + prand(i + 101, 4096),
       10000000000 + tri::int8 * 4294967296 + prand(i + 211, 65536),
       '2000-01-01'::date + tri * 40 + prand(i + 307, 8),
       '2000-01-01'::timestamp + tri * interval '1 hour' + prand(i + 401, 60) * interval '1 second',
       '2000-01-01'::timestamptz + tri * interval '1 hour' + prand(i + 503, 60) * interval '1 second'
FROM (SELECT i, LEAST(i % 256, 256 - i % 256) AS tri FROM generate_series(0, 999) i) s;
INSERT INTO t SELECT * FROM source WHERE tsx BETWEEN 12000 AND 12999;
SELECT count(compress_chunk(ch)) FROM show_chunks('t') ch;

-- DFOR with W_bases = 0:
-- every block starts with 8 equal values before a jittered ramp, so the
-- stride bases are all equal and the packed bases stream is empty. the
-- 16- and 32-bit types use stride 4, so their strided deltas inside the
-- constant head are 0 and the delta_residual_base scalar is omitted too.
INSERT INTO source
SELECT 13000 + i,
       1000 + CASE WHEN pos < 8 THEN 0 ELSE (pos - 7) * 100 + prand(i, 64) END,
       100000 + CASE WHEN pos < 8 THEN 0 ELSE (pos - 7) * 20000 + prand(i + 101, 4096) END,
       10000000000 + CASE WHEN pos < 8 THEN 0 ELSE (pos - 7)::int8 * 4294967296 + prand(i + 211, 65536) END,
       '2000-01-01'::date + CASE WHEN pos < 8 THEN 0 ELSE (pos - 7) * 30 + prand(i + 307, 8) END,
       '2000-01-01'::timestamp + CASE WHEN pos < 8 THEN interval '0' ELSE (pos - 7) * interval '1 hour' + prand(i + 401, 60) * interval '1 second' END,
       '2000-01-01'::timestamptz + CASE WHEN pos < 8 THEN interval '0' ELSE (pos - 7) * interval '1 hour' + prand(i + 503, 60) * interval '1 second' END
FROM (SELECT i, i % 256 AS pos FROM generate_series(0, 999) i) s;
INSERT INTO t SELECT * FROM source WHERE tsx BETWEEN 13000 AND 13999;
SELECT count(compress_chunk(ch)) FROM show_chunks('t') ch;

-- DFOR with W_delta_residuals = 0:
-- a strided staircase: a fixed pattern whose period matches the stride
-- (4 for the 16/32-bit types, 8 for the 64-bit types) plus a constant
-- step per period. every strided delta is exactly the step, so the
-- delta residual body is empty and the step is carried by the
-- delta_residual_base scalar. the pattern diffs vary, which breaks the
-- constant-delta check at RR_DELTA_RLE_CHECKPOINT (32).
INSERT INTO source
SELECT 14000 + i,
       1000 + (ARRAY[0, 210, 90, 350])[pos % 4 + 1] + (pos / 4) * 400,
       100000 + (ARRAY[0, 31000, 13000, 52000])[pos % 4 + 1] + (pos / 4) * 60000,
       10000000000 + (ARRAY[0, 5100000000, 1700000000, 7300000000, 2900000000, 8100000000, 900000000, 6200000000])[pos % 8 + 1] + (pos / 8)::int8 * 10000000000,
       '2000-01-01'::date + (ARRAY[0, 2600, 1100, 4300])[pos % 4 + 1] + (pos / 4) * 5000,
       '2000-01-01'::timestamp + (ARRAY[0, 13, 5, 21, 9, 2, 17, 11])[pos % 8 + 1] * interval '1 hour' + (pos / 8) * interval '1 day',
       '2000-01-01'::timestamptz + (ARRAY[0, 7, 19, 3, 15, 22, 10, 6])[pos % 8 + 1] * interval '1 hour' + (pos / 8) * interval '1 day'
FROM (SELECT i, i % 256 AS pos FROM generate_series(0, 999) i) s;
INSERT INTO t SELECT * FROM source WHERE tsx BETWEEN 14000 AND 14999;
SELECT count(compress_chunk(ch)) FROM show_chunks('t') ch;

-- PFOR with W = 0:
-- an exactly constant base with jittered outliers on every 20th row.
-- every residual is either zero or has its bits inside the 8-bit search
-- window, so rr_pfor_search narrows W to 0: the packed body is empty
-- and the outliers carry their full residuals in the exception store.
-- the outlier jitter keeps the per-block key count around 14, so the
-- DICT probe pays a full-length index stream and loses on cost.
INSERT INTO source
SELECT 15000 + i,
       CASE WHEN i % 20 = 13 THEN 30000 + prand(i, 512) ELSE 1000 END,
       CASE WHEN i % 20 = 13 THEN 100000000 + prand(i + 101, 512) ELSE 100000 END,
       10000000000 + CASE WHEN i % 20 = 13 THEN 35184372088832 + prand(i + 211, 512) ELSE 0 END,
       '2000-01-01'::date + CASE WHEN i % 20 = 13 THEN 25000 + prand(i + 307, 512) ELSE 0 END,
       '2000-01-01'::timestamp + CASE WHEN i % 20 = 13 THEN interval '35184372088832 microseconds' + prand(i + 401, 512) * interval '1 microsecond' ELSE interval '0' END,
       '2000-01-01'::timestamptz + CASE WHEN i % 20 = 13 THEN interval '35184372088832 microseconds' + prand(i + 503, 512) * interval '1 microsecond' ELSE interval '0' END
FROM generate_series(0, 999) i;
INSERT INTO t SELECT * FROM source WHERE tsx BETWEEN 15000 AND 15999;
SELECT count(compress_chunk(ch)) FROM show_chunks('t') ch;

-- decompress every column of every chunk. the per-column counts also
-- verify the NULL bookkeeping
SELECT count(*) AS total,
       count(i2) AS i2, count(i4) AS i4, count(i8) AS i8,
       count(d) AS d, count(ts) AS ts, count(tz) AS tz
FROM t;

-- compare the decompressed rows against the uncompressed reference copy
SELECT count(*) AS roundtrip_mismatches FROM (
  (SELECT * FROM t EXCEPT SELECT * FROM source)
  UNION ALL
  (SELECT * FROM source EXCEPT SELECT * FROM t)
) diff;

-- verify the existence of the compressed chunks using the RR compressor
CREATE TABLE compressed_chunks AS
SELECT
  cs.compress_relid as compressed_chunk,
  ccs.numrows_pre_compression,
  ccs.numrows_post_compression
FROM
  show_chunks('t') c
  INNER JOIN _timescaledb_catalog.chunk cat
    ON (c = cat.relid)
  INNER JOIN _timescaledb_catalog.compression_settings cs
    ON (cs.relid = cat.relid)
  INNER JOIN _timescaledb_catalog.compression_chunk_size ccs
    ON (ccs.chunk_id = cat.id);

CREATE TABLE compression_info (compressed_chunk regclass, col text, result text, num_rows int);

DO $$
DECLARE
  table_ref regclass;
  col name;
BEGIN
  FOR table_ref IN
    SELECT compressed_chunk as table_ref FROM compressed_chunks
  LOOP
    FOR col IN
      SELECT att.attname
      FROM pg_attribute att
      WHERE att.attrelid = table_ref
        AND att.atttypid = '_timescaledb_internal.compressed_data'::regtype
        AND att.attnum > 0
        AND NOT att.attisdropped
      ORDER BY att.attnum
    LOOP
      EXECUTE format(
        'INSERT INTO compression_info (
          SELECT
            %L::regclass as compressed_chunk,
            %L,
            (_timescaledb_functions.compressed_data_info(%I))::text as result,
            count(*) as num_rows
          FROM %s
          GROUP BY 1,2,3)',
        table_ref, col, col, table_ref
      );
    END LOOP;
  END LOOP;
END;
$$;

SELECT
  ci.*,
  ccs.numrows_pre_compression,
  ccs.numrows_post_compression
FROM
  compression_info ci
  INNER JOIN compressed_chunks ccs
    ON (ci.compressed_chunk = ccs.compressed_chunk)
ORDER BY
  1,2,3;

-- cleanup:
DROP TABLE compression_info;
DROP TABLE compressed_chunks;
DROP TABLE source;
DROP FUNCTION prand(int, int);
RESET timescaledb.enable_rapid_raccoon_compression;
