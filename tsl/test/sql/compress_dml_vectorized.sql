-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- UPDATE and DELETE on a compressed chunk check every batch that passed the
-- index, heap and bloom filters against the in-memory scan keys before
-- decompressing it. With bulk decompression enabled the key columns are
-- decompressed in bulk and checked with vector predicates
-- (batch_matches_vectorized), otherwise the batch is checked row by row
-- (batch_matches). Both must filter the same batches and change the same
-- rows, so the EXPLAIN ANALYZE output is expected to be identical.

CREATE TABLE vect_dml(
    time timestamptz NOT NULL,
    device int,
    sensor int,
    value float8);
SELECT create_hypertable('vect_dml', 'time', create_default_indexes => false);

-- 5000 rows in one chunk, 5 devices: 5 compressed batches of 1000 rows.
-- sensor = 7 only occurs in the batch of device 2 and sensor = 13 only in
-- the batch of device 3, 50 rows each. There is no index and no sparse
-- index on sensor, so only the in-memory check can filter the batches.
INSERT INTO vect_dml
SELECT '2024-01-01'::timestamptz + x * interval '1 second', x % 5, x % 100, x
FROM generate_series(1, 5000) x;

ALTER TABLE vect_dml SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device',
    timescaledb.compress_orderby = 'time');
SELECT count(compress_chunk(c)) FROM show_chunks('vect_dml') c;

-- The UPDATE scans 5 batches, filters 4 after decompressing their sensor
-- column and decompresses the batch of device 2. The DELETE then scans the
-- remaining 4 batches, filters 3 and decompresses the batch of device 3.

-- vectorized check
SET timescaledb.enable_bulk_decompression = on;
BEGIN;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
UPDATE vect_dml SET value = value + 1 WHERE sensor = 7;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
DELETE FROM vect_dml WHERE sensor = 13;
ROLLBACK;

-- row by row check
SET timescaledb.enable_bulk_decompression = off;
BEGIN;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
UPDATE vect_dml SET value = value + 1 WHERE sensor = 7;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
DELETE FROM vect_dml WHERE sensor = 13;
ROLLBACK;
RESET timescaledb.enable_bulk_decompression;

DROP TABLE vect_dml;

-- One batch read in every way at once: time, seq, temp, flag and id are
-- decompressed in bulk, label and amount through an iterator, status comes from
-- a dictionary, note is stored with the NULL algorithm, and extra was added
-- after compression.
CREATE TABLE mixed_dml(
    time timestamptz NOT NULL,
    device int,
    seq int,
    temp float8,
    label text,
    status text,
    flag bool,
    id uuid,
    amount numeric,
    note text,
    UNIQUE (device, time));
SELECT create_hypertable('mixed_dml', 'time', create_default_indexes => false);
INSERT INTO mixed_dml
SELECT '2024-01-01'::timestamptz + x * interval '1 minute', x % 5, x, x / 10.0,
    'label ' || x, CASE WHEN x % 3 = 0 THEN 'ok' ELSE 'warn' END, x % 2 = 0,
    md5(x::text)::uuid, x * 1.5, NULL
FROM generate_series(1, 60) x;
ALTER TABLE mixed_dml SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device',
    timescaledb.compress_orderby = 'time');
SELECT count(compress_chunk(c)) FROM show_chunks('mixed_dml') c;
ALTER TABLE mixed_dml ADD COLUMN extra int DEFAULT 42;

SELECT cs.compress_relid::text AS compressed_chunk
FROM _timescaledb_catalog.chunk c
JOIN _timescaledb_catalog.compression_settings cs ON cs.relid = c.relid
JOIN _timescaledb_catalog.hypertable h ON c.hypertable_id = h.id
WHERE h.table_name = 'mixed_dml' \gset

SELECT (_timescaledb_functions.compressed_data_info(time)).algorithm AS time,
    (_timescaledb_functions.compressed_data_info(seq)).algorithm AS seq,
    (_timescaledb_functions.compressed_data_info(temp)).algorithm AS temp,
    (_timescaledb_functions.compressed_data_info(flag)).algorithm AS flag,
    (_timescaledb_functions.compressed_data_info(id)).algorithm AS id,
    (_timescaledb_functions.compressed_data_info(label)).algorithm AS label,
    (_timescaledb_functions.compressed_data_info(status)).algorithm AS status,
    (_timescaledb_functions.compressed_data_info(amount)).algorithm AS amount,
    (_timescaledb_functions.compressed_data_info(note)).algorithm AS note
FROM :compressed_chunk WHERE device = 0;

-- Each statement decompresses a different batch: a row-by-row match on the
-- numeric key, a vectorized match on the array text, on the dictionary text and
-- on the delta-delta integer, and the conflict check of the upsert. The result
-- must not depend on the GUC.
SET timescaledb.enable_bulk_decompression = on;
BEGIN;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
DELETE FROM mixed_dml WHERE amount = 15.0;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
UPDATE mixed_dml SET temp = temp + 1 WHERE label = 'label 7';
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
UPDATE mixed_dml SET temp = temp + 1 WHERE status = 'ok' AND device = 1;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
UPDATE mixed_dml SET temp = temp + 1 WHERE seq = 13;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
INSERT INTO mixed_dml VALUES ('2024-01-01 00:24', 4, 24, 0, 'label 24', 'ok', true, md5('24')::uuid, 36, NULL)
ON CONFLICT (device, time) DO UPDATE SET temp = 0;
SELECT device, count(*), sum(temp), sum(amount), count(note), min(extra),
    md5(string_agg(label || status || flag::text || id::text, ',' ORDER BY time))
FROM mixed_dml GROUP BY device ORDER BY device;
ROLLBACK;

SET timescaledb.enable_bulk_decompression = off;
BEGIN;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
DELETE FROM mixed_dml WHERE amount = 15.0;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
UPDATE mixed_dml SET temp = temp + 1 WHERE label = 'label 7';
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
UPDATE mixed_dml SET temp = temp + 1 WHERE status = 'ok' AND device = 1;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
UPDATE mixed_dml SET temp = temp + 1 WHERE seq = 13;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
INSERT INTO mixed_dml VALUES ('2024-01-01 00:24', 4, 24, 0, 'label 24', 'ok', true, md5('24')::uuid, 36, NULL)
ON CONFLICT (device, time) DO UPDATE SET temp = 0;
SELECT device, count(*), sum(temp), sum(amount), count(note), min(extra),
    md5(string_agg(label || status || flag::text || id::text, ',' ORDER BY time))
FROM mixed_dml GROUP BY device ORDER BY device;
ROLLBACK;
RESET timescaledb.enable_bulk_decompression;

DROP TABLE mixed_dml;
