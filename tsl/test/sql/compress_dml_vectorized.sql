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
