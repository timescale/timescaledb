-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Tests for parameterized ChunkAppend in ordered mode crashing when creating
-- a MergeAppend because the latter doesn't support parameterization in
-- Postgres. We cover several code paths where this can happen (space
-- partitioning and partially compressed chunks).

\set PREFIX 'EXPLAIN (costs off)'

SET max_parallel_workers_per_gather = 0;
SET enable_material = off;
SET enable_seqscan = off;
SET enable_hashjoin = off;
SET enable_mergejoin = off;

----------------------------------------------------------------------
-- Case 1: Single-dimension + partial compression
----------------------------------------------------------------------
CREATE TABLE ht_single(time timestamptz NOT NULL, device int NOT NULL, value float);
SELECT table_name FROM create_hypertable('ht_single','time');

ALTER TABLE ht_single SET (
  timescaledb.compress,
  timescaledb.compress_segmentby='device',
  timescaledb.compress_orderby='time'
);

CREATE INDEX ht_single_device_time_idx ON ht_single (device, time);

INSERT INTO ht_single
SELECT time, device, device * 0.1
FROM generate_series('2020-01-01'::timestamptz, '2020-01-14'::timestamptz, '4 hour') time,
     generate_series(1,3) device;

SELECT count(compress_chunk(c)) FROM show_chunks('ht_single') c;

-- Make partially compressed
INSERT INTO ht_single
SELECT time, device, device * 0.1
FROM generate_series('2020-01-01'::timestamptz, '2020-01-14'::timestamptz, '6 hour') time,
     generate_series(1,3) device;

CREATE TABLE devices1(device int);
INSERT INTO devices1 SELECT generate_series(1,3);
ANALYZE ht_single, devices1;

:PREFIX
SELECT m.time, m.device, m.value
FROM devices1 d
JOIN ht_single m ON m.device = d.device
WHERE m.time < now()
ORDER BY m.time;

DROP TABLE ht_single, devices1;

----------------------------------------------------------------------
-- Case 2: Space-partitioned (fully compressed)
----------------------------------------------------------------------
CREATE TABLE ht_space(time timestamptz NOT NULL, device int NOT NULL, value float);
SELECT table_name FROM create_hypertable('ht_space','time');
SELECT add_dimension('ht_space', 'device', number_partitions => 2);

ALTER TABLE ht_space SET (
  timescaledb.compress,
  timescaledb.compress_segmentby='device',
  timescaledb.compress_orderby='time'
);

CREATE INDEX ht_space_device_time_idx ON ht_space (device, time);

INSERT INTO ht_space
SELECT time, device, device * 0.1
FROM generate_series('2020-01-01'::timestamptz, '2020-01-14'::timestamptz, '4 hour') time,
     generate_series(1,4) device;

SELECT count(compress_chunk(c)) FROM show_chunks('ht_space') c;

CREATE TABLE devices2(device int);
INSERT INTO devices2 SELECT generate_series(1,4);
ANALYZE ht_space, devices2;

:PREFIX
SELECT m.time, m.device, m.value
FROM devices2 d
JOIN ht_space m ON m.device = d.device
WHERE m.time < now()
ORDER BY m.time;

DROP TABLE ht_space, devices2;

----------------------------------------------------------------------
-- Case 3: Space-partitioned + partial compression
----------------------------------------------------------------------
CREATE TABLE ht_space_partial(time timestamptz NOT NULL, device int NOT NULL, value float);
SELECT table_name FROM create_hypertable('ht_space_partial','time');
SELECT add_dimension('ht_space_partial', 'device', number_partitions => 2);

ALTER TABLE ht_space_partial SET (
  timescaledb.compress,
  timescaledb.compress_segmentby='device',
  timescaledb.compress_orderby='time'
);

CREATE INDEX ht_space_partial_device_time_idx ON ht_space_partial (device, time);

INSERT INTO ht_space_partial
SELECT time, device, device * 0.1
FROM generate_series('2020-01-01'::timestamptz, '2020-01-14'::timestamptz, '4 hour') time,
     generate_series(1,4) device;

SELECT count(compress_chunk(c)) FROM show_chunks('ht_space_partial') c;

-- Make partially compressed
INSERT INTO ht_space_partial
SELECT time, device, device * 0.1
FROM generate_series('2020-01-01'::timestamptz, '2020-01-14'::timestamptz, '6 hour') time,
     generate_series(1,4) device;

CREATE TABLE devices3(device int);
INSERT INTO devices3 SELECT generate_series(1,4);
ANALYZE ht_space_partial, devices3;

:PREFIX
SELECT m.time, m.device, m.value
FROM devices3 d
JOIN ht_space_partial m ON m.device = d.device
WHERE m.time < now()
ORDER BY m.time;

RESET enable_material;
RESET enable_seqscan;
RESET enable_hashjoin;
RESET enable_mergejoin;
RESET max_parallel_workers_per_gather;
DROP TABLE ht_space_partial, devices3;
