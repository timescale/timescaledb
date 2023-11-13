-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE TABLE readings(time timestamptz UNIQUE, location int, device int, temp float, humidity float);

SELECT create_hypertable('readings', 'time');

SELECT setseed(1);

INSERT INTO readings (time, location, device, temp, humidity)
SELECT t, ceil(random()*10), ceil(random()*30), random()*40, random()*100
FROM generate_series('2022-06-01'::timestamptz, '2022-07-01', '5s') t;

ALTER TABLE readings SET (
	  timescaledb.compress,
	  timescaledb.compress_orderby = 'time',
	  timescaledb.compress_segmentby = 'device'
);

SELECT format('%I.%I', chunk_schema, chunk_name)::regclass AS chunk
  FROM timescaledb_information.chunks
 WHERE format('%I.%I', hypertable_schema, hypertable_name)::regclass = 'readings'::regclass
 LIMIT 1 \gset

-- We do some basic checks that the compressed data is the same as the
-- uncompressed. In this case, we just count the rows for each device.
SELECT device, count(*) INTO orig FROM readings GROUP BY device;

-- Initially an index on time
SELECT * FROM test.show_indexes(:'chunk');

EXPLAIN (verbose, costs off)
SELECT count(*) FROM :chunk
WHERE time = '2022-06-01'::timestamptz;

SELECT count(*) FROM :chunk
WHERE time = '2022-06-01'::timestamptz;

SELECT count(*) FROM :chunk
WHERE location = 1;

-- We should be able to set the table access method for a chunk, which
-- will automatically compress the chunk.
ALTER TABLE :chunk SET ACCESS METHOD tscompression;
SET timescaledb.enable_transparent_decompression TO false;

-- Show access method used on chunk
SELECT c.relname, a.amname FROM pg_class c
INNER JOIN pg_am a ON (c.relam = a.oid)
WHERE c.oid = :'chunk'::regclass;

-- This should compress the chunk
SELECT chunk_name FROM chunk_compression_stats('readings') WHERE compression_status='Compressed';

-- Should give the same result as above
SELECT device, count(*) INTO comp FROM readings GROUP BY device;

-- Row counts for each device should match, so this should be empty.
SELECT device FROM orig JOIN comp USING (device) WHERE orig.count != comp.count;

EXPLAIN (verbose, costs off)
SELECT count(*) FROM :chunk
WHERE time = '2022-06-01'::timestamptz;

SELECT count(*) FROM :chunk
WHERE time = '2022-06-01'::timestamptz;

-- Create a new index on a compressed column
CREATE INDEX ON readings (location);

-- Index added on location
SELECT * FROM test.show_indexes(:'chunk');

-- Query by location should be an index scan
EXPLAIN (verbose, costs off)
SELECT count(*) FROM :chunk
WHERE location = 1;

-- Count by location should be the same as non-index scan before
-- compression above
SELECT count(*) FROM :chunk
WHERE location = 1;


-- Test that a conflict happens when inserting a value that already exists in the
-- compressed part of the chunk
SELECT count(*) FROM :chunk WHERE time  = '2022-06-01'::timestamptz;

\set ON_ERROR_STOP 0
INSERT INTO readings VALUES ('2022-06-01', 1, 1, 1.0, 1.0);
-- Same result if inserted directly into the compressed chunk
INSERT INTO :chunk VALUES ('2022-06-01', 1, 1, 1.0, 1.0);
\set ON_ERROR_STOP 1

-- Test insert of a non-conflicting value into the compressed chunk,
-- first directly into chunk and then via hypertable. The value should
-- end up in the non-compressed part in contrast to the conflicting
-- value above.
INSERT INTO :chunk VALUES ('2022-06-01 00:00:02', 1, 1, 1.0, 1.0);
INSERT INTO readings VALUES ('2022-06-01 00:00:03', 1, 1, 1.0, 1.0);

SELECT * FROM readings WHERE time BETWEEN '2022-06-01'::timestamptz AND '2022-06-01 01:00'::timestamptz ORDER BY time ASC LIMIT 10;

-- Inserting the same values again should lead to conflicts
\set ON_ERROR_STOP 0
INSERT INTO :chunk VALUES ('2022-06-01 00:00:02', 1, 1, 1.0, 1.0);
INSERT INTO readings VALUES ('2022-06-01 00:00:03'::timestamptz, 1, 1, 1.0, 1.0);
\set ON_ERROR_STOP 1

SELECT device, count(*) FROM readings WHERE device=1 GROUP BY device;
-- Speculative insert when conflicting row is in the non-compressed part
INSERT INTO :chunk VALUES ('2022-06-01 00:00:02', 2, 1, 1.0, 1.0) ON CONFLICT (time) DO UPDATE SET location = 11;
-- Show the updated tuple
SELECT * FROM readings WHERE time = '2022-06-01 00:00:02'::timestamptz;
INSERT INTO readings VALUES ('2022-06-01 00:00:02', 3, 1, 1.0, 1.0) ON CONFLICT (time) DO UPDATE SET location = 12;
SELECT * FROM readings WHERE time = '2022-06-01 00:00:02'::timestamptz;

-- Speculative insert when conflicting row is in the compressed part
\set ON_ERROR_STOP 0
INSERT INTO :chunk VALUES ('2022-06-01', 2, 1, 1.0, 1.0) ON CONFLICT (time) DO UPDATE SET location = 13;
\set ON_ERROR_STOP 1
SELECT * FROM readings WHERE time = '2022-06-01'::timestamptz;
INSERT INTO readings VALUES ('2022-06-01', 3, 1, 1.0, 1.0) ON CONFLICT (time) DO UPDATE SET location = 14;
SELECT * FROM readings WHERE time = '2022-06-01'::timestamptz;

-- Speculative insert without a conflicting
INSERT INTO :chunk VALUES ('2022-06-01 00:00:06', 2, 1, 1.0, 1.0) ON CONFLICT (time) DO UPDATE SET location = 15;
SELECT * FROM readings WHERE time = '2022-06-01 00:00:06';
INSERT INTO readings VALUES ('2022-06-01 00:00:07', 3, 1, 1.0, 1.0) ON CONFLICT (time) DO UPDATE SET location = 16;
SELECT * FROM readings WHERE time = '2022-06-01 00:00:07';

INSERT INTO readings VALUES ('2022-06-01', 3, 1, 1.0, 1.0) ON CONFLICT (time) DO NOTHING;

SET enable_indexscan = false;
SET enable_bitmapscan = false; -- currently doesn't work on compression TAM

-- Columnar scan with qual on segmentby where filtering should be
-- turned into scankeys
EXPLAIN (costs off, timing off, summary off)
SELECT * FROM :chunk WHERE device < 4 ORDER BY time, device LIMIT 5;
SELECT * FROM :chunk WHERE device < 4 ORDER BY time, device LIMIT 5;

-- Show with indexscan
SET enable_indexscan = true;
EXPLAIN (costs off, timing off, summary off)
SELECT * FROM :chunk WHERE device < 4 ORDER BY time, device LIMIT 5;
SELECT * FROM :chunk WHERE device < 4 ORDER BY time, device LIMIT 5;
SET enable_indexscan = false;

-- Compare the output to transparent decompression. Heap output is
-- shown further down.
SET timescaledb.enable_transparent_decompression TO true;
EXPLAIN (costs off, timing off, summary off)
SELECT * FROM :chunk WHERE device < 4 ORDER BY time, device LIMIT 5;
SELECT * FROM :chunk WHERE device < 4 ORDER BY time, device LIMIT 5;
SET timescaledb.enable_transparent_decompression TO false;

-- Qual on compressed column with index
EXPLAIN (costs off, timing off, summary off)
SELECT * FROM :chunk WHERE location < 4 ORDER BY time, device LIMIT 5;
SELECT * FROM :chunk WHERE location < 4 ORDER BY time, device LIMIT 5;

-- With index scan
SET enable_indexscan = true;
EXPLAIN (costs off, timing off, summary off)
SELECT * FROM :chunk WHERE location < 4 ORDER BY time, device LIMIT 5;
SELECT * FROM :chunk WHERE location < 4 ORDER BY time, device LIMIT 5;
SET enable_indexscan = false;

-- With transparent decompression
SET timescaledb.enable_transparent_decompression TO true;
SELECT * FROM :chunk WHERE location < 4 ORDER BY time, device LIMIT 5;
SET timescaledb.enable_transparent_decompression TO false;

-- Ordering on compressed column that has index
SET enable_indexscan = true;
EXPLAIN (costs off, timing off, summary off)
SELECT * FROM :chunk ORDER BY location ASC LIMIT 5;
SELECT * FROM :chunk ORDER BY location ASC LIMIT 5;

-- Show with transparent decompression
SET timescaledb.enable_transparent_decompression TO true;
SELECT * FROM :chunk ORDER BY location ASC LIMIT 5;
SET timescaledb.enable_transparent_decompression TO false;

-- Test that filtering is not removed on ColumnarScan when it includes
-- columns that cannot be scankeys.
DROP INDEX readings_location_idx;
EXPLAIN (analyze, costs off, timing off, summary off)
SELECT * FROM :chunk WHERE device < 4 AND location = 2 LIMIT 5;

-- Test that columnar scan can be turned off
SET timescaledb.enable_columnarscan = false;
EXPLAIN (analyze, costs off, timing off, summary off)
SELECT * FROM :chunk WHERE device < 4 ORDER BY device ASC LIMIT 5;


-- We should be able to change it back to heap.
ALTER TABLE :chunk SET ACCESS METHOD heap;

-- Show same output as first query above but for heap
SELECT * FROM :chunk WHERE device < 4 ORDER BY time, device LIMIT 5;

-- Show access method used on chunk
SELECT c.relname, a.amname FROM pg_class c
INNER JOIN pg_am a ON (c.relam = a.oid)
WHERE c.oid = :'chunk'::regclass;

-- Should give the same result as above
SELECT device, count(*) INTO decomp FROM readings GROUP BY device;

-- Row counts for each device should match, except for the chunk we did inserts on.
SELECT device, orig.count AS orig_count, decomp.count AS decomp_count, (decomp.count - orig.count) AS diff
FROM orig JOIN decomp USING (device) WHERE orig.count != decomp.count;
