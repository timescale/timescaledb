-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE TABLE readings(time timestamptz, location int, device int, temp float, humidity float);

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


SET timescaledb.enable_transparent_decompression TO false;

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

-- We should be able to change it back to heap.
ALTER TABLE :chunk SET ACCESS METHOD heap;

-- Should give the same result as above
SELECT device, count(*) INTO decomp FROM readings GROUP BY device;

-- Row counts for each device should match, so this should be empty.
SELECT device FROM orig JOIN decomp USING (device) WHERE orig.count != decomp.count;
