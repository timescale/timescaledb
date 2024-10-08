-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
show timescaledb.hyperstore_indexam_whitelist;
set role :ROLE_DEFAULT_PERM_USER;

SET timescaledb.arrow_cache_maxsize = 4;

CREATE TABLE readings(
       time timestamptz UNIQUE,
       location int,
       device int,
       temp numeric(4,1),
       humidity float,
       jdata jsonb
);

SELECT create_hypertable('readings', by_range('time', '1d'::interval));
-- Disable incremental sort to make tests stable
SET enable_incremental_sort = false;
SELECT setseed(1);

INSERT INTO readings (time, location, device, temp, humidity, jdata)
SELECT t, ceil(random()*10), ceil(random()*30), random()*40, random()*100, '{"a":1,"b":2}'::jsonb
FROM generate_series('2022-06-01'::timestamptz, '2022-07-01'::timestamptz, '5m') t;

ALTER TABLE readings SET (
	  timescaledb.compress,
	  timescaledb.compress_orderby = 'time',
	  timescaledb.compress_segmentby = 'device'
);

-- Set some test chunks as global variables
SELECT format('%I.%I', chunk_schema, chunk_name)::regclass AS chunk
  FROM timescaledb_information.chunks
 WHERE format('%I.%I', hypertable_schema, hypertable_name)::regclass = 'readings'::regclass
 LIMIT 1 \gset

SELECT format('%I.%I', chunk_schema, chunk_name)::regclass AS chunk2
  FROM timescaledb_information.chunks
 WHERE format('%I.%I', hypertable_schema, hypertable_name)::regclass = 'readings'::regclass
 ORDER BY chunk2 DESC
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
ALTER TABLE :chunk SET ACCESS METHOD hyperstore;
SET timescaledb.enable_transparent_decompression TO false;

vacuum analyze readings;

-- Show access method used on chunk
SELECT c.relname, a.amname FROM pg_class c
INNER JOIN pg_am a ON (c.relam = a.oid)
WHERE c.oid = :'chunk'::regclass;

-- This should show the chunk as compressed
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

-- Check that we error out on unsupported index types
\set ON_ERROR_STOP 0
create index on readings using brin (device);
create index on readings using gin (jdata);
create index on readings using magicam (device);
\set ON_ERROR_STOP 1

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

SET enable_indexscan = false;

-- Columnar scan with qual on segmentby where filtering should be
-- turned into scankeys
EXPLAIN (costs off, timing off, summary off)
SELECT * FROM :chunk WHERE device < 4 ORDER BY time, device LIMIT 5;
SELECT * FROM :chunk WHERE device < 4 ORDER BY time, device LIMIT 5;

-- Show with indexscan
SET enable_indexscan = true;
SET enable_seqscan = false;
SET timescaledb.enable_columnarscan = false;
EXPLAIN (costs off, timing off, summary off)
SELECT * FROM :chunk WHERE device < 4 ORDER BY time, device LIMIT 5;
SELECT * FROM :chunk WHERE device < 4 ORDER BY time, device LIMIT 5;
SET enable_indexscan = false;

-- Compare the output to transparent decompression. Heap output is
-- shown further down.
SET timescaledb.enable_transparent_decompression TO 'hyperstore';
EXPLAIN (costs off, timing off, summary off)
SELECT * FROM :chunk WHERE device < 4 ORDER BY time, device LIMIT 5;
SELECT * FROM :chunk WHERE device < 4 ORDER BY time, device LIMIT 5;
SET timescaledb.enable_transparent_decompression TO false;

-- Qual on compressed column with index
SET timescaledb.enable_columnarscan = true;
EXPLAIN (costs off, timing off, summary off)
SELECT * FROM :chunk WHERE location < 4 ORDER BY time, device LIMIT 5;
SELECT * FROM :chunk WHERE location < 4 ORDER BY time, device LIMIT 5;

-- With index scan
SET enable_indexscan = true;
SET timescaledb.enable_columnarscan = false;
EXPLAIN (costs off, timing off, summary off)
SELECT * FROM :chunk WHERE location < 4 ORDER BY time, device LIMIT 5;
SELECT * FROM :chunk WHERE location < 4 ORDER BY time, device LIMIT 5;
SET enable_indexscan = false;
SET enable_seqscan = true;
SET timescaledb.enable_columnarscan = true;

-- With transparent decompression
SET timescaledb.enable_transparent_decompression TO 'hyperstore';
SELECT * FROM :chunk WHERE location < 4 ORDER BY time, device LIMIT 5;
SET timescaledb.enable_transparent_decompression TO false;

-- Ordering on compressed column that has index
SET enable_indexscan = true;
EXPLAIN (costs off, timing off, summary off)
SELECT * FROM :chunk ORDER BY location ASC LIMIT 5;
SELECT * FROM :chunk ORDER BY location ASC LIMIT 5;

-- Show with transparent decompression
SET timescaledb.enable_transparent_decompression TO 'hyperstore';
SELECT * FROM :chunk ORDER BY location ASC LIMIT 5;
SET timescaledb.enable_transparent_decompression TO false;

-- We should be able to change it back to heap.
-- Compression metadata should be cleaned up
SELECT count(*) FROM _timescaledb_catalog.compression_chunk_size ccs
INNER JOIN _timescaledb_catalog.chunk c ON (c.id = ccs.chunk_id)
WHERE format('%I.%I', c.schema_name, c.table_name)::regclass = :'chunk'::regclass;

SELECT device, count(*) INTO num_rows_before FROM :chunk GROUP BY device;

SELECT format('%I.%I', c2.schema_name, c2.table_name)::regclass AS cchunk
FROM _timescaledb_catalog.chunk c1
INNER JOIN _timescaledb_catalog.chunk c2
ON (c1.compressed_chunk_id = c2.id);
ALTER TABLE :chunk SET ACCESS METHOD heap;
SET timescaledb.enable_transparent_decompression TO 'hyperstore';

-- The compressed chunk should no longer exist
SELECT format('%I.%I', c2.schema_name, c2.table_name)::regclass AS cchunk
FROM _timescaledb_catalog.chunk c1
INNER JOIN _timescaledb_catalog.chunk c2
ON (c1.compressed_chunk_id = c2.id);

SELECT device, count(*) INTO num_rows_after FROM :chunk GROUP BY device;
SELECT device, num_rows_after.count AS after,
	   num_rows_before.count AS before,
	   (num_rows_after.count - num_rows_before.count) AS diff
FROM num_rows_after JOIN num_rows_before USING (device)
WHERE num_rows_after.count != num_rows_before.count;

SELECT count(*) FROM _timescaledb_catalog.compression_chunk_size ccs
INNER JOIN _timescaledb_catalog.chunk c ON (c.id = ccs.chunk_id)
WHERE format('%I.%I', c.schema_name, c.table_name)::regclass = :'chunk'::regclass;

SELECT compress_chunk(:'chunk');

-- A new compressed chunk should be created
SELECT format('%I.%I', c2.schema_name, c2.table_name)::regclass AS cchunk
FROM _timescaledb_catalog.chunk c1
INNER JOIN _timescaledb_catalog.chunk c2
ON (c1.compressed_chunk_id = c2.id);

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

-- Convert back to hyperstore to check that metadata was cleaned up
-- from last time this table used hyperstore
ALTER TABLE :chunk SET ACCESS METHOD hyperstore;
SET timescaledb.enable_transparent_decompression TO false;

-- Get the chunk's corresponding compressed chunk
SELECT format('%I.%I', c2.schema_name, c2.table_name)::regclass AS cchunk
FROM _timescaledb_catalog.chunk c1
INNER JOIN _timescaledb_catalog.chunk c2
ON (c1.compressed_chunk_id = c2.id) LIMIT 1 \gset

SELECT range_start, range_end
FROM timescaledb_information.chunks
WHERE format('%I.%I', chunk_schema, chunk_name)::regclass = :'chunk'::regclass;

--
-- ADD COLUMN
--
-- Check that adding a column works across recompression.  First save
-- some sample data from the table that will be used as a comparison
-- to ensure adding a column doesn't mess up the data or column
-- mapping.
CREATE TEMP TABLE sample_readings AS
SELECT * FROM readings
WHERE time BETWEEN '2022-06-01 00:00:00' AND '2022-06-01 00:10:00'::timestamptz;

SELECT count(*) FROM sample_readings;

-- Now add the column
ALTER TABLE readings ADD COLUMN pressure float;

-- Check that the sample data remains the same in the modified
-- table. Should return the same count as above if everything is the
-- same.
SELECT count(*) FROM readings r
JOIN sample_readings s USING (time, location, device, temp, humidity);

-- insert some new (non-compressed) data into the chunk in order to
-- test recompression
INSERT INTO :chunk (time, location, device, temp, humidity, pressure)
SELECT t, ceil(random()*10), ceil(random()*30), random()*40, random()*100, random() * 30
FROM generate_series('2022-06-01 00:06:15'::timestamptz, '2022-06-01 17:00', '5m') t;

-- Check that new data is returned
SELECT * FROM :chunk WHERE time = '2022-06-01 00:06:15'::timestamptz;

-- Want to check that index scans work after recompression, so the
-- query should be an index scan.
EXPLAIN (verbose, costs off)
SELECT * FROM :chunk WHERE time = '2022-06-01 00:06:15'::timestamptz;

-- Show counts in compressed chunk prior to recompression
SELECT sum(_ts_meta_count) FROM :cchunk;
CALL recompress_chunk(:'chunk');

-- Data should be returned even after recompress, but now from the
-- compressed relation. Still using index scan.
EXPLAIN (verbose, costs off)
SELECT * FROM :chunk WHERE time = '2022-06-01 00:06:15'::timestamptz;
SELECT * FROM :chunk WHERE time = '2022-06-01 00:06:15'::timestamptz;

-- Drop column and add again
ALTER TABLE readings DROP COLUMN pressure;

EXPLAIN (verbose, costs off)
SELECT * FROM :chunk WHERE time = '2022-06-01 00:06:15'::timestamptz;
SELECT * FROM :chunk WHERE time = '2022-06-01 00:06:15'::timestamptz;

ALTER TABLE readings ADD COLUMN pressure float;

EXPLAIN (verbose, costs off)
SELECT * FROM :chunk WHERE time = '2022-06-01 00:06:15'::timestamptz;
SELECT * FROM :chunk WHERE time = '2022-06-01 00:06:15'::timestamptz;

\set ON_ERROR_STOP 0
-- Can't recompress twice without new non-compressed rows
CALL recompress_chunk(:'chunk');
\set ON_ERROR_STOP 1

-- Compressed count after recompression
SELECT sum(_ts_meta_count) FROM :cchunk;

-- A count on the chunk should return the same count
SELECT count(*) FROM :chunk;

drop table readings;
