-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- need superuser for copy from program
\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE TABLE metrics (time TIMESTAMPTZ NOT NULL, device TEXT, value float) WITH (tsdb.hypertable, tsdb.partition_column = 'time');

-- first try without the GUCs
BEGIN;
COPY metrics FROM PROGRAM 'seq 100 | xargs -II date -d "2025-01-01 + I hour" +"%Y-%m-%d %H:%M:%S,d1,0.I"' WITH (FORMAT CSV);
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM metrics;
ROLLBACK;

SET timescaledb.enable_compressed_copy = true;
SET timescaledb.enable_compressed_copy_presorted = true;

-- simple test with compressed copy enabled
BEGIN;
COPY metrics FROM PROGRAM 'seq 10000 | xargs -II date -d "2025-01-01 + I minute" +"%Y-%m-%d %H:%M:%S,d1,0.I"' WITH (FORMAT CSV);
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM metrics;
ROLLBACK;

-- test compressing into uncompressed chunk
RESET timescaledb.enable_compressed_copy;
RESET timescaledb.enable_compressed_copy_presorted;
BEGIN;
COPY metrics FROM PROGRAM 'seq 10000 | xargs -II date -d "2025-01-01 + I minute" +"%Y-%m-%d %H:%M:%S,d1,0.I"' WITH (FORMAT CSV);
SET timescaledb.enable_compressed_copy = true;
SET timescaledb.enable_compressed_copy_presorted = true;
COPY metrics FROM PROGRAM 'seq 10000 | xargs -II date -d "2025-01-01 + I minute" +"%Y-%m-%d %H:%M:%S,d1,0.I"' WITH (FORMAT CSV);
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM metrics;
SELECT DISTINCT status FROM _timescaledb_catalog.chunk WHERE compressed_chunk_id IS NOT NULL;
ROLLBACK;

-- simple test with compressed copy enabled and reversed order
BEGIN;
SET timescaledb.enable_compressed_copy = true;
SET timescaledb.enable_compressed_copy_presorted = true;
COPY metrics FROM PROGRAM 'seq 10000 | xargs -II date -d "2025-01-01 - I minute" +"%Y-%m-%d %H:%M:%S,d1,0.I"' WITH (FORMAT CSV);
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM metrics;
-- status should be 9
SELECT DISTINCT status FROM _timescaledb_catalog.chunk WHERE compressed_chunk_id IS NOT NULL;
ROLLBACK;

-- simple test with compressed copy enabled and no presorted
BEGIN;
SET timescaledb.enable_compressed_copy = true;
SET timescaledb.enable_compressed_copy_presorted = false;
COPY metrics FROM PROGRAM 'seq 10000 | xargs -II date -d "2025-01-01 - I minute" +"%Y-%m-%d %H:%M:%S,d1,0.I"' WITH (FORMAT CSV);
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM metrics;
-- status should be 11
SELECT DISTINCT status FROM _timescaledb_catalog.chunk WHERE compressed_chunk_id IS NOT NULL;
ROLLBACK;

-- test with segmentby
BEGIN;
ALTER TABLE metrics SET (tsdb.segmentby = 'device');
SET timescaledb.enable_compressed_copy = true;
SET timescaledb.enable_compressed_copy_presorted = true;
COPY metrics FROM PROGRAM 'seq 0 0.2 9.8 | sed -e ''s!.[0-9]$!!'' | xargs -II date -d "2025-01-01 - I minute" +"%Y-%m-%d %H:%M:%S,dI,0.I"' WITH (FORMAT CSV);
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM metrics;
SELECT format('%I.%I',schema_name,table_name) AS "COMPRESSED_CHUNK" FROM _timescaledb_catalog.chunk where compressed_chunk_id IS NULL \gset
-- should have 10 batches
SELECT count(*) FROM :COMPRESSED_CHUNK;
ROLLBACK;

-- simple test with unique constraints
BEGIN;
SET timescaledb.enable_compressed_copy = true;
ALTER TABLE metrics ADD CONSTRAINT unique_time_device UNIQUE (time, device);
COPY metrics FROM PROGRAM 'seq 100 | xargs -II date -d "2025-01-01 - I minute" +"%Y-%m-%d %H:%M:%S,d1,0.I"' WITH (FORMAT CSV);
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM metrics;
SELECT DISTINCT status FROM _timescaledb_catalog.chunk WHERE compressed_chunk_id IS NOT NULL;
ROLLBACK;

