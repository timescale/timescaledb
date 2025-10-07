-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- need superuser for copy from program
\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE TABLE metrics (time TIMESTAMPTZ NOT NULL, device TEXT, value float) WITH (tsdb.hypertable, tsdb.partition_column = 'time', tsdb.orderby='time');

-- first try without the GUCs
BEGIN;
COPY metrics FROM PROGRAM 'seq 100 | xargs -II date -d "2025-01-01 + I hour" +"%Y-%m-%d %H:%M:%S,d1,0.I"' WITH (FORMAT CSV);
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM metrics;
ROLLBACK;

SET timescaledb.enable_direct_compress_copy = true;
SET timescaledb.enable_direct_compress_copy_sort_batches = true;
SET timescaledb.enable_direct_compress_copy_client_sorted = false;

-- simple test with compressed copy enabled
BEGIN;
COPY metrics FROM PROGRAM 'seq 3000 | xargs -II date -d "2025-01-01 + I minute" +"%Y-%m-%d %H:%M:%S,d1,0.I"' WITH (FORMAT CSV);
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM metrics;
SELECT first(time,rn), last(time,rn) FROM (SELECT ROW_NUMBER() OVER () as rn, time FROM metrics) sub;
ROLLBACK;

-- simple test with compressed copy enabled and reversed order
BEGIN;
COPY metrics FROM PROGRAM 'seq 3000 | xargs -II date -d "2025-01-01 - I minute" +"%Y-%m-%d %H:%M:%S,d1,0.I"' WITH (FORMAT CSV);
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM metrics;
SELECT first(time,rn), last(time,rn) FROM (SELECT ROW_NUMBER() OVER () as rn, time FROM metrics) sub;
ROLLBACK;

SET timescaledb.enable_direct_compress_copy_sort_batches = false;
-- simple test with compressed copy enabled and without batch sorting
BEGIN;
COPY metrics FROM PROGRAM 'seq 3000 | xargs -II date -d "2025-01-01 + I minute" +"%Y-%m-%d %H:%M:%S,d1,0.I"' WITH (FORMAT CSV);
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM metrics;
SELECT first(time,rn), last(time,rn) FROM (SELECT ROW_NUMBER() OVER () as rn, time FROM metrics) sub;
ROLLBACK;

-- simple test with compressed copy enabled and reversed order and no batch sorting
BEGIN;
COPY metrics FROM PROGRAM 'seq 3000 | xargs -II date -d "2025-01-01 - I minute" +"%Y-%m-%d %H:%M:%S,d1,0.I"' WITH (FORMAT CSV);
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM metrics;
SELECT first(time,rn), last(time,rn) FROM (SELECT ROW_NUMBER() OVER () as rn, time FROM metrics) sub;
ROLLBACK;

-- test compressing into uncompressed chunk
RESET timescaledb.enable_direct_compress_copy;
RESET timescaledb.enable_direct_compress_copy_sort_batches;
RESET timescaledb.enable_direct_compress_copy_client_sorted;

BEGIN;
COPY metrics FROM PROGRAM 'seq 3000 | xargs -II date -d "2025-01-01 + I minute" +"%Y-%m-%d %H:%M:%S,d1,0.I"' WITH (FORMAT CSV);
SET timescaledb.enable_direct_compress_copy = true;
SET timescaledb.enable_direct_compress_copy_client_sorted = true;
COPY metrics FROM PROGRAM 'seq 3000 | xargs -II date -d "2025-01-01 + I minute" +"%Y-%m-%d %H:%M:%S,d1,0.I"' WITH (FORMAT CSV);
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM metrics;
SELECT DISTINCT status FROM _timescaledb_catalog.chunk WHERE compressed_chunk_id IS NOT NULL;
ROLLBACK;

-- simple test with compressed copy enabled and reversed order
BEGIN;
SET timescaledb.enable_direct_compress_copy = true;
SET timescaledb.enable_direct_compress_copy_client_sorted = true;
COPY metrics FROM PROGRAM 'seq 3000 | xargs -II date -d "2025-01-01 - I minute" +"%Y-%m-%d %H:%M:%S,d1,0.I"' WITH (FORMAT CSV);
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM metrics;
-- status should be 9
SELECT DISTINCT status FROM _timescaledb_catalog.chunk WHERE compressed_chunk_id IS NOT NULL;
ROLLBACK;

-- simple test with compressed copy enabled and no presorted
BEGIN;
SET timescaledb.enable_direct_compress_copy = true;
SET timescaledb.enable_direct_compress_copy_client_sorted = false;
COPY metrics FROM PROGRAM 'seq 3000 | xargs -II date -d "2025-01-01 - I minute" +"%Y-%m-%d %H:%M:%S,d1,0.I"' WITH (FORMAT CSV);
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM metrics;
-- status should be 11
SELECT DISTINCT status FROM _timescaledb_catalog.chunk WHERE compressed_chunk_id IS NOT NULL;
ROLLBACK;

-- test with segmentby
BEGIN;
ALTER TABLE metrics SET (tsdb.segmentby = 'device');
SET timescaledb.enable_direct_compress_copy = true;
SET timescaledb.enable_direct_compress_copy_client_sorted = true;
COPY metrics FROM PROGRAM 'seq 0 0.2 9.8 | sed -e ''s!.[0-9]$!!'' | xargs -II date -d "2025-01-01 - I minute" +"%Y-%m-%d %H:%M:%S,dI,0.I"' WITH (FORMAT CSV);
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM metrics;
SELECT format('%I.%I',schema_name,table_name) AS "COMPRESSED_CHUNK" FROM _timescaledb_catalog.chunk where compressed_chunk_id IS NULL \gset
-- should have 10 batches
SELECT count(*) FROM :COMPRESSED_CHUNK;
ROLLBACK;

-- test unique constraints prevent direct compress
BEGIN;
SET timescaledb.enable_direct_compress_copy = true;
ALTER TABLE metrics ADD CONSTRAINT unique_time_device UNIQUE (time, device);
COPY metrics FROM STDIN WITH (FORMAT CSV);
2025-01-01,d1,0.3
\.
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM metrics;
SELECT DISTINCT status FROM _timescaledb_catalog.chunk WHERE compressed_chunk_id IS NOT NULL;
ROLLBACK;

-- test triggers prevent direct compress
BEGIN;
SET timescaledb.enable_direct_compress_copy = true;
CREATE OR REPLACE FUNCTION test_trigger() RETURNS TRIGGER AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql;
CREATE TRIGGER metrics_trigger BEFORE INSERT OR UPDATE ON metrics FOR EACH ROW EXECUTE FUNCTION test_trigger();
COPY metrics FROM STDIN WITH (FORMAT CSV);
2025-01-01,d1,0.3
\.
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM metrics;
SELECT DISTINCT status FROM _timescaledb_catalog.chunk WHERE compressed_chunk_id IS NOT NULL;
ROLLBACK;

-- test caggs prevent direct compress
BEGIN;
SET timescaledb.enable_direct_compress_copy = true;
CREATE MATERIALIZED VIEW metrics_cagg WITH (tsdb.continuous) AS SELECT time_bucket('1 hour', time) AS bucket, device, avg(value) AS avg_value FROM metrics GROUP BY bucket, device WITH NO DATA;
COPY metrics FROM STDIN WITH (FORMAT CSV);
2025-01-01,d1,0.3
\.
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM metrics;
SELECT DISTINCT status FROM _timescaledb_catalog.chunk WHERE compressed_chunk_id IS NOT NULL;
ROLLBACK;


-- test chunk status handling
CREATE TABLE metrics_status(time timestamptz) WITH (tsdb.hypertable,tsdb.partition_column='time');

-- normal insert should result in chunk status 0
INSERT INTO metrics_status SELECT '2025-01-01';
SELECT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_status') chunk;

BEGIN;
-- compressed copy into uncompressed chunk should result in chunk status 11 (compressed,partial,unordered)
SET timescaledb.enable_direct_compress_copy = true;
SET timescaledb.enable_direct_compress_copy_client_sorted = false;
COPY metrics_status FROM STDIN;
2025-01-01
\.
SELECT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_status') chunk;
ROLLBACK;

BEGIN;
-- compressed sorted copy into uncompressed chunk should result in chunk status 9 (compressed,partial)
SET timescaledb.enable_direct_compress_copy = true;
SET timescaledb.enable_direct_compress_copy_client_sorted = true;
COPY metrics_status FROM STDIN;
2025-01-01
\.
SELECT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_status') chunk;
ROLLBACK;

TRUNCATE metrics_status;

BEGIN;
-- compressed copy into new chunk should result in chunk status 3 (compressed,unordered)
SET timescaledb.enable_direct_compress_copy = true;
SET timescaledb.enable_direct_compress_copy_client_sorted = false;
COPY metrics_status FROM STDIN;
2025-01-01
\.
SELECT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_status') chunk;
ROLLBACK;

BEGIN;
-- compressed sorted copy into new chunk should result in chunk status 1 (compressed)
SET timescaledb.enable_direct_compress_copy = true;
SET timescaledb.enable_direct_compress_copy_client_sorted = true;
COPY metrics_status FROM STDIN;
2025-01-01
\.
SELECT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_status') chunk;
ROLLBACK;

SET timescaledb.enable_direct_compress_copy = false;
SET timescaledb.enable_direct_compress_copy_client_sorted = false;
INSERT INTO metrics_status SELECT '2025-01-01';
SELECT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_status') chunk;
SELECT compress_chunk(show_chunks('metrics_status'));
SELECT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_status') chunk;

BEGIN;
-- compressed copy into fully compressed chunk should result in chunk status 3 (compressed,unordered)
SET timescaledb.enable_direct_compress_copy = true;
SET timescaledb.enable_direct_compress_copy_client_sorted = false;
COPY metrics_status FROM STDIN;
2025-01-01
\.
SELECT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_status') chunk;
ROLLBACK;

BEGIN;
-- compressed copy new chunk should result in chunk status 1 (compressed)
SET timescaledb.enable_direct_compress_copy = true;
SET timescaledb.enable_direct_compress_copy_client_sorted = true;
COPY metrics_status FROM STDIN;
2025-01-01
\.
SELECT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_status') chunk;
ROLLBACK;

SELECT tableoid::regclass AS "CHUNK" FROM metrics_status WHERE time = '2025-01-01' LIMIT 1 \gset

-- repeat tests with direct reference to chunk
BEGIN;
-- compressed copy into fully compressed chunk should result in chunk status 3 (compressed,unordered)
SET timescaledb.enable_direct_compress_copy = true;
SET timescaledb.enable_direct_compress_copy_client_sorted = false;
COPY :CHUNK FROM STDIN;
2025-01-01
\.
SELECT _timescaledb_functions.chunk_status_text(:'CHUNK'::regclass);
ROLLBACK;

BEGIN;
-- compressed copy new chunk should result in chunk status 1 (compressed)
SET timescaledb.enable_direct_compress_copy = true;
SET timescaledb.enable_direct_compress_copy_client_sorted = true;
COPY :CHUNK FROM STDIN;
2025-01-01
\.
SELECT _timescaledb_functions.chunk_status_text(:'CHUNK'::regclass);
ROLLBACK;

