-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- to debug wal output you can use pg_walinspect like so:
/*
\c :DBNAME :ROLE_SUPERUSER
CREATE EXTENSION pg_walinspect;
SELECT pg_current_wal_insert_lsn() AS lsn1 \gset
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, SUMMARY OFF, TIMING OFF, WAL ON)
INSERT INTO metrics SELECT '2025-01-01', 'd1', i::float FROM generate_series(0,10) i;

SELECT resource_manager, record_type, record_length
FROM pg_get_wal_records_info(:'lsn1', pg_current_wal_insert_lsn())
ORDER BY start_lsn;
*/

-- test WAL output of direct compressed insert
SET timescaledb.enable_direct_compress_insert = true;
SET timescaledb.enable_direct_compress_insert_client_sorted = true;
CREATE TABLE metrics (time timestamptz, device text, value float) WITH (tsdb.hypertable);
-- initial insert to create all the metadata and chunks
INSERT INTO metrics SELECT '2025-01-01', 'd1', i::float FROM generate_series(0,10) i;
-- repeat run with WAL output
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, SUMMARY OFF, TIMING OFF, WAL ON)
INSERT INTO metrics SELECT '2025-01-01', 'd1', i::float FROM generate_series(0,10) i;

EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, SUMMARY OFF, TIMING OFF, WAL ON)
INSERT INTO metrics SELECT '2025-01-01', 'd1', i::float FROM generate_series(0,1000) i;

RESET timescaledb.enable_direct_compress_insert;
RESET timescaledb.enable_direct_compress_insert_client_sorted;

-- repeat without direct compress
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, SUMMARY OFF, TIMING OFF, WAL ON)
INSERT INTO metrics SELECT '2025-01-01', 'd1', i::float FROM generate_series(0,10) i;

EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, SUMMARY OFF, TIMING OFF, WAL ON)
INSERT INTO metrics SELECT '2025-01-01', 'd1', i::float FROM generate_series(0,1000) i;

