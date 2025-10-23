-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- need superuser to modify statistics
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER
\ir include/skip_scan_load.sql

-- we want to run with analyze here so we can see counts in the nodes
\set PREFIX 'EXPLAIN (analyze, buffers off, costs off, timing off, summary off)'

-- test SkipScan NOT NULL mode
-- also sets up "skip_scan_b" table with a bool column and a not null column
\ir include/skip_scan_notnull_setup.sql

SET timescaledb.debug_skip_scan_info  TO true;
\set TABLE skip_scan
\ir include/skip_scan_notnull.sql

-- Set up hypertable with bool column
SELECT create_hypertable('skip_scan_b', 'time', chunk_time_interval => 500, create_default_indexes => false, migrate_data => true);
-- Set up hypertable with not null column
SELECT create_hypertable('skip_scan_nn', 'time', chunk_time_interval => 500, create_default_indexes => false, migrate_data => true);
\set TABLE skip_scan_ht
\ir include/skip_scan_notnull.sql

-- Compress on bool column
ALTER TABLE skip_scan_b SET (timescaledb.compress, timescaledb.compress_orderby='time desc', timescaledb.compress_segmentby='b');
SELECT compress_chunk(ch) FROM show_chunks('skip_scan_b') ch;
-- Compress on not null column
ALTER TABLE skip_scan_nn SET (timescaledb.compress, timescaledb.compress_orderby='time desc', timescaledb.compress_segmentby='dev');
SELECT compress_chunk(ch) FROM show_chunks('skip_scan_nn') ch;
\set TABLE skip_scan_htc
\ir include/skip_scan_notnull.sql

RESET timescaledb.debug_skip_scan_info;
