-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE INDEX ON :TABLE(dev);
CREATE INDEX ON :TABLE(time);

-- To test scenarios with SkipScan/no SkipScan over a mix of uncompressed and compressed chunks
SET timescaledb.enable_compressed_skipscan TO false;

-- SkipScan with ordered append
:PREFIX SELECT DISTINCT ON (time) time FROM :TABLE ORDER BY time;

--baseline query with skipscan
:PREFIX SELECT DISTINCT ON (dev) dev, dev_name FROM :TABLE;

-- compression doesnt prevent skipscan
SELECT compress_chunk('_timescaledb_internal._hyper_1_1_chunk');
:PREFIX SELECT DISTINCT ON (dev) dev, dev_name FROM :TABLE;
SELECT decompress_chunk('_timescaledb_internal._hyper_1_1_chunk');

--baseline query with skipscan
:PREFIX SELECT DISTINCT ON (dev) dev, dev_name FROM :TABLE;

-- partial indexes don't prevent skipscan
DROP INDEX _timescaledb_internal._hyper_1_1_chunk_skip_scan_ht_dev_idx;
:PREFIX SELECT DISTINCT ON (dev) dev, dev_name FROM :TABLE;

-- IndexPath without pathkeys doesnt use SkipScan
EXPLAIN (buffers off, costs off, timing off, summary off) SELECT DISTINCT 1 FROM pg_rewrite;

RESET timescaledb.enable_compressed_skipscan;
