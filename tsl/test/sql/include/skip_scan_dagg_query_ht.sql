-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE INDEX ON :TABLE(dev);
CREATE INDEX ON :TABLE(time);

-- To test scenarios with SkipScan/no SkipScan over a mix of uncompressed and compressed chunks
SET timescaledb.enable_compressed_skipscan TO false;

-- IndexPath without pathkeys doesnt use SkipScan
EXPLAIN (costs off, timing off, summary off) SELECT count(DISTINCT 1) FROM pg_rewrite;
:PREFIX SELECT count(DISTINCT dev) FROM :TABLE where dev=1;

-- SkipScan with ordered append
:PREFIX SELECT count(DISTINCT time), time FROM :TABLE GROUP BY time ORDER BY time;

--baseline query with skipscan
:PREFIX SELECT count(DISTINCT dev) FROM :TABLE;

-- compression on one chunk doesnt prevent skipscan on other chunks
SELECT compress_chunk('_timescaledb_internal._hyper_1_1_chunk');
:PREFIX SELECT count(DISTINCT dev) FROM :TABLE;

-- compression on one chunk will prevent skipscan if it's the only chunk remaining
:PREFIX SELECT count(DISTINCT dev) FROM :TABLE WHERE time = 100;
SELECT decompress_chunk('_timescaledb_internal._hyper_1_1_chunk');

-- skipscan will be applied on decompressed chunk if it's the only chunk remaining
:PREFIX SELECT count(DISTINCT dev) FROM :TABLE WHERE time = 100;

--baseline query with skipscan
:PREFIX SELECT count(DISTINCT dev) FROM :TABLE;

-- partial indexes don't prevent skipscan
DROP INDEX _timescaledb_internal._hyper_1_1_chunk_skip_scan_ht_dev_idx;
DROP INDEX _timescaledb_internal._hyper_1_1_chunk_skip_scan_ht_dev_idx1;
:PREFIX SELECT count(DISTINCT dev) FROM :TABLE;

DROP INDEX _timescaledb_internal._hyper_1_1_chunk_skip_scan_ht_dev_time_idx;
:PREFIX SELECT count(DISTINCT dev) FROM :TABLE;

RESET timescaledb.enable_compressed_skipscan;
