-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE INDEX ON :TABLE(dev);
CREATE INDEX ON :TABLE(time);

-- IndexPath without pathkeys doesnt use SkipScan
EXPLAIN (costs off, timing off, summary off) SELECT count(DISTINCT 1) FROM pg_rewrite;
:PREFIX SELECT count(DISTINCT dev) FROM :TABLE where dev=1;

-- SkipScan with ordered append
:PREFIX SELECT count(DISTINCT time), time FROM :TABLE GROUP BY time ORDER BY time;

--baseline query with skipscan
:PREFIX SELECT count(DISTINCT dev) FROM :TABLE;

-- compression doesnt prevent skipscan
SELECT compress_chunk('_timescaledb_internal._hyper_1_1_chunk');
:PREFIX SELECT count(DISTINCT dev) FROM :TABLE;
SELECT decompress_chunk('_timescaledb_internal._hyper_1_1_chunk');

--baseline query with skipscan
:PREFIX SELECT count(DISTINCT dev) FROM :TABLE;

-- partial indexes don't prevent skipscan
DROP INDEX _timescaledb_internal._hyper_1_1_chunk_skip_scan_ht_dev_idx;
DROP INDEX _timescaledb_internal._hyper_1_1_chunk_skip_scan_ht_dev_idx1;
:PREFIX SELECT count(DISTINCT dev) FROM :TABLE;

DROP INDEX _timescaledb_internal._hyper_1_1_chunk_skip_scan_ht_dev_time_idx;
:PREFIX SELECT count(DISTINCT dev) FROM :TABLE;
