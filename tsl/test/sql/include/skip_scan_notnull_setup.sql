-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test 2-key index "(dev,dev_name)" where index qual may not be on a skip col
-- Make sure it's the only index to be used for "dev" skipscan key

-- skip_scan table
DROP INDEX IF EXISTS skip_scan_dev_idx;
DROP INDEX IF EXISTS skip_scan_dev_time_idx;
CREATE INDEX skip_scan_dev_devname_idx ON skip_scan(dev,dev_name);

-- skip_scan_ht table
DROP INDEX IF EXISTS skip_scan_ht_dev_idx;
DROP INDEX IF EXISTS skip_scan_ht_dev_idx1;
DROP INDEX IF EXISTS skip_scan_ht_dev_time_idx;
CREATE INDEX skip_scan_ht_dev_devname_idx ON skip_scan_ht(dev,dev_name);

-- skip_scan_htc table
ALTER TABLE skip_scan_htc SET (timescaledb.compress, timescaledb.compress_orderby='time desc', timescaledb.compress_segmentby='dev,dev_name');
SELECT compress_chunk(ch) FROM show_chunks('skip_scan_htc') ch;
