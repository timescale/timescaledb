-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test 2-key index "(dev,dev_name)" where index qual may not be on a skip col
-- Make sure it's the only index to be used for "dev" skipscan key

-- skip_scan table
CREATE INDEX skip_scan_dev_devname_idx ON skip_scan(dev,dev_name);

-- skip_scan_ht table
CREATE INDEX skip_scan_ht_dev_devname_idx ON skip_scan_ht(dev,dev_name);

-- skip_scan_htc table
ALTER TABLE skip_scan_htc SET (timescaledb.compress, timescaledb.compress_orderby='time desc', timescaledb.compress_segmentby='dev,dev_name');
SELECT compress_chunk(ch) FROM show_chunks('skip_scan_htc') ch;

-- table with a bool column
CREATE TABLE skip_scan_b as select dev, coalesce(time,100) time, nullif(dev%3,2)::boolean as b from skip_scan;
CREATE INDEX skip_scan_b_idx ON skip_scan_b(b);

--table with not null constraint
CREATE TABLE skip_scan_nn as select coalesce(dev,0) dev, coalesce(time,100) time from skip_scan;
ALTER TABLE skip_scan_nn ALTER COLUMN dev SET NOT NULL;
CREATE INDEX skip_scan_nn_idx ON skip_scan_nn(dev);
