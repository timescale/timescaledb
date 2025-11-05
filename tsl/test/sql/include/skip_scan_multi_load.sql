-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Create tables with 4 not-null index columns (ASC, DESC, ASC, ASC) to test various multikey SkipScan scenarios
-- Not-null scenarios are tested separately in skip_scan_notnull.sql, here we are testing multi-keys specifically

CREATE TABLE mskip_scan(time int not null, status int not null, region text not null, dev int not null, dev_name text not null, val int);

-- Status: (1,2), region: (reg1..reg4), dev: (1..5), dev_name same as dev, 4000 timestamps divided into 4 chunks tied to status, 80K rows total
-- We want to test both (1 distinct, N distinct) and (N distinct, 1 distinct) scenarios,
-- (status,*) where time<100 will do for the former, (dev, dev_name) will do for the latter

INSERT INTO mskip_scan SELECT t, 1, 'reg_' || r::text, d, 'device_' || d::text, t*d FROM generate_series(0, 999) t, generate_series(1, 4) r, generate_series(1, 5) d;
INSERT INTO mskip_scan SELECT t, 1, 'reg_' || r::text, d, 'device_' || d::text, t*d FROM generate_series(1000, 1999) t, generate_series(1, 4) r, generate_series(1, 5) d;
INSERT INTO mskip_scan SELECT t, 1, 'reg_' || r::text, d, 'device_' || d::text, t*d FROM generate_series(2000, 2999) t, generate_series(1, 4) r, generate_series(1, 5) d;
INSERT INTO mskip_scan SELECT t, 1, 'reg_' || r::text, d, 'device_' || d::text, t*d FROM generate_series(3000, 3999) t, generate_series(1, 4) r, generate_series(1, 5) d;

CREATE INDEX on mskip_scan(status, region, dev, dev_name);
-- To test reverse column orders
CREATE INDEX on mskip_scan(status, region DESC, dev);

ANALYZE mskip_scan;

-- Same with a hypertable
CREATE TABLE mskip_scan_ht(time int not null, status int not null, region text not null, dev int not null, dev_name text not null, val int);
SELECT create_hypertable('mskip_scan_ht', 'time', chunk_time_interval => 1000, create_default_indexes => false);

INSERT INTO mskip_scan_ht SELECT t, 1, 'reg_' || r::text, d, 'device_' || d::text, t*d FROM generate_series(0, 999) t, generate_series(1, 4) r, generate_series(1, 5) d;
INSERT INTO mskip_scan_ht SELECT t, 1, 'reg_' || r::text, d, 'device_' || d::text, t*d FROM generate_series(1000, 1999) t, generate_series(1, 4) r, generate_series(1, 5) d;
INSERT INTO mskip_scan_ht SELECT t, 1, 'reg_' || r::text, d, 'device_' || d::text, t*d FROM generate_series(2000, 2999) t, generate_series(1, 4) r, generate_series(1, 5) d;
INSERT INTO mskip_scan_ht SELECT t, 1, 'reg_' || r::text, d, 'device_' || d::text, t*d FROM generate_series(3000, 3999) t, generate_series(1, 4) r, generate_series(1, 5) d;

CREATE INDEX on mskip_scan_ht(status, region, dev, dev_name);
-- To test reverse column orders
CREATE INDEX on mskip_scan_ht(status, region DESC, dev);

ANALYZE mskip_scan_ht;

-- Same with columnar
CREATE TABLE mskip_scan_htc(time int not null, status int not null, region text not null, dev int not null, dev_name text not null, val int);
SELECT create_hypertable('mskip_scan_htc', 'time', chunk_time_interval => 1000, create_default_indexes => false);

-- Cannot have reverse orders on segmented columns in columnar index, so it won't be tested for columnar table
ALTER TABLE mskip_scan_htc SET (timescaledb.compress, timescaledb.compress_orderby='time desc', timescaledb.compress_segmentby='status,region,dev,dev_name');

INSERT INTO mskip_scan_htc SELECT t, 1, 'reg_' || r::text, d, 'device_' || d::text, t*d FROM generate_series(0, 999) t, generate_series(1, 4) r, generate_series(1, 5) d;
INSERT INTO mskip_scan_htc SELECT t, 1, 'reg_' || r::text, d, 'device_' || d::text, t*d FROM generate_series(1000, 1999) t, generate_series(1, 4) r, generate_series(1, 5) d;
INSERT INTO mskip_scan_htc SELECT t, 1, 'reg_' || r::text, d, 'device_' || d::text, t*d FROM generate_series(2000, 2999) t, generate_series(1, 4) r, generate_series(1, 5) d;
INSERT INTO mskip_scan_htc SELECT t, 1, 'reg_' || r::text, d, 'device_' || d::text, t*d FROM generate_series(3000, 3999) t, generate_series(1, 4) r, generate_series(1, 5) d;

SELECT compress_chunk(ch) FROM show_chunks('mskip_scan_htc') ch;

ANALYZE mskip_scan_htc;

alter table mskip_scan set (autovacuum_enabled = off);
alter table mskip_scan_ht set (autovacuum_enabled = off);
alter table mskip_scan_htc set (autovacuum_enabled = off);
