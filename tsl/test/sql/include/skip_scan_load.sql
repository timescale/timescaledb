-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE TABLE skip_scan(time int, dev int, dev_name text, val int);

INSERT INTO skip_scan SELECT t, d, 'device_' || d::text, t*d FROM generate_series(1, 1000) t, generate_series(1, 10) d;
INSERT INTO skip_scan VALUES (NULL, 0, -1, NULL), (0, NULL, -1, NULL);
INSERT INTO skip_scan(time,dev,dev_name,val) SELECT t, NULL, NULL, NULL FROM generate_series(0, 999, 50) t;

ANALYZE skip_scan;

CREATE TABLE skip_scan_nulls(time int);
CREATE INDEX ON skip_scan_nulls(time);
INSERT INTO skip_scan_nulls SELECT NULL FROM generate_series(1,100);

ANALYZE skip_scan_nulls;

-- create hypertable with different physical layouts in the chunks
CREATE TABLE skip_scan_ht(f1 int, f2 int, f3 int, time int NOT NULL, dev int, dev_name text, val int);
SELECT create_hypertable('skip_scan_ht', 'time', chunk_time_interval => 250, create_default_indexes => false);

INSERT INTO skip_scan_ht(time,dev,dev_name,val) SELECT t, d, 'device_' || d::text, random() FROM generate_series(0, 249) t, generate_series(1, 10) d;
ALTER TABLE skip_scan_ht DROP COLUMN f1;
INSERT INTO skip_scan_ht(time,dev,dev_name,val) SELECT t, d, 'device_' || d::text, random() FROM generate_series(250, 499) t, generate_series(1, 10) d;
ALTER TABLE skip_scan_ht DROP COLUMN f2;
INSERT INTO skip_scan_ht(time,dev,dev_name,val) SELECT t, d, 'device_' || d::text, random() FROM generate_series(500, 749) t, generate_series(1, 10) d;
ALTER TABLE skip_scan_ht DROP COLUMN f3;
INSERT INTO skip_scan_ht(time,dev,dev_name,val) SELECT t, d, 'device_' || d::text, random() FROM generate_series(750, 999) t, generate_series(1, 10) d;

INSERT INTO skip_scan_ht(time,dev,dev_name,val) SELECT t, NULL, NULL, NULL FROM generate_series(0, 999, 50) t;

ANALYZE skip_scan_ht;

ALTER TABLE skip_scan_ht SET (timescaledb.compress,timescaledb.compress_orderby='time desc', timescaledb.compress_segmentby='dev');

CREATE TABLE skip_scan_insert(time int, dev int, dev_name text, val int, query text);

CREATE OR REPLACE FUNCTION int_func_immutable() RETURNS int LANGUAGE SQL IMMUTABLE SECURITY DEFINER AS $$SELECT 1; $$;
CREATE OR REPLACE FUNCTION int_func_stable() RETURNS int LANGUAGE SQL STABLE SECURITY DEFINER AS $$ SELECT 2; $$;
CREATE OR REPLACE FUNCTION int_func_volatile() RETURNS int LANGUAGE SQL VOLATILE SECURITY DEFINER AS $$ SELECT 3; $$;

CREATE OR REPLACE FUNCTION inta_func_immutable() RETURNS int[] LANGUAGE SQL IMMUTABLE SECURITY DEFINER AS $$ SELECT ARRAY[1,2,3]; $$;
CREATE OR REPLACE FUNCTION inta_func_stable() RETURNS int[] LANGUAGE SQL STABLE SECURITY DEFINER AS $$ SELECT ARRAY[2,3,4]; $$;
CREATE OR REPLACE FUNCTION inta_func_volatile() RETURNS int[] LANGUAGE SQL VOLATILE SECURITY DEFINER AS $$ SELECT ARRAY[3,4,5]; $$;

-- adjust statistics so we get skipscan plans
UPDATE pg_statistic SET stadistinct=1, stanullfrac=0.5 WHERE starelid='skip_scan'::regclass;
UPDATE pg_statistic SET stadistinct=1, stanullfrac=0.5 WHERE starelid='skip_scan_nulls'::regclass;
UPDATE pg_statistic SET stadistinct=1, stanullfrac=0.5 WHERE starelid='skip_scan_ht'::regclass;
UPDATE pg_statistic SET stadistinct=1, stanullfrac=0.5 WHERE starelid IN (select inhrelid from pg_inherits where inhparent='skip_scan_ht'::regclass);

-- Turn off autovacuum to not trigger new vacuums that restores the
-- adjusted statistics
alter table skip_scan set (autovacuum_enabled = off);
alter table skip_scan_nulls set (autovacuum_enabled = off);
alter table skip_scan_ht set (autovacuum_enabled = off);
