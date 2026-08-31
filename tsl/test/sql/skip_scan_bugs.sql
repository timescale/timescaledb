-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set PREFIX 'EXPLAIN (buffers off, costs off, timing off, summary off)'

-- Fix for issue #10421: do not drop MergeAppend subpaths with pathkeys not matching distinct pathkeys
-- when the compressed SkipScan plan is chosen.

-- This table will have uncompressed index on (sale_day) and compressed index on(style, sale_day)
CREATE TABLE t_10421 (
    style    text,
    sale_day date NOT NULL
) WITH (tsdb.hypertable, tsdb.partition_column = 'sale_day',
        tsdb.chunk_interval = '360 days', tsdb.segmentby = 'style');

INSERT INTO t_10421
SELECT 'S' || s, d
FROM generate_series(1, 20) s,
     generate_series('2026-01-02'::date, '2026-01-31'::date, '1 day') d;

SELECT count(compress_chunk(c)) FROM show_chunks('t_10421') c;

-- newer rows land in the same chunk, which is now partially compressed
INSERT INTO t_10421
SELECT 'S' || s, d
FROM generate_series(1, 20) s,
     generate_series('2026-02-01'::date, '2026-02-02'::date, '1 day') d;

-- steer the planner to the index-based plan shape
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET max_parallel_workers_per_gather = 0;

-- Make sure SkipScan is used on compressed part
-- and IndexScan with sort keys different from distinct keys but matching the predicate
-- is used for uncompressed part
:PREFIX
SELECT count(DISTINCT style) FROM t_10421
WHERE sale_day > '2026-01-31';

-- all 20 styles have sales after Jan 31, in the uncompressed part only
SET timescaledb.enable_compressed_skipscan = off;
SELECT count(DISTINCT style) FROM t_10421
WHERE sale_day > '2026-01-31';  -- returns 20, correct

SET timescaledb.enable_compressed_skipscan = on;  -- default setting
SELECT count(DISTINCT style) FROM t_10421
WHERE sale_day > '2026-01-31';  -- returns 20, correct

drop table t_10421 cascade;

-- Fix for issue #10429: don't use SkipScan over unmatched IndexScan with sort on top.

-- Set up table with multikeys indexes on compressed and uncompressed data
CREATE TABLE t_10429(
	run_datetime              timestamp without time zone NOT NULL,
	runno                     integer                     NOT NULL,
	interval_datetime         timestamp without time zone NOT NULL,
	constraintid              text                        NOT NULL,
	versionno                 integer                     NOT NULL,
	bidtype                   text                        NOT NULL,
	relevant_regions          text                        NOT NULL
);
CREATE INDEX fpp_fcas_summary_run_datetime_idx ON t_10429
	USING btree (run_datetime, runno, interval_datetime, constraintid, versionno);
SELECT table_name FROM public.create_hypertable(
	relation => 't_10429',
	time_column_name => 'run_datetime',
	chunk_time_interval => interval '6 months',
	create_default_indexes => false
);
ALTER TABLE t_10429 SET (
	timescaledb.compress,
	timescaledb.compress_segmentby = 'runno, constraintid, versionno',
	timescaledb.compress_orderby='interval_datetime DESC, run_datetime DESC'
);

-- Fill in and compress some data
WITH base AS (
    SELECT
        gs AS run_datetime,
        gs - interval '5 minutes' AS interval_datetime,
        c.constraintid,
        b.bidtype,
        r.region AS relevant_regions,
        row_number() OVER (
            PARTITION BY gs, c.constraintid
            ORDER BY b.bidtype, r.region
        )::integer AS runno
    FROM generate_series(
        '2026-01-01 00:00:00'::timestamp,
        '2026-08-04 23:55:00'::timestamp,
        interval '1 day'
    ) AS gs
    CROSS JOIN (
        VALUES  ('CONSTRAINT_001'), ('CONSTRAINT_002'), ('CONSTRAINT_003')
    ) AS c(constraintid)
    CROSS JOIN (
        VALUES  ('RAISE6SEC'), ('RAISE60SEC')
    ) AS b(bidtype)
    CROSS JOIN (
        VALUES  ('NSW1'), ('QLD1')
    ) AS r(region)
)
INSERT INTO t_10429 (
    run_datetime,
    runno,
    interval_datetime,
    constraintid,
    versionno,
    bidtype,
    relevant_regions
)
SELECT
    run_datetime,
    runno,
    interval_datetime,
    constraintid,
    1 AS versionno,
    bidtype,
    relevant_regions
FROM base;

SELECT count(compress_chunk(ch)) FROM show_chunks('t_10429') ch;

-- Add uncompressed data so that we have partial chunk with different sort order
-- on compressed vs. uncompressed data
WITH base AS (
    SELECT
        gs AS run_datetime,
        gs - interval '5 minutes' AS interval_datetime,
        c.constraintid,
        b.bidtype,
        r.region AS relevant_regions,
        row_number() OVER (
            PARTITION BY gs, c.constraintid
            ORDER BY b.bidtype, r.region
        )::integer AS runno
    FROM generate_series(
        '2026-01-01 00:00:00'::timestamp,
        '2026-08-04 23:55:00'::timestamp,
        interval '1 day'
    ) AS gs
    CROSS JOIN (
        VALUES  ('CONSTRAINT_001'), ('CONSTRAINT_002'), ('CONSTRAINT_003')
    ) AS c(constraintid)
    CROSS JOIN (
        VALUES  ('RAISE6SEC'), ('RAISE60SEC')
    ) AS b(bidtype)
    CROSS JOIN (
        VALUES  ('NSW1'), ('QLD1')
    ) AS r(region)
)
INSERT INTO t_10429 (
    run_datetime,
    runno,
    interval_datetime,
    constraintid,
    versionno,
    bidtype,
    relevant_regions
)
SELECT
    run_datetime,
    runno,
    interval_datetime,
    constraintid,
    1 AS versionno,
    bidtype,
    relevant_regions
FROM base;

ANALYZE t_10429;

SET datestyle = 'iso, mdy';
SET timezone = 'UTC';

-- In this query we should not use SkipScan over uncompressed IndexScan
-- as IndexScan pathkey on (interval_datetime) is not matching distinct pathkeys for the query.
-- It should return result without error.
SET timescaledb.debug_skip_scan_info  TO true;
SELECT DISTINCT
ON (interval_datetime, run_datetime, runno, constraintid) *
FROM
t_10429
WHERE
interval_datetime > '2026-08-01'
AND interval_datetime <= '2026-08-02'
ORDER BY
interval_datetime,
run_datetime DESC,
runno DESC,
constraintid,
versionno DESC;
RESET timescaledb.debug_skip_scan_info;

drop table t_10429 cascade;

RESET datestyle;
RESET timezone;

-- Fix #10409: skip on the correct index column in case of equivalent distinct columns
CREATE TABLE zone_reports (reported_zone int4, assigned_zone int4, sensor_group int4 NOT NULL);
CREATE INDEX ON zone_reports (assigned_zone, sensor_group, reported_zone);
INSERT INTO zone_reports SELECT v % 4, v % 4, v % 40 FROM generate_series(1, 200000) AS v;
ANALYZE zone_reports;

-- Should use SkipScan on (assigned_zone, sensor_group) and return correct result
SET timescaledb.debug_skip_scan_info  TO true;
SELECT DISTINCT ON (reported_zone, sensor_group) * FROM zone_reports WHERE reported_zone = assigned_zone;
RESET timescaledb.debug_skip_scan_info;

-- Cannot use SkipScan as  "assigned_zone" is not in indexscan output
SET timescaledb.debug_skip_scan_info  TO true;
SELECT DISTINCT ON (reported_zone, sensor_group) reported_zone, sensor_group FROM zone_reports WHERE reported_zone = assigned_zone AND assigned_zone < 4;
RESET timescaledb.debug_skip_scan_info;

-- Cannot use SkipScan as "assigned_zone" is not in indexscan output
SET timescaledb.debug_skip_scan_info  TO true;
SELECT count(DISTINCT reported_zone) FROM zone_reports WHERE reported_zone = assigned_zone;
RESET timescaledb.debug_skip_scan_info;

-- Can use SkipScan as "assigned_zone" is produced by indexscan
SET timescaledb.debug_skip_scan_info  TO true;
SELECT DISTINCT ON (reported_zone, sensor_group) assigned_zone + 1, sensor_group FROM zone_reports WHERE reported_zone = assigned_zone AND assigned_zone < 4;
RESET timescaledb.debug_skip_scan_info;

-- Test coverage for distinct PathKeys usage
-- No SkipScan for volatile keys
:PREFIX SELECT DISTINCT random() FROM zone_reports;
-- No SkipScan over Filter aggregates
:PREFIX SELECT count(DISTINCT assigned_zone) FILTER (WHERE sensor_group >5) AS result FROM zone_reports;

-- Tests for hypertable and compressed data
SELECT table_name FROM create_hypertable('zone_reports', 'sensor_group', chunk_time_interval => 20, create_default_indexes => false, migrate_data => true);

-- Can use SkipScan
SET timescaledb.debug_skip_scan_info  TO true;
SELECT DISTINCT ON (reported_zone) * FROM zone_reports WHERE reported_zone = assigned_zone;
RESET timescaledb.debug_skip_scan_info;

ALTER TABLE zone_reports SET (timescaledb.compress, timescaledb.compress_orderby='sensor_group', timescaledb.compress_segmentby='assigned_zone,reported_zone');
SELECT count(compress_chunk(ch)) FROM show_chunks('zone_reports') ch;

-- Can use SkipScan as both equivalent columns are segmentby
SET timescaledb.debug_skip_scan_info  TO true;
SELECT DISTINCT ON (reported_zone) * FROM zone_reports WHERE reported_zone = assigned_zone;
RESET timescaledb.debug_skip_scan_info;

-- Can use SkipScan as the equivalent column with highest sort order is segmentby
SET timescaledb.debug_skip_scan_info  TO true;
SELECT DISTINCT ON (assigned_zone) * FROM zone_reports WHERE sensor_group = assigned_zone;
RESET timescaledb.debug_skip_scan_info;

drop table zone_reports;

RESET enable_seqscan;
RESET enable_bitmapscan;
RESET max_parallel_workers_per_gather;
