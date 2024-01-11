-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set EXPLAIN 'EXPLAIN (costs off,timing off,summary off)'
\set EXPLAIN_ANALYZE 'EXPLAIN (analyze,costs off,timing off,summary off)'

CREATE TABLE decompress_tracking(time timestamptz not null, device text, value float, primary key(time, device));
SELECT table_name FROM create_hypertable('decompress_tracking','time');
ALTER TABLE decompress_tracking SET (timescaledb.compress, timescaledb.compress_segmentby='device');

INSERT INTO decompress_tracking SELECT '2020-01-01'::timestamptz + format('%s hour', g)::interval, 'd1', random() FROM generate_series(1,10) g;
INSERT INTO decompress_tracking SELECT '2020-01-01'::timestamptz + format('%s hour', g)::interval, 'd2', random() FROM generate_series(1,20) g;
INSERT INTO decompress_tracking SELECT '2020-01-01'::timestamptz + format('%s hour', g)::interval, 'd3', random() FROM generate_series(1,30) g;

SELECT count(compress_chunk(ch)) FROM show_chunks('decompress_tracking') ch;

ANALYZE decompress_tracking;

-- no tracking without analyze
:EXPLAIN UPDATE decompress_tracking SET value = value + 3;

BEGIN; :EXPLAIN_ANALYZE UPDATE decompress_tracking SET value = value + 3; ROLLBACK;
BEGIN; :EXPLAIN_ANALYZE UPDATE decompress_tracking SET value = value + 3 WHERE device = 'd2'; ROLLBACK;
BEGIN; :EXPLAIN_ANALYZE DELETE FROM decompress_tracking; ROLLBACK;
BEGIN; :EXPLAIN_ANALYZE DELETE FROM decompress_tracking WHERE device = 'd3'; ROLLBACK;
BEGIN; :EXPLAIN_ANALYZE INSERT INTO decompress_tracking SELECT '2020-01-01 1:30','d1',random(); ROLLBACK;
BEGIN; :EXPLAIN_ANALYZE INSERT INTO decompress_tracking SELECT '2020-01-01','d2',random(); ROLLBACK;
BEGIN; :EXPLAIN_ANALYZE INSERT INTO decompress_tracking SELECT '2020-01-01','d4',random(); ROLLBACK;
BEGIN; :EXPLAIN_ANALYZE INSERT INTO decompress_tracking (VALUES ('2020-01-01 1:30','d1',random()),('2020-01-01 1:30','d2',random())); ROLLBACK;

-- test prepared statements EXPLAIN still works after execution
SET plan_cache_mode TO force_generic_plan;
PREPARE p1 AS UPDATE decompress_tracking SET value = value + 3 WHERE device = 'd1';
BEGIN;
    EXPLAIN (COSTS OFF) EXECUTE p1;
    EXECUTE p1;
    EXPLAIN (COSTS OFF) EXECUTE p1;
ROLLBACK;

DROP TABLE decompress_tracking;
