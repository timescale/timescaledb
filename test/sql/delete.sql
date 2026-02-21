-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\o /dev/null
\ir include/insert_two_partitions.sql
\o

SELECT * FROM "two_Partitions" ORDER BY "timeCustom", device_id, series_0, series_1;

DELETE FROM "two_Partitions" WHERE series_0 = 1.5;
DELETE FROM "two_Partitions" WHERE series_0 = 100;
SELECT * FROM "two_Partitions" ORDER BY "timeCustom", device_id, series_0, series_1;

-- Make sure DELETE isn't optimized if it includes Append plans
-- Need to turn of nestloop to make append appear the same on PG96 and PG10
set enable_nestloop = 'off';

CREATE OR REPLACE FUNCTION series_val()
RETURNS integer LANGUAGE PLPGSQL STABLE AS
$BODY$
BEGIN
    RETURN 5;
END;
$BODY$;

-- ConstraintAwareAppend applied for SELECT
EXPLAIN (buffers off, costs off)
SELECT FROM "two_Partitions"
WHERE series_1 IN (SELECT series_1 FROM "two_Partitions" WHERE series_1 > series_val());

-- ConstraintAwareAppend NOT applied for DELETE
EXPLAIN (buffers off, costs off)
DELETE FROM "two_Partitions"
WHERE series_1 IN (SELECT series_1 FROM "two_Partitions" WHERE series_1 > series_val());


SELECT * FROM "two_Partitions" ORDER BY "timeCustom", device_id, series_0, series_1;
BEGIN;
DELETE FROM "two_Partitions"
WHERE series_1 IN (SELECT series_1 FROM "two_Partitions" WHERE series_1 > series_val());
SELECT * FROM "two_Partitions" ORDER BY "timeCustom", device_id, series_0, series_1;
ROLLBACK;

BEGIN;
DELETE FROM "two_Partitions"
WHERE series_1 IN (SELECT series_1 FROM "two_Partitions" WHERE series_1 > series_val()) RETURNING "timeCustom";
SELECT * FROM "two_Partitions" ORDER BY "timeCustom", device_id, series_0, series_1;
ROLLBACK;

-- test update on chunks directly
CREATE TABLE direct_delete(time timestamptz) WITH (tsdb.hypertable);

INSERT INTO direct_delete VALUES ('2020-01-01');
SELECT show_chunks('direct_delete') AS "CHUNK" \gset

--should have ModifyHyperable node
EXPLAIN (costs off, timing off, summary off) DELETE FROM :CHUNK;
EXPLAIN (costs off, timing off, summary off) DELETE FROM ONLY :CHUNK;

-- DELETE should succeed
BEGIN;
DELETE FROM :CHUNK RETURNING *;
ROLLBACK;

BEGIN;
DELETE FROM ONLY :CHUNK RETURNING *;
ROLLBACK;

-- Test that EXPLAIN VERBOSE on prepared statements does not corrupt cached plans.
SET plan_cache_mode = 'force_generic_plan';
CREATE TABLE explain_verbose_ht( time timestamptz NOT NULL, device int, value float) WITH (tsdb.hypertable);

INSERT INTO explain_verbose_ht SELECT t, 1, 0.1 FROM generate_series('2026-01-01'::timestamptz, '2026-01-08'::timestamptz, interval '6 hours') t;

-- Verify the DELETE plan uses ChunkAppend
EXPLAIN (costs off) DELETE FROM explain_verbose_ht WHERE time > '2025-01-01'::text::timestamptz;

PREPARE delete_ht AS DELETE FROM explain_verbose_ht WHERE time > '2025-01-01'::text::timestamptz AND device = 2;

EXECUTE delete_ht;
EXPLAIN (verbose, costs off) EXECUTE delete_ht;
EXECUTE delete_ht;

DEALLOCATE delete_ht;

-- repeat test with explain analyze
PREPARE delete_ht AS DELETE FROM explain_verbose_ht WHERE time > '2025-01-01'::text::timestamptz AND device = 2;

EXECUTE delete_ht;
EXPLAIN (verbose, analyze, buffers off, costs off, timing off, summary off) EXECUTE delete_ht;
EXECUTE delete_ht;

DEALLOCATE delete_ht;

RESET plan_cache_mode;

-- github issue #6790
-- test DELETE with WHERE EXISTS on hypertable
CREATE TABLE i6790(time timestamptz NOT NULL, device int, value float) WITH (tsdb.hypertable);
INSERT INTO i6790 SELECT t, 1, 0.1 FROM generate_series('2026-01-01'::timestamptz, '2026-01-03'::timestamptz, interval '12 hours') t;

-- DELETE with simple EXISTS - creates gating Result node wrapping ChunkAppend
DELETE FROM i6790 WHERE EXISTS (SELECT 1);
-- all rows should be gone
SELECT count(*) FROM i6790;

-- repopulate for next test
INSERT INTO i6790 SELECT t, 1, 0.1 FROM generate_series('2026-01-01'::timestamptz, '2026-01-03'::timestamptz, interval '12 hours') t;

-- DELETE with correlated EXISTS
DELETE FROM i6790 WHERE EXISTS (SELECT 1 FROM i6790 g WHERE g.device = i6790.device);
SELECT count(*) FROM i6790;

