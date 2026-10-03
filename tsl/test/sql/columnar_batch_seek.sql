-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Equality lookups on the leading orderby column of a chunk without
-- segmentby scan the batch start index backward from the value, and stop
-- after the first batch that starts before it.
\c :TEST_DBNAME :ROLE_SUPERUSER

SET max_parallel_workers_per_gather = 0;

-- Whether the plan of a query uses batch seek.
CREATE FUNCTION plan_has_seek(q text) RETURNS bool LANGUAGE plpgsql AS $$
DECLARE
    l text;
BEGIN
    FOR l IN EXECUTE 'EXPLAIN (costs off) ' || q LOOP
        IF l LIKE '%Batch Seek%' THEN
            RETURN true;
        END IF;
    END LOOP;
    RETURN false;
END
$$;

-- Number of sensor ids in [lo, hi] where a lookup with a constant differs
-- from the reference table.
CREATE FUNCTION const_mismatches(tbl regclass, ref regclass, lo int, hi int) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    m int := 0;
    a record;
    b record;
BEGIN
    FOR x IN lo..hi LOOP
        EXECUTE format('SELECT count(*) c, coalesce(sum(val), 0) s FROM %s WHERE sensor_id = %s', tbl, x) INTO a;
        EXECUTE format('SELECT count(*) c, coalesce(sum(val), 0) s FROM %s WHERE sensor_id = %s', ref, x) INTO b;
        IF a.c <> b.c OR a.s <> b.s THEN
            m := m + 1;
        END IF;
    END LOOP;
    RETURN m;
END
$$;

-- Same check with the value coming from the outer side of a join.
CREATE FUNCTION param_mismatches(tbl regclass, ref regclass, lo int, hi int) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    m int;
BEGIN
    EXECUTE format($q$
        SELECT count(*) FILTER (WHERE (h.c, h.s) IS DISTINCT FROM (r.c, r.s))
        FROM generate_series(%s, %s) x,
        LATERAL (SELECT count(*) c, coalesce(sum(val), 0) s FROM %s WHERE sensor_id = x) h,
        LATERAL (SELECT count(*) c, coalesce(sum(val), 0) s FROM %s WHERE sensor_id = x) r
    $q$, lo, hi, tbl, ref) INTO m;
    RETURN m;
END
$$;

-- Sensor ids 1..200 skipping multiples of 5, with row counts that make some
-- sensors span several batches and others share one.
CREATE TABLE ref (sensor_id int NOT NULL, time timestamptz NOT NULL, val int);
INSERT INTO ref
SELECT s, '2026-01-01'::timestamptz + n * interval '1 second', s * 7 + n
FROM generate_series(1, 200) s, generate_series(1, (s * 37) % 2500 + 1) n
WHERE s % 5 <> 0;
CREATE INDEX ON ref (sensor_id);

CREATE TABLE ob (LIKE ref);
SELECT FROM create_hypertable('ob', 'time', chunk_time_interval => interval '1 year');
ALTER TABLE ob SET (timescaledb.compress, timescaledb.compress_segmentby = '',
    timescaledb.compress_orderby = 'sensor_id, time');
INSERT INTO ob SELECT * FROM ref;
SELECT count(compress_chunk(c)) FROM show_chunks('ob') c;
VACUUM ANALYZE ob;

SELECT show_chunks('ob') || '_compressed' AS cchunk \gset

-- Batches cross sensor boundaries.
SELECT count(*) AS batches,
    count(*) FILTER (WHERE _ts_meta_v2_first_sensor_id <> _ts_meta_v2_last_sensor_id) AS crossing
FROM :cchunk;

SET enable_seqscan = off;
EXPLAIN (costs off) SELECT count(*) FROM ob WHERE sensor_id = 100;
SELECT plan_has_seek('SELECT * FROM ob WHERE sensor_id = 100') AS const_seek,
    plan_has_seek('SELECT * FROM generate_series(1, 3) x, LATERAL (SELECT count(*) FROM ob WHERE sensor_id = x) l') AS param_seek;

-- Index scan, then filter on a seq scan of the compressed chunk.
SELECT const_mismatches('ob', 'ref', -1, 202), param_mismatches('ob', 'ref', -1, 202);
RESET enable_seqscan;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT const_mismatches('ob', 'ref', 90, 110), param_mismatches('ob', 'ref', -1, 202);
RESET enable_indexscan;
RESET enable_bitmapscan;

-- Prepared statement keeps working when X changes between executions.
SET plan_cache_mode = force_generic_plan;
PREPARE lookup(int) AS SELECT count(*), sum(val) FROM ob WHERE sensor_id = $1;
EXECUTE lookup(99);
SELECT count(*), sum(val) FROM ref WHERE sensor_id = 99;
EXECUTE lookup(101);
SELECT count(*), sum(val) FROM ref WHERE sensor_id = 101;

-- Not used when disabled.
SET timescaledb.enable_columnar_batch_seek = off;
SELECT plan_has_seek('SELECT * FROM ob WHERE sensor_id = 100');
RESET timescaledb.enable_columnar_batch_seek;

-- Not used for a value of another type (not supported yet), still correct.
SELECT plan_has_seek('SELECT * FROM ob WHERE sensor_id = 100::bigint');
SELECT count(*), sum(val) FROM ob WHERE sensor_id = 101::bigint;

-- Not used with DESC orderby, a nullable orderby column or segmentby.
CREATE TABLE ob_desc (LIKE ref);
SELECT FROM create_hypertable('ob_desc', 'time', chunk_time_interval => interval '1 year');
ALTER TABLE ob_desc SET (timescaledb.compress, timescaledb.compress_segmentby = '',
    timescaledb.compress_orderby = 'sensor_id DESC, time');
INSERT INTO ob_desc SELECT * FROM ref;
SELECT count(compress_chunk(c)) FROM show_chunks('ob_desc') c;

CREATE TABLE ob_null (sensor_id int, time timestamptz NOT NULL, val int);
SELECT FROM create_hypertable('ob_null', 'time', chunk_time_interval => interval '1 year');
ALTER TABLE ob_null SET (timescaledb.compress, timescaledb.compress_segmentby = '',
    timescaledb.compress_orderby = 'sensor_id, time');
INSERT INTO ob_null SELECT * FROM ref;
INSERT INTO ob_null VALUES (NULL, '2026-01-01', 1);
SELECT count(compress_chunk(c)) FROM show_chunks('ob_null') c;

CREATE TABLE seg (LIKE ref);
SELECT FROM create_hypertable('seg', 'time', chunk_time_interval => interval '1 year');
ALTER TABLE seg SET (timescaledb.compress, timescaledb.compress_segmentby = 'sensor_id',
    timescaledb.compress_orderby = 'time');
INSERT INTO seg SELECT * FROM ref;
SELECT count(compress_chunk(c)) FROM show_chunks('seg') c;

SELECT plan_has_seek('SELECT * FROM ob_desc WHERE sensor_id = 100') AS desc_seek,
    plan_has_seek('SELECT * FROM ob_null WHERE sensor_id = 100') AS null_seek,
    plan_has_seek('SELECT * FROM seg WHERE sensor_id = 100') AS seg_seek;
SELECT const_mismatches('ob_desc', 'ref', 98, 102), const_mismatches('ob_null', 'ref', 98, 102);

-- Ordered output is still correct when the index is scanned backward for the
-- seek.
SELECT (SELECT array_agg(time) FROM (SELECT time FROM ob WHERE sensor_id = s ORDER BY sensor_id, time) o)
    IS NOT DISTINCT FROM
    (SELECT array_agg(time ORDER BY time) FROM ref WHERE sensor_id = s) AS same_order
FROM generate_series(98, 102) s;
SELECT (SELECT array_agg(time) FROM (SELECT time FROM ob WHERE sensor_id = s ORDER BY sensor_id DESC, time DESC) o)
    IS NOT DISTINCT FROM
    (SELECT array_agg(time ORDER BY time DESC) FROM ref WHERE sensor_id = s) AS same_order_desc
FROM generate_series(98, 102) s;

-- Other conditions on the batch metadata are checked after the index scan,
-- so they don't hide the batch where the seek stops.
SELECT const_mismatches('ob', 'ref', 98, 102);
SELECT count(*), sum(val) FROM ob WHERE sensor_id = 101 AND time > '2026-01-01 00:10';
SELECT count(*), sum(val) FROM ref WHERE sensor_id = 101 AND time > '2026-01-01 00:10';

-- Partial chunk: rows inserted after compression stay in the uncompressed
-- part and the batches keep their order.
INSERT INTO ob VALUES (101, '2026-01-01 13:00', 5), (102, '2026-01-01 13:00', 5);
INSERT INTO ref VALUES (101, '2026-01-01 13:00', 5), (102, '2026-01-01 13:00', 5);
SELECT _timescaledb_functions.chunk_status_text(c) FROM show_chunks('ob') c;
SELECT plan_has_seek('SELECT * FROM ob WHERE sensor_id = 100');
SELECT const_mismatches('ob', 'ref', 98, 104), param_mismatches('ob', 'ref', 98, 104);
SELECT count(compress_chunk(c)) FROM show_chunks('ob') c;
SELECT _timescaledb_functions.chunk_status_text(c) FROM show_chunks('ob') c;
SELECT plan_has_seek('SELECT * FROM ob WHERE sensor_id = 100');
SELECT const_mismatches('ob', 'ref', 98, 104), param_mismatches('ob', 'ref', 98, 104);

-- Unordered chunk: batches written by direct compress overlap the sorted
-- ones. The generic plan made while the chunk was ordered must still be
-- correct, and new plans must not use batch seek.
EXECUTE lookup(99);
SET timescaledb.enable_direct_compress_insert = on;
INSERT INTO ob SELECT s, '2026-01-01 12:00'::timestamptz + n * interval '1 second', n
FROM generate_series(1, 200) s, generate_series(1, 3) n WHERE s % 5 <> 0;
INSERT INTO ref SELECT s, '2026-01-01 12:00'::timestamptz + n * interval '1 second', n
FROM generate_series(1, 200) s, generate_series(1, 3) n WHERE s % 5 <> 0;
RESET timescaledb.enable_direct_compress_insert;
SELECT _timescaledb_functions.chunk_status_text(c) FROM show_chunks('ob') c;
EXECUTE lookup(99);
SELECT count(*), sum(val) FROM ref WHERE sensor_id = 99;
SELECT plan_has_seek('SELECT * FROM ob WHERE sensor_id = 100');
SELECT const_mismatches('ob', 'ref', 90, 110), param_mismatches('ob', 'ref', -1, 202);

-- Recompression sorts the batches again.
SELECT count(decompress_chunk(c)) FROM show_chunks('ob') c;
SELECT count(compress_chunk(c)) FROM show_chunks('ob') c;
SELECT _timescaledb_functions.chunk_status_text(c) FROM show_chunks('ob') c;
SELECT plan_has_seek('SELECT * FROM ob WHERE sensor_id = 100');
SELECT const_mismatches('ob', 'ref', 90, 110), param_mismatches('ob', 'ref', -1, 202);
RESET plan_cache_mode;

-- Many small batches over several leaf pages. When the batches of a value
-- reach back to the previous leaf page, the lookup continues with the index
-- scan.
SET timescaledb.compression_batch_size_limit = 10;
CREATE TABLE ob_small (LIKE ref);
SELECT FROM create_hypertable('ob_small', 'time', chunk_time_interval => interval '1 year');
ALTER TABLE ob_small SET (timescaledb.compress, timescaledb.compress_segmentby = '',
    timescaledb.compress_orderby = 'sensor_id, time');
INSERT INTO ob_small SELECT * FROM ref;
SELECT count(compress_chunk(c)) FROM show_chunks('ob_small') c;
RESET timescaledb.compression_batch_size_limit;
VACUUM ANALYZE ob_small;

SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT plan_has_seek('SELECT * FROM ob_small WHERE sensor_id = 100');
SELECT const_mismatches('ob_small', 'ref', 1, 200), param_mismatches('ob_small', 'ref', -1, 202);
EXPLAIN (analyze, costs off, timing off, summary off)
SELECT * FROM generate_series(-1, 202) x,
LATERAL (SELECT count(*) FROM ob_small WHERE sensor_id = x) l;

-- Deleted batches are not where the lookup stops.
BEGIN;
SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0;
DELETE FROM ob_small WHERE sensor_id IN (99, 101, 102);
DELETE FROM ref WHERE sensor_id IN (99, 101, 102);
SELECT const_mismatches('ob_small', 'ref', 95, 106), param_mismatches('ob_small', 'ref', 95, 106);
ROLLBACK;
RESET enable_seqscan;
RESET enable_bitmapscan;

-- Range lookups start from the last batch that starts before the lower bound.
CREATE FUNCTION range_mismatches(tbl regclass, ref regclass) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    m int := 0;
    cond text;
    a record;
    b record;
BEGIN
    FOR lo IN -3..205 BY 7 LOOP
        FOREACH cond IN ARRAY ARRAY[
            format('sensor_id > %s AND sensor_id < %s', lo, lo + 5),
            format('sensor_id >= %s AND sensor_id <= %s', lo, lo + 30),
            format('sensor_id > %s', lo),
            format('sensor_id >= %s AND sensor_id < %s', lo, lo)]
        LOOP
            EXECUTE format('SELECT count(*) c, coalesce(sum(val), 0) s FROM %s WHERE %s', tbl, cond) INTO a;
            EXECUTE format('SELECT count(*) c, coalesce(sum(val), 0) s FROM %s WHERE %s', ref, cond) INTO b;
            IF a.c <> b.c OR a.s <> b.s THEN
                RAISE NOTICE 'mismatch for %: % <> %', cond, a, b;
                m := m + 1;
            END IF;
        END LOOP;
    END LOOP;
    RETURN m;
END
$$;

SET enable_seqscan = off;
SET enable_bitmapscan = off;
EXPLAIN (costs off)
SELECT sum(val) FROM ob WHERE sensor_id > 100 AND sensor_id < 110;
SELECT plan_has_seek('SELECT * FROM ob WHERE sensor_id > 100') AS one_sided_seek,
    plan_has_seek('SELECT * FROM ob WHERE sensor_id < 100') AS upper_only_seek;
EXPLAIN (analyze, costs off, timing off, summary off)
SELECT sum(val) FROM ob_small WHERE sensor_id > 100 AND sensor_id < 110;
SELECT range_mismatches('ob', 'ref'), range_mismatches('ob_small', 'ref');
SELECT count(*) FILTER (WHERE (h.c, h.s) IS DISTINCT FROM (r.c, r.s))
FROM generate_series(-1, 202) x,
LATERAL (SELECT count(*) c, coalesce(sum(val), 0) s FROM ob_small WHERE sensor_id > x AND sensor_id < x + 3) h,
LATERAL (SELECT count(*) c, coalesce(sum(val), 0) s FROM ref WHERE sensor_id > x AND sensor_id < x + 3) r;

-- Deleted batches are not used as the range start.
BEGIN;
SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0;
DELETE FROM ob_small WHERE sensor_id IN (99, 101, 102);
DELETE FROM ref WHERE sensor_id IN (99, 101, 102);
SELECT range_mismatches('ob_small', 'ref');
ROLLBACK;
RESET enable_seqscan;
RESET enable_bitmapscan;

-- "= ANY" looks up each value of the array.
-- sensor_id of the rows for a run-time array, in the order of the scan.
CREATE FUNCTION seek_order(tbl text, descending bool) RETURNS int[]
LANGUAGE plpgsql AS $$
DECLARE
    r int[];
BEGIN
    EXECUTE format('SELECT array_agg(sensor_id) FROM (SELECT sensor_id FROM %s
        WHERE sensor_id = ANY(ARRAY(SELECT unnest(''{120, 7, 33, 34}''::int[])))
        ORDER BY sensor_id %s) s', tbl, CASE WHEN descending THEN 'DESC' ELSE '' END) INTO r;
    RETURN r;
END
$$;

CREATE FUNCTION any_mismatches(tbl regclass, ref regclass) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    m int := 0;
    arr text;
    a record;
    b record;
BEGIN
    FOR lo IN -3..205 BY 11 LOOP
        FOREACH arr IN ARRAY ARRAY[
            format('ARRAY[%s]', lo),
            format('ARRAY[%s, %s, %s]', lo + 2, lo, lo + 1),
            format('ARRAY[%s, %s, %s, NULL]', lo, lo, lo + 40),
            format('ARRAY(SELECT generate_series(%s, %s, 3))', lo, lo + 25),
            format('ARRAY(SELECT unnest(ARRAY[%s, %s, %s, NULL]))', lo + 40, lo, lo),
            'ARRAY[]::int[]', 'NULL::int[]']
        LOOP
            EXECUTE format('SELECT count(*) c, coalesce(sum(val), 0) s FROM %s WHERE sensor_id = ANY(%s)', tbl, arr) INTO a;
            EXECUTE format('SELECT count(*) c, coalesce(sum(val), 0) s FROM %s WHERE sensor_id = ANY(%s)', ref, arr) INTO b;
            IF a.c <> b.c OR a.s <> b.s THEN
                RAISE NOTICE 'mismatch for %: % <> %', arr, a, b;
                m := m + 1;
            END IF;
        END LOOP;
    END LOOP;
    RETURN m;
END
$$;

-- Arrays known at run time use batch seek.
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET plan_cache_mode = force_generic_plan;
PREPARE any_ob(int[]) AS SELECT sum(val) FROM ob WHERE sensor_id = ANY($1);
PREPARE any_ob_small(int[]) AS SELECT sum(val) FROM ob_small WHERE sensor_id = ANY($1);
EXPLAIN (costs off) EXECUTE any_ob('{3, 100, 101}');
SELECT plan_has_seek('SELECT * FROM ob WHERE sensor_id <> ALL(ARRAY(SELECT generate_series(1, 2)))') AS all_seek;
EXPLAIN (analyze, costs off, timing off, summary off) EXECUTE any_ob('{150, 98, 99, 100, 99}');
EXPLAIN (analyze, costs off, timing off, summary off) EXECUTE any_ob_small('{150, 98, 99, 100, 99}');
EXECUTE any_ob('{150, 98, 99, 100, 99}');
EXECUTE any_ob_small('{150, 98, 99, 100, 99}');
SELECT sum(val) FROM ref WHERE sensor_id = ANY('{150, 98, 99, 100, 99}');
DEALLOCATE any_ob;
DEALLOCATE any_ob_small;
RESET plan_cache_mode;
SELECT any_mismatches('ob', 'ref'), any_mismatches('ob_small', 'ref');

-- Rows come in orderby order, in both directions.
SELECT t, d, seek_order(t, d) = seek_order('ref', d) AS ordered
FROM (VALUES ('ob'), ('ob_small')) t(t), (VALUES (false), (true)) d(d);

-- Deleted batches are skipped.
BEGIN;
SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0;
DELETE FROM ob_small WHERE sensor_id IN (99, 101, 102);
DELETE FROM ref WHERE sensor_id IN (99, 101, 102);
SELECT any_mismatches('ob_small', 'ref');
ROLLBACK;

-- Unordered chunk.
SET timescaledb.enable_direct_compress_insert = on;
INSERT INTO ob SELECT s, '2026-01-01 13:00'::timestamptz + n * interval '1 second', n
FROM generate_series(1, 200) s, generate_series(1, 3) n WHERE s % 5 <> 0;
INSERT INTO ref SELECT s, '2026-01-01 13:00'::timestamptz + n * interval '1 second', n
FROM generate_series(1, 200) s, generate_series(1, 3) n WHERE s % 5 <> 0;
RESET timescaledb.enable_direct_compress_insert;
SELECT _timescaledb_functions.chunk_status_text(c) FROM show_chunks('ob') c;
SELECT any_mismatches('ob', 'ref');
SELECT seek_order('ob', false) = seek_order('ref', false) AS ordered,
    seek_order('ob', true) = seek_order('ref', true) AS ordered_desc;
RESET enable_seqscan;
RESET enable_bitmapscan;

-- Constant arrays keep one range per value, which is also selective without
-- batch seek.
SELECT count(decompress_chunk(c)) FROM show_chunks('ob') c;
SELECT count(compress_chunk(c)) FROM show_chunks('ob') c;
EXPLAIN (costs off)
SELECT sum(val) FROM ob WHERE sensor_id = ANY(ARRAY[3, 100, 101]);

-- The default planner picks batch seek for lookups, and a plain scan when most
-- of the chunk is read.
CREATE TABLE ob_cost (sensor_id int NOT NULL, time timestamptz NOT NULL, val int);
SELECT FROM create_hypertable('ob_cost', 'time', chunk_time_interval => interval '1 year');
ALTER TABLE ob_cost SET (timescaledb.compress, timescaledb.compress_segmentby = '',
    timescaledb.compress_orderby = 'sensor_id, time');
INSERT INTO ob_cost SELECT s, '2026-01-01'::timestamptz + n * interval '1 minute', n
FROM generate_series(1, 40000) s, generate_series(1, 10) n;
SELECT count(compress_chunk(c)) FROM show_chunks('ob_cost') c;
VACUUM ANALYZE ob_cost;
SELECT plan_has_seek('SELECT * FROM ob_cost WHERE sensor_id = 39990') AS point,
    plan_has_seek('SELECT * FROM generate_series(1, 40000, 40) k(id),
        LATERAL (SELECT count(*) FROM ob_cost WHERE sensor_id = k.id) l') AS lateral,
    plan_has_seek('SELECT count(*) FROM generate_series(1, 40000, 40) k(id)
        JOIN ob_cost t ON t.sensor_id = k.id') AS join_seek,
    plan_has_seek('SELECT * FROM ob_cost WHERE sensor_id = ANY(ARRAY(SELECT generate_series(39990, 39995)))') AS any_seek,
    plan_has_seek('SELECT * FROM ob_cost WHERE sensor_id > 1') AS most_of_chunk;

-- The condition on the seek column is checked first, so the batches without
-- the value don't decompress the other filter columns.
SET enable_seqscan = off;
SET enable_bitmapscan = off;
EXPLAIN (costs off)
SELECT count(*) FROM ob_cost WHERE val = 3 AND sensor_id = 39990;
SELECT count(*) FROM ob_cost WHERE val = 3 AND sensor_id = 39990;
SELECT count(*) FROM ob_cost WHERE val = 3 AND sensor_id = ANY(ARRAY(SELECT generate_series(39990, 39995)));
RESET enable_seqscan;
RESET enable_bitmapscan;
