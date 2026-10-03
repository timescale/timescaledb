-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Vectorized filters with join and initplan parameters. The parameter value
-- changes between rescans, so the vectorized filter has to pick up the new
-- value every time.
\c :TEST_DBNAME :ROLE_SUPERUSER

SET max_parallel_workers_per_gather = 0;

CREATE TABLE ref (sensor_id int NOT NULL, time timestamptz NOT NULL, val int, tag text);
INSERT INTO ref
SELECT s, '2026-01-01'::timestamptz + n * interval '1 minute', (s * 7 + n) % 1000, 'tag' || (s % 5)
FROM generate_series(1, 300) s, generate_series(1, (s * 37) % 400 + 1) n;
CREATE INDEX ON ref (sensor_id);

-- Two chunks, so that the aggregation is split per chunk and can be
-- vectorized.
CREATE TABLE ob (LIKE ref);
SELECT FROM create_hypertable('ob', 'time', chunk_time_interval => interval '1 day');
ALTER TABLE ob SET (timescaledb.compress, timescaledb.compress_segmentby = '',
    timescaledb.compress_orderby = 'sensor_id, time');
INSERT INTO ob SELECT * FROM ref;
INSERT INTO ob VALUES (1, '2026-01-10', 1, 'x');
DELETE FROM ob WHERE time = '2026-01-10';
SELECT count(compress_chunk(c)) FROM show_chunks('ob') c;
VACUUM ANALYZE ob;

CREATE TABLE seg (LIKE ref);
SELECT FROM create_hypertable('seg', 'time', chunk_time_interval => interval '1 day');
ALTER TABLE seg SET (timescaledb.compress, timescaledb.compress_segmentby = 'sensor_id',
    timescaledb.compress_orderby = 'time');
INSERT INTO seg SELECT * FROM ref;
INSERT INTO seg VALUES (1, '2026-01-10', 1, 'x');
DELETE FROM seg WHERE time = '2026-01-10';
SELECT count(compress_chunk(c)) FROM show_chunks('seg') c;
VACUUM ANALYZE seg;

-- Number of outer values where the lateral lookup differs from the reference.
CREATE FUNCTION lateral_mismatches(tbl regclass, outer_values text, cond text) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    m int;
BEGIN
    EXECUTE format($q$
        SELECT count(*) FILTER (WHERE (h.c, h.s) IS DISTINCT FROM (r.c, r.s))
        FROM (%s) x(x),
        LATERAL (SELECT count(*) c, sum(val) s FROM %s WHERE %s) h,
        LATERAL (SELECT count(*) c, sum(val) s FROM ref WHERE %s) r
    $q$, outer_values, tbl, cond, cond) INTO m;
    RETURN m;
END
$$;

-- The join parameter filter is vectorized, and so is the aggregation above it.
EXPLAIN (costs off)
SELECT * FROM generate_series(1, 3) x,
LATERAL (SELECT sum(val) FROM ob WHERE sensor_id = x) l;

EXPLAIN (costs off)
SELECT * FROM generate_series(1, 3) x,
LATERAL (SELECT sum(val) FROM seg WHERE val > x) l;

-- Equality on the orderby column and ranges on a plain column, with values
-- that change on every rescan, repeat, and fall outside the data.
SELECT lateral_mismatches('ob', 'SELECT generate_series(-1, 302)', 'sensor_id = x');
SELECT lateral_mismatches('ob', 'SELECT generate_series(0, 1000, 7)', 'val > x');
SELECT lateral_mismatches('seg', 'SELECT generate_series(0, 1000, 7)', 'val <= x');
SELECT lateral_mismatches('ob', 'VALUES (5), (5), (7), (5), (300), (300)', 'sensor_id = x');

-- NULL parameter matches nothing.
SELECT lateral_mismatches('ob', 'VALUES (1), (NULL::int), (3)', 'sensor_id = x');
SELECT lateral_mismatches('seg', 'VALUES (1), (NULL::int), (3)', 'val = x');

-- Parameter of another type, text with collation, and arrays.
SELECT lateral_mismatches('ob', 'SELECT generate_series(1, 300, 13)::bigint', 'sensor_id = x');
SELECT lateral_mismatches('seg', 'VALUES (''tag1''), (''tag3''), (''nope'')', 'tag = x');
SELECT lateral_mismatches('seg', 'VALUES (ARRAY[1, 2, 3]), (ARRAY[500, 501]), (NULL::int[])', 'val = ANY(x)');

-- Parameter inside an expression that is folded to a constant.
SELECT lateral_mismatches('seg', 'SELECT generate_series(0, 1000, 50)', 'val > x + 10');

-- Initplan parameter.
EXPLAIN (costs off)
SELECT sum(val) FROM ob WHERE sensor_id = (SELECT max(sensor_id) - 1 FROM ref);
SELECT sum(val) FROM ob WHERE sensor_id = (SELECT max(sensor_id) - 1 FROM ref);
SELECT sum(val) FROM ref WHERE sensor_id = (SELECT max(sensor_id) - 1 FROM ref);

-- Correlated subquery in the target list.
SELECT sum((SELECT sum(val) FROM seg WHERE val = x)) AS seg,
    sum((SELECT sum(val) FROM ref WHERE val = x)) AS ref
FROM generate_series(1, 1000, 3) x;

-- Without aggregation above the scan.
SELECT count(*) FROM generate_series(1, 300, 7) x,
LATERAL (SELECT * FROM ob WHERE sensor_id = x AND val > x) l;
SELECT count(*) FROM generate_series(1, 300, 7) x,
LATERAL (SELECT * FROM ref WHERE sensor_id = x AND val > x) l;

-- Partial chunk: the uncompressed rows use the regular filter.
INSERT INTO ob VALUES (5, '2026-01-01 23:00', 999, 'y'), (6, '2026-01-01 23:00', 999, 'y');
INSERT INTO ref VALUES (5, '2026-01-01 23:00', 999, 'y'), (6, '2026-01-01 23:00', 999, 'y');
SELECT lateral_mismatches('ob', 'SELECT generate_series(1, 10)', 'sensor_id = x');
SELECT lateral_mismatches('ob', 'SELECT generate_series(990, 1000)', 'val >= x');
