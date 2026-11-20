-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Tests for timescaledb.scan_uncompressed_chunks.

\set ECHO errors

-- Same result set with the GUC off and on.
CREATE FUNCTION same_result(q text) RETURNS bool LANGUAGE plpgsql AS $$
DECLARE n bigint;
BEGIN
    PERFORM set_config('timescaledb.scan_uncompressed_chunks', 'off', true);
    EXECUTE format('CREATE TEMP TABLE r_off AS SELECT t::text AS r FROM (%s) t', q);
    PERFORM set_config('timescaledb.scan_uncompressed_chunks', 'on', true);
    EXECUTE format('CREATE TEMP TABLE r_on AS SELECT t::text AS r FROM (%s) t', q);
    SELECT count(*) INTO n FROM ((TABLE r_off EXCEPT ALL TABLE r_on)
                                 UNION ALL (TABLE r_on EXCEPT ALL TABLE r_off)) d;
    DROP TABLE r_off, r_on;
    RETURN n = 0;
END $$;

\set ECHO all

SET max_parallel_workers_per_gather = 0;

-- Four compressed chunks. The DROP COLUMN gives the first two chunks a
-- different column layout than the last two. Times are even so the odd row
-- inserted below is unique.
CREATE TABLE m(time int NOT NULL, junk int, device int, value float);
SELECT table_name FROM create_hypertable('m', 'time', chunk_time_interval => 100);
INSERT INTO m SELECT t, 0, d, t * 10 + d FROM generate_series(0, 198, 2) t, generate_series(1, 3) d;
ALTER TABLE m DROP COLUMN junk;
ALTER TABLE m ADD COLUMN extra int DEFAULT 7;
INSERT INTO m(time, device, value, extra)
SELECT t, d, t * 10 + d, d FROM generate_series(200, 398, 2) t, generate_series(1, 3) d;
ALTER TABLE m SET (timescaledb.compress,
                   timescaledb.compress_segmentby = 'device',
                   timescaledb.compress_orderby = 'time');
SELECT count(compress_chunk(c)) FROM show_chunks('m') c;

-- Second chunk becomes partial, and its uncompressed row is the latest
-- device 1 row below 200.
INSERT INTO m(time, device, value) VALUES (199, 1, -1);
-- Fifth chunk stays uncompressed.
INSERT INTO m(time, device, value, extra)
SELECT t, d, t * 10 + d, d FROM generate_series(400, 498, 2) t, generate_series(1, 3) d;
ANALYZE m;

SELECT _timescaledb_functions.chunk_status_text(c) FROM show_chunks('m') c ORDER BY c;

-- The uncompressed row wins over the compressed batches.
SET timescaledb.enable_deferred_chunk_append = off;
SET timescaledb.scan_uncompressed_chunks = on;
SELECT time, device FROM m WHERE device = 1 AND time < 200 ORDER BY time DESC LIMIT 1;
SELECT time, device FROM m WHERE device = 1 AND time >= 100 ORDER BY time LIMIT 1;
RESET timescaledb.enable_deferred_chunk_append;

-- Same results with the GUC off and on.
CREATE TABLE queries(q text);
INSERT INTO queries VALUES
    ('SELECT count(*), sum(value), max(extra) FROM m'),
    ('SELECT device, count(*), sum(value), max(extra) FROM m GROUP BY device'),
    ('SELECT extra, avg(value) FROM m GROUP BY extra'),
    ('SELECT time_bucket(50, time) b, device, sum(value) FROM m GROUP BY b, device'),
    ('SELECT device, count(*) FILTER (WHERE value > 1000), sum(value + device) FROM m GROUP BY device'),
    ('SELECT count(DISTINCT device), count(DISTINCT extra) FROM m'),
    ('SELECT device, max(value) FROM m WHERE time > 150 GROUP BY device HAVING max(value) > 0'),
    ('SELECT d.device, sum(m.value) FROM m JOIN (VALUES (1), (3)) d(device) USING (device) GROUP BY d.device'),
    ('SELECT time, device FROM m WHERE device = 1 ORDER BY time DESC LIMIT 1'),
    ('SELECT time, device FROM m WHERE device = 1 AND time < 200 ORDER BY time DESC LIMIT 1'),
    ('SELECT time, device FROM m WHERE device = 2 ORDER BY time LIMIT 1'),
    ('SELECT time, device FROM m WHERE device IN (1, 2) ORDER BY device, time LIMIT 1'),
    ('SELECT * FROM m WHERE device = 3 ORDER BY time DESC LIMIT 5');

SELECT same_result(q), q FROM queries ORDER BY q COLLATE "C";

SET timescaledb.enable_deferred_chunk_append = off;
SELECT same_result(q), q FROM queries ORDER BY q COLLATE "C";
RESET timescaledb.enable_deferred_chunk_append;

SET max_parallel_workers_per_gather = 2;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
SELECT same_result(q), q FROM queries ORDER BY q COLLATE "C";
RESET parallel_setup_cost;
RESET parallel_tuple_cost;
RESET min_parallel_table_scan_size;
SET max_parallel_workers_per_gather = 0;

-- Generic plan.
SET timescaledb.scan_uncompressed_chunks = on;
SET plan_cache_mode = force_generic_plan;
PREPARE p(int) AS SELECT device, count(*), sum(value) FROM m WHERE device = $1 GROUP BY device;
EXECUTE p(1);
EXECUTE p(2);
SET timescaledb.scan_uncompressed_chunks = off;
EXECUTE p(1);
EXECUTE p(2);
DEALLOCATE p;
RESET plan_cache_mode;

RESET timescaledb.scan_uncompressed_chunks;
DROP TABLE queries;
DROP TABLE m;
