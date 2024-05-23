-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SET timescaledb.enable_chunk_append TO false;
SET timescaledb.enable_constraint_aware_append TO false;
SET timescaledb.current_timestamp_mock TO '1990-01-01';

\set PREFIX 'EXPLAIN (COSTS OFF, SUMMARY OFF, TIMING OFF)'

-- create a test table
-- any query with successful now_constify will have 1 chunk while
-- others will have 2 chunks in plan
CREATE TABLE const_now(time timestamptz, time2 timestamptz, device_id int, value float);
SELECT table_name FROM create_hypertable('const_now','time');
INSERT INTO const_now SELECT '1000-01-01','1000-01-01',1,0.5;
INSERT INTO const_now SELECT '1000-01-01','1000-01-01',2,0.5;
INSERT INTO const_now SELECT '3000-01-01','3000-01-01',1,0.5;
INSERT INTO const_now SELECT '3000-01-01','3000-01-01',2,0.5;

-- test valid variants we are optimizing
-- all of these should have a constified value as filter
-- none of these initial tests will actually exclude chunks
-- because we want to see the constified now expression in
-- EXPLAIN output
:PREFIX SELECT FROM const_now WHERE time > now();
:PREFIX SELECT FROM const_now WHERE time >= now();
:PREFIX SELECT FROM const_now WHERE time > now() - '24h'::interval;
:PREFIX SELECT FROM const_now WHERE time > now() + '10m'::interval;
:PREFIX SELECT FROM const_now WHERE time >= now() - '10m'::interval;
:PREFIX SELECT FROM const_now WHERE time >= now() + '10m'::interval;
:PREFIX SELECT FROM const_now WHERE time > now() - '2d'::interval;
:PREFIX SELECT FROM const_now WHERE time > now() + '3d'::interval;
:PREFIX SELECT FROM const_now WHERE time > now() - '1week'::interval;
:PREFIX SELECT FROM const_now WHERE time > now() - '1month'::interval;
:PREFIX SELECT FROM const_now WHERE time > CURRENT_TIMESTAMP;
:PREFIX SELECT FROM const_now WHERE time >= CURRENT_TIMESTAMP;
:PREFIX SELECT FROM const_now WHERE time > CURRENT_TIMESTAMP - '24h'::interval;
:PREFIX SELECT FROM const_now WHERE time > CURRENT_TIMESTAMP + '10m'::interval;
:PREFIX SELECT FROM const_now WHERE time >= CURRENT_TIMESTAMP - '10m'::interval;
:PREFIX SELECT FROM const_now WHERE time >= CURRENT_TIMESTAMP + '10m'::interval;
:PREFIX SELECT FROM const_now WHERE time > CURRENT_TIMESTAMP - '2d'::interval;
:PREFIX SELECT FROM const_now WHERE time > CURRENT_TIMESTAMP + '3d'::interval;
:PREFIX SELECT FROM const_now WHERE time > CURRENT_TIMESTAMP - '1week'::interval;
:PREFIX SELECT FROM const_now WHERE time > CURRENT_TIMESTAMP - '1month'::interval;


-- test bitmapheapscan
SET enable_indexscan TO false;
:PREFIX SELECT FROM const_now WHERE time > now();
RESET enable_indexscan;

-- test multiple constraints
:PREFIX SELECT FROM const_now WHERE time >= now() + '10m'::interval AND device_id = 2;
:PREFIX SELECT FROM const_now WHERE time >= now() + '10m'::interval AND (device_id = 2 OR device_id = 3);
:PREFIX SELECT FROM const_now WHERE time >= now() + '10m'::interval AND time >= now() - '10m'::interval;

-- variants we don't optimize
:PREFIX SELECT FROM const_now WHERE time > now()::date;
:PREFIX SELECT FROM const_now WHERE round(EXTRACT(EPOCH FROM now())) > 0.5;

-- we only modify top-level ANDed now() expressions
:PREFIX SELECT FROM const_now WHERE time > now() - '1m'::interval OR time > now() + '1m'::interval;
:PREFIX SELECT FROM const_now WHERE device_id = 2 OR (time > now() - '1m'::interval AND time > now() + '1m'::interval);

-- CTE
:PREFIX WITH q1 AS (
  SELECT * FROM const_now WHERE time > now()
) SELECT FROM q1;

:PREFIX WITH q1 AS (
  SELECT * FROM const_now
) SELECT FROM q1 WHERE time > now();

-- JOIN
:PREFIX SELECT FROM const_now m1, const_now m2 WHERE m1.time > now();
:PREFIX SELECT FROM const_now m1, const_now m2 WHERE m2.time > now();
:PREFIX SELECT FROM const_now m1, const_now m2 WHERE m1.time > now() AND m2.time > now();

-- only top-level constraints in WHERE clause are constified
:PREFIX SELECT FROM const_now m1 INNER JOIN const_now m2 ON (m1.time > now());
:PREFIX SELECT FROM const_now m1 INNER JOIN const_now m2 ON (m1.time > now()) WHERE m2.time > now();

-- test UPDATE
:PREFIX UPDATE const_now SET value = EXTRACT(EPOCH FROM now()) WHERE time > now();

-- test DELETE
:PREFIX DELETE FROM const_now WHERE time > now();

-- test chunks actually get excluded
-- should exclude all
SET timescaledb.current_timestamp_mock TO '2010-01-01';
:PREFIX SELECT FROM const_now WHERE time > now();

-- should exclude all but 1 chunk
SET timescaledb.current_timestamp_mock TO '2000-01-14';
:PREFIX SELECT FROM const_now WHERE time > now();

-- should have one time filter false
:PREFIX SELECT FROM const_now WHERE time > now();

-- no constification because it's not partitioning column
:PREFIX SELECT FROM const_now WHERE time2 > now();

DROP TABLE const_now;

-- test prepared statements
CREATE TABLE prep_const_now(time timestamptz, device int, value float);
SELECT table_name FROM create_hypertable('prep_const_now', 'time');

INSERT INTO prep_const_now SELECT '3000-01-02', 1, 0.2;
INSERT INTO prep_const_now SELECT '3001-01-02', 2, 0.3;
INSERT INTO prep_const_now SELECT '3002-01-02', 3, 0.4;

SET timescaledb.current_timestamp_mock TO '3001-01-01';
PREPARE p1 AS SELECT FROM prep_const_now WHERE time > now();
:PREFIX EXECUTE p1;
EXECUTE p1;
SET timescaledb.current_timestamp_mock TO '3002-01-01';
-- plan won't change cause the query didnt get replanned
:PREFIX EXECUTE p1;
EXECUTE p1;

-- same test with ChunkAppend enabled
set timescaledb.enable_chunk_append = on;
set timescaledb.enable_constraint_aware_append = on;
DEALLOCATE p1;
PREPARE p1 AS SELECT FROM prep_const_now WHERE time > now();
set timescaledb.current_timestamp_mock TO '3001-01-01';
:PREFIX EXECUTE p1;
EXECUTE p1;
SET timescaledb.current_timestamp_mock TO '3002-01-01';
-- plan won't change cause the query didnt get replanned
:PREFIX EXECUTE p1;
EXECUTE p1;
set timescaledb.enable_chunk_append = off;
set timescaledb.enable_constraint_aware_append = off;

DROP TABLE prep_const_now;

-- test outer var references dont trip up constify_now
-- no optimization is done in this case
:PREFIX SELECT * FROM
  metrics_tstz m1
  INNER JOIN metrics_tstz as m2 on (true)
WHERE
  EXISTS (SELECT * FROM metrics_tstz AS m3 WHERE m2.time > now());

-- test dst interaction with day intervals
SET timezone TO 'Europe/Berlin';
CREATE TABLE const_now_dst(time timestamptz not null);
SELECT table_name FROM create_hypertable('const_now_dst','time',chunk_time_interval:='30minutes'::interval);

-- create 2 chunks
INSERT INTO const_now_dst SELECT '2022-03-27 03:15:00+02';
INSERT INTO const_now_dst SELECT '2022-03-27 03:45:00+02';

SELECT * FROM const_now_dst WHERE time >= '2022-03-28 0:45+0'::timestamptz - '1d'::interval;
SELECT * FROM const_now_dst WHERE time >= '2022-03-28 1:15+0'::timestamptz - '1d'::interval;

SET timescaledb.current_timestamp_mock TO '2022-03-28 0:45+0';

-- must have 2 chunks in plan
:PREFIX SELECT FROM const_now_dst WHERE time > now() - '1day'::interval;

SET timescaledb.current_timestamp_mock TO '2022-03-28 1:15+0';

-- must have 2 chunks in plan
:PREFIX SELECT FROM const_now_dst WHERE time > now() - '1day'::interval;

TRUNCATE const_now_dst;
SELECT set_chunk_time_interval('const_now_dst','1 day'::interval, 'time');

-- test month calculation safety buffer
SET timescaledb.current_timestamp_mock TO '2001-03-1 0:30:00+00';
INSERT INTO const_now_dst SELECT generate_series('2001-01-28'::timestamptz, '2001-02-01', '1day'::interval);
set timezone to 'utc+1';
-- must have 5 chunks in plan
:PREFIX SELECT * FROM const_now_dst WHERE time > now() - '1 month'::interval;
set timezone to 'utc-1';
-- must have 5 chunks in plan
:PREFIX SELECT * FROM const_now_dst WHERE time > now() - '1 month'::interval;

DROP TABLE const_now_dst;

-- test now constification with VIEWs
SET timescaledb.current_timestamp_mock TO '2003-01-01 0:30:00+00';
CREATE TABLE now_view_test(time timestamptz,device text, value float);
SELECT table_name FROM create_hypertable('now_view_test','time');
-- create 5 chunks
INSERT INTO now_view_test SELECT generate_series('2000-01-01'::timestamptz,'2004-01-01'::timestamptz,'1year'::interval), 'a', 0.5;
CREATE VIEW now_view AS SELECT time, device, avg(value) from now_view_test GROUP BY 1,2;
-- should have all 5 chunks in EXPLAIN
:PREFIX SELECT * FROM now_view;
-- should have 2 chunks in EXPLAIN
:PREFIX SELECT * FROM now_view WHERE time > now() - '168h'::interval;
DROP TABLE now_view_test CASCADE;

-- #4709
-- test queries with constraints involving columns from different nesting levels
SELECT * FROM
  (SELECT * FROM metrics m1 LIMIT 1) m1
  INNER JOIN (SELECT * FROM metrics m2 LIMIT 1) m2 ON true,
  LATERAL (SELECT m2.time FROM devices LIMIT 1) as subq_1
WHERE subq_1.time > m1.time;

CREATE TABLE logged_data (
    rawtag_id   INTEGER,
    timestamp   TIMESTAMP WITH TIME ZONE NOT NULL,
    value       REAL,
    site_id     INTEGER
);

SELECT table_name FROM create_hypertable('logged_data', 'timestamp');

INSERT INTO logged_data (rawtag_id, timestamp, value, site_id)
VALUES
(1, '2023-01-01'::timestamptz, 13, 1),
(1, '2023-01-07'::timestamptz, 13, 1),
(1, '2023-01-10'::timestamptz, 13, 1),
(1, '2023-01-15'::timestamptz, 13, 1),
(1, '2023-01-20'::timestamptz, 15, 1),
(2, '2023-01-01'::timestamptz, 90, 1),
(3, '2023-01-07'::timestamptz, 2, 1),
(4, '2023-01-10'::timestamptz, 13, 3),
(2, '2023-01-15'::timestamptz, 13, 4),
(5, '2023-01-20'::timestamptz, 13, 1);

-- four chunks, all of them should be excluded at plantime
SELECT COUNT(*) FROM show_chunks('logged_data');

SET timescaledb.current_timestamp_mock TO '2024-01-01 0:30:00+00';

SET timescaledb.enable_chunk_append TO true;
SET timescaledb.enable_constraint_aware_append TO true;

-- for all the queries below, exclusion should be happening at plantime
EXPLAIN (ANALYZE, SUMMARY OFF, TIMING OFF) SELECT * FROM logged_data WHERE
timestamp BETWEEN now() - interval '1 day' AND now()
AND rawtag_id = 1 ORDER BY "timestamp" ASC;

EXPLAIN (ANALYZE, SUMMARY OFF, TIMING OFF) SELECT * FROM logged_data WHERE
timestamp <= now() AND timestamp >= now() - interval '1 day'
AND rawtag_id = 1 ORDER BY "timestamp" ASC;

EXPLAIN (ANALYZE, SUMMARY OFF, TIMING OFF) SELECT * FROM logged_data WHERE
timestamp <= now() AND timestamp >= now() - interval '1 day'
ORDER BY "timestamp" ASC;

-- mock_now() in the middle of the table
SET timescaledb.current_timestamp_mock TO '2023-01-11';
PREPARE pbtw AS SELECT * FROM logged_data WHERE
timestamp BETWEEN now() - interval '5 day' AND now() AND rawtag_id = 1
ORDER BY "timestamp" ASC;

EXPLAIN (SUMMARY OFF, TIMING OFF) EXECUTE pbtw;
EXECUTE pbtw;
-- now move mock_now() to the future
SET timescaledb.current_timestamp_mock TO '2023-01-21 0:30:00+00';
EXPLAIN (SUMMARY OFF, TIMING OFF) EXECUTE pbtw;
EXECUTE pbtw;
-- much further into the future, no rows should be returned
SET timescaledb.current_timestamp_mock TO '2024-01-21 0:30:00+00';
EXPLAIN (SUMMARY OFF, TIMING OFF) EXECUTE pbtw;
EXECUTE pbtw;
DEALLOCATE pbtw;

-- repeat without ChunkAppend
set timescaledb.enable_chunk_append = off;
set timescaledb.enable_constraint_aware_append = off;
SET timescaledb.current_timestamp_mock TO '2023-01-11';
PREPARE pbtw AS SELECT * FROM logged_data WHERE
timestamp BETWEEN now() - interval '5 day' AND now() AND rawtag_id = 1
ORDER BY "timestamp" ASC;

EXPLAIN (SUMMARY OFF, TIMING OFF) EXECUTE pbtw;
EXECUTE pbtw;
-- now move mock_now() to the future
SET timescaledb.current_timestamp_mock TO '2023-01-21 0:30:00+00';
EXPLAIN (SUMMARY OFF, TIMING OFF) EXECUTE pbtw;
EXECUTE pbtw;
-- much further into the future, no rows should be returned
SET timescaledb.current_timestamp_mock TO '2024-01-21 0:30:00+00';
EXPLAIN (SUMMARY OFF, TIMING OFF) EXECUTE pbtw;
EXECUTE pbtw;
DEALLOCATE pbtw;

DROP TABLE logged_data;
