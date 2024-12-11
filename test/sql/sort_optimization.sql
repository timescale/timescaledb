-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\set PREFIX 'EXPLAIN (COSTS OFF) '

CREATE TABLE order_test(time int NOT NULL, device_id int, value float);
CREATE INDEX ON order_test(time,device_id);
CREATE INDEX ON order_test(device_id,time);

SELECT create_hypertable('order_test','time',chunk_time_interval:=1000);

INSERT INTO order_test SELECT 0,10,0.5;
INSERT INTO order_test SELECT 1,9,0.5;
INSERT INTO order_test SELECT 2,8,0.5;

-- we want to see here that index scans are possible for the chosen expressions
-- so we disable seqscan so we dont need to worry about other factors which would
-- make PostgreSQL prefer seqscan over index scan
SET enable_seqscan TO off;

-- test sort optimization with single member order by
SELECT time_bucket(10,time),device_id,value FROM order_test ORDER BY 1;
-- should use index scan
:PREFIX SELECT time_bucket(10,time),device_id,value FROM order_test ORDER BY 1;

-- test sort optimization with ordering by multiple columns and time_bucket not last
SELECT time_bucket(10,time),device_id,value FROM order_test ORDER BY 1,2;
-- must not use index scan
:PREFIX SELECT time_bucket(10,time),device_id,value FROM order_test ORDER BY 1,2;

-- test sort optimization with ordering by multiple columns and time_bucket as last member
SELECT time_bucket(10,time),device_id,value FROM order_test ORDER BY 2,1;
-- should use index scan
:PREFIX SELECT time_bucket(10,time),device_id,value FROM order_test ORDER BY 2,1;

-- test sort optimization with interval calculation with non-fixed interval
-- #7097
CREATE TABLE i7097_1(time timestamptz NOT NULL, quantity float, "isText" boolean);
CREATE TABLE i7097_2(time timestamptz NOT NULL, quantity float, "isText" boolean);

CREATE INDEX ON i7097_1(time) WHERE "isText" IS NULL;
CREATE INDEX ON i7097_2(time) WHERE "isText" IS NULL;

SELECT table_name FROM create_hypertable('i7097_1', 'time', create_default_indexes => false);
SELECT table_name FROM create_hypertable('i7097_2', 'time', create_default_indexes => false);

INSERT INTO i7097_1(time, quantity)
SELECT time, round((random() * (100-3) + 3)::NUMERIC) AS quantity
FROM generate_series('2023-01-01T00:00:00+01:00', '2023-05-01T00:00:00+01:00', interval 'PT10M') AS t(time);

INSERT INTO i7097_2(time, quantity)
SELECT time, round((random() * (100-3) + 3)::NUMERIC) AS quantity
FROM generate_series('2023-01-01T00:00:00+01:00', '2023-05-01T00:00:00+01:00', interval 'PT10M') AS t(time);

VACUUM ANALYZE i7097_1, i7097_2;

SET TIME ZONE 'Europe/Paris';
WITH
"cte1" AS (SELECT time + interval 'P1Y' AS time, avg(quantity) AS quantity FROM i7097_1 WHERE time >= '2024-03-31T00:00:00+01:00'::timestamptz - interval 'P1Y' AND time < '2024-03-31T23:59:59+02:00'::timestamptz + (- interval 'P1Y') AND "isText" IS NULL GROUP BY 1 ORDER BY 1 ASC),
"cte2" AS (SELECT time + interval 'P1Y' AS time, avg(quantity) AS quantity FROM i7097_2 WHERE time >= '2024-03-31T00:00:00+01:00'::timestamptz - interval 'P1Y' AND time < '2024-03-31T23:59:59+02:00'::timestamptz + (- interval 'P1Y') AND "isText" IS NULL GROUP BY 1 ORDER BY 1 ASC)
SELECT count(*) FROM (SELECT time, cte1.quantity + cte2.quantity FROM cte1 FULL OUTER JOIN cte2 USING (time)) j;

