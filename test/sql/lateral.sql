-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE TABLE regular_table(name text, junk text);
CREATE TABLE ht(time timestamptz NOT NULL, location text);
SELECT create_hypertable('ht', 'time');

INSERT INTO ht(time) select timestamp 'epoch' + (i * interval '1 second') from generate_series(1, 100) as T(i);
INSERT INTO regular_table values('name', 'junk');

SELECT * FROM regular_table ik LEFT JOIN LATERAL (select max(time::timestamptz) from ht s where ik.name='name' and s.time < now()) s on true;
select * from regular_table ik LEFT JOIN LATERAL (select max(time::timestamptz) from ht s where ik.name='name' and s.time > now()) s on true;

DROP TABLE regular_table;
DROP TABLE ht;

CREATE TABLE orders(id int, user_id int, time TIMESTAMPTZ NOT NULL);
SELECT create_hypertable('orders', 'time');
INSERT INTO orders values(1,1,timestamp 'epoch' + '1 second');
INSERT INTO orders values(2,1,timestamp 'epoch' + '2 second');
INSERT INTO orders values(3,1,timestamp 'epoch' + '3 second');
INSERT INTO orders values(4,2,timestamp 'epoch' + '4 second');
INSERT INTO orders values(5,1,timestamp 'epoch' + '5 second');
INSERT INTO orders values(6,3,timestamp 'epoch' + '6 second');
INSERT INTO orders values(7,1,timestamp 'epoch' + '7 second');
INSERT INTO orders values(8,4,timestamp 'epoch' + '8 second');
INSERT INTO orders values(9,2,timestamp 'epoch' + '9 second');

-- Need a LATERAL query with a reference to the upper-level table and
-- with a restriction on time
-- Upper-level table constraint should be a constant in order to trigger
-- creation of a one-time filter in the planner
SELECT user_id, first_order_time, max_time FROM
(SELECT user_id, min(time) AS first_order_time FROM orders GROUP BY user_id) o1
LEFT JOIN LATERAL
(SELECT max(time) AS max_time FROM orders WHERE o1.user_id = '2' AND time > now()) o2 ON true
ORDER BY user_id, first_order_time, max_time;

SELECT user_id, first_order_time, max_time FROM
(SELECT user_id, min(time) AS first_order_time FROM orders GROUP BY user_id) o1
LEFT JOIN LATERAL
(SELECT max(time) AS max_time FROM orders WHERE o1.user_id = '2' AND time < now()) o2 ON true
ORDER BY user_id, first_order_time, max_time;

-- Nested LATERALs
SELECT user_id, first_order_time, time1, min_time FROM
(SELECT user_id, min(time) AS first_order_time FROM orders GROUP BY user_id) o1
LEFT JOIN LATERAL
(SELECT user_id as o2user_id, time AS time1 FROM orders WHERE o1.user_id = '2' AND time < now()) o2 ON true
LEFT JOIN LATERAL
(SELECT min(time) as min_time FROM orders WHERE o2.o2user_id = '1' AND time < now()) o3 ON true
ORDER BY user_id, first_order_time, time1, min_time;

-- Cleanup
DROP TABLE orders;

---- OUTER JOIN tests ---
--github issue 2500

CREATE TABLE t1_timescale (a int, b int);
CREATE TABLE t2 (a int, b int);
SELECT create_hypertable('t1_timescale', 'a', chunk_time_interval=>1000);

INSERT into t2 values (3, 3), (15 , 15);
INSERT into t1_timescale select generate_series(5, 25, 1), 77;
UPDATE t1_timescale SET b = 15 WHERE a = 15;

SELECT * FROM t1_timescale
FULL OUTER JOIN  t2 on t1_timescale.b=t2.b and t2.b between 10 and 20
ORDER BY 1, 2, 3, 4;

SELECT * FROM t1_timescale
LEFT OUTER JOIN  t2 on t1_timescale.b=t2.b and t2.b between 10 and 20
WHERE t1_timescale.a=5
ORDER BY 1, 2, 3, 4;

SELECT * FROM t1_timescale
RIGHT JOIN  t2 on t1_timescale.b=t2.b and t2.b between 10 and 20
ORDER BY 1, 2, 3, 4;

SELECT * FROM t1_timescale
RIGHT JOIN  t2 on t1_timescale.b=t2.b and t2.b between 10 and 20
WHERE t1_timescale.a=5
ORDER BY 1, 2, 3, 4;

SELECT * FROM t1_timescale
LEFT OUTER JOIN  t2 on t1_timescale.a=t2.a and t2.b between 10 and 20
WHERE t1_timescale.a IN ( 10, 15, 20, 25)
ORDER BY 1, 2, 3, 4;

SELECT * FROM t1_timescale
RIGHT OUTER JOIN  t2 on t1_timescale.a=t2.a and t2.b between 10 and 20
ORDER BY 1, 2, 3, 4;

-- chunk exclusion on a hash partitioned column when the hypertable is on the
-- parameterized side of a nested loop
CREATE TABLE metric(time timestamptz NOT NULL, device_id bigint NOT NULL, value float);
SELECT create_hypertable('metric', by_range('time', INTERVAL '30 days'));
SELECT add_dimension('metric', by_hash('device_id', 4));
CREATE INDEX ON metric(device_id, time);
INSERT INTO metric
SELECT '2025-01-01'::timestamptz + i * INTERVAL '6 hours', d, d
FROM generate_series(0, 359) i, generate_series(1, 8) d;
CREATE INDEX ON metric(value, time);
ANALYZE metric;

-- only show the exclusion counts as the rest of the plan varies between versions
CREATE FUNCTION exclusion_info(query text) RETURNS SETOF text LANGUAGE plpgsql AS
$$
DECLARE
  ln text;
BEGIN
  FOR ln IN EXECUTE 'EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) ' || query
  LOOP
    IF ln ~ 'ChunkAppend|Append|excluded' THEN
      RETURN NEXT regexp_replace(ln, ' \(actual.*', '');
    END IF;
  END LOOP;
END;
$$;

SET enable_hashjoin TO off;
SET enable_mergejoin TO off;
SET enable_material TO off;
SET enable_memoize TO off;
SET max_parallel_workers_per_gather TO 0;

-- the join parameter on the hash partitioned column is the only qual
SELECT exclusion_info($$
  SELECT count(*) FROM (VALUES (1::bigint), (2)) v(device_id),
  LATERAL (SELECT 1 FROM metric m WHERE m.device_id = v.device_id) x
$$);

SELECT count(*) FROM (VALUES (1::bigint), (2)) v(device_id),
LATERAL (SELECT 1 FROM metric m WHERE m.device_id = v.device_id) x;

SELECT count(*) FROM metric WHERE device_id IN (1, 2);

-- join parameters on the hash and on the range dimension
SELECT exclusion_info($$
  SELECT count(*) FROM
    (VALUES (1::bigint, '2025-03-01'::timestamptz), (2, '2025-02-01'::timestamptz)) v(device_id, time),
  LATERAL (SELECT 1 FROM metric m WHERE m.device_id = v.device_id AND m.time >= v.time) x
$$);

SELECT count(*) FROM
  (VALUES (1::bigint, '2025-03-01'::timestamptz), (2, '2025-02-01'::timestamptz)) v(device_id, time),
LATERAL (SELECT 1 FROM metric m WHERE m.device_id = v.device_id AND m.time >= v.time) x;

SELECT count(*) FROM metric WHERE device_id = 1 AND time >= '2025-03-01';

SELECT count(*) FROM metric WHERE device_id = 2 AND time >= '2025-02-01';

-- the outer value has a different type than the partitioning column
SELECT exclusion_info($$
  SELECT count(*) FROM (VALUES (1), (2)) v(device_id),
  LATERAL (SELECT 1 FROM metric m WHERE m.device_id = v.device_id) x
$$);

SELECT count(*) FROM (VALUES (1), (2)) v(device_id),
LATERAL (SELECT 1 FROM metric m WHERE m.device_id = v.device_id) x;

-- the partitioning column on the other side of the comparison
SELECT exclusion_info($$
  SELECT count(*) FROM (VALUES (1), (2)) v(device_id),
  LATERAL (SELECT 1 FROM metric m WHERE v.device_id = m.device_id) x
$$);

SELECT count(*) FROM (VALUES (1), (2)) v(device_id),
LATERAL (SELECT 1 FROM metric m WHERE v.device_id = m.device_id) x;

-- a NULL parameter must not exclude rows for the other values
SELECT count(*) FROM (VALUES (1::bigint), (NULL)) v(device_id),
LATERAL (SELECT 1 FROM metric m WHERE m.device_id = v.device_id) x;

-- no ChunkAppend when the join parameter is not on a partitioning column
SELECT exclusion_info($$
  SELECT count(*) FROM (VALUES (1.0::float), (2.0)) v(value),
  LATERAL (SELECT 1 FROM metric m WHERE m.value = v.value) x
$$);

-- no ChunkAppend for a comparison on the partitioning column that can never
-- contradict the constraints of a chunk
SELECT exclusion_info($$
  SELECT count(*) FROM
    (VALUES (1.0::float, '2025-02-01'::timestamptz), (2.0, '2025-03-01'::timestamptz)) v(value, time),
  LATERAL (SELECT 1 FROM metric m WHERE m.value = v.value AND m.time <> v.time) x
$$);

-- text partitioning column
CREATE TABLE metric_text(time timestamptz NOT NULL, device text NOT NULL);
SELECT create_hypertable('metric_text', by_range('time', INTERVAL '30 days'));
SELECT add_dimension('metric_text', by_hash('device', 4));
CREATE INDEX ON metric_text(device, time);
INSERT INTO metric_text
SELECT '2025-01-01'::timestamptz + i * INTERVAL '6 hours', 'device-' || d
FROM generate_series(0, 359) i, generate_series(1, 8) d;
ANALYZE metric_text;

SELECT exclusion_info($$
  SELECT count(*) FROM (VALUES ('device-1'), ('device-2')) v(device),
  LATERAL (SELECT 1 FROM metric_text m WHERE m.device = v.device) x
$$);

SELECT count(*) FROM (VALUES ('device-1'), ('device-2')) v(device),
LATERAL (SELECT 1 FROM metric_text m WHERE m.device = v.device) x;

SELECT count(*) FROM metric_text WHERE device IN ('device-1', 'device-2');

-- no ChunkAppend for an ANY comparison on the hash partitioned column, the
-- clause on the partition hash is only built for a plain equality
SELECT exclusion_info($$
  SELECT count(*) FROM (VALUES (ARRAY[1::bigint, 2])) v(device_ids),
  LATERAL (SELECT 1 FROM metric m WHERE m.device_id = ANY (v.device_ids)) x
$$);

SELECT count(*) FROM (VALUES (ARRAY[1::bigint, 2])) v(device_ids),
LATERAL (SELECT 1 FROM metric m WHERE m.device_id = ANY (v.device_ids)) x;

-- no ChunkAppend for an inequality on the hash partitioned column
SELECT exclusion_info($$
  SELECT count(*) FROM (VALUES (1::bigint), (2)) v(device_id),
  LATERAL (SELECT 1 FROM metric m WHERE m.device_id > v.device_id) x
$$);

SELECT count(*) FROM (VALUES (1::bigint), (2)) v(device_id),
LATERAL (SELECT 1 FROM metric m WHERE m.device_id > v.device_id) x;

-- varchar partitioning column, the comparison carries a relabel to text
CREATE TABLE metric_varchar(time timestamptz NOT NULL, device varchar(32) NOT NULL);
SELECT create_hypertable('metric_varchar', by_range('time', INTERVAL '30 days'));
SELECT add_dimension('metric_varchar', by_hash('device', 4));
CREATE INDEX ON metric_varchar(device, time);
INSERT INTO metric_varchar
SELECT '2025-01-01'::timestamptz + i * INTERVAL '6 hours', 'device-' || d
FROM generate_series(0, 359) i, generate_series(1, 8) d;
ANALYZE metric_varchar;

SELECT exclusion_info($$
  SELECT count(*) FROM (VALUES ('device-1'::varchar), ('device-2')) v(device),
  LATERAL (SELECT 1 FROM metric_varchar m WHERE m.device = v.device) x
$$);

SELECT count(*) FROM (VALUES ('device-1'::varchar), ('device-2')) v(device),
LATERAL (SELECT 1 FROM metric_varchar m WHERE m.device = v.device) x;

SELECT count(*) FROM metric_varchar WHERE device IN ('device-1', 'device-2');

-- bpchar partitioning column
CREATE TABLE metric_bpchar(time timestamptz NOT NULL, device char(16) NOT NULL);
SELECT create_hypertable('metric_bpchar', by_range('time', INTERVAL '30 days'));
SELECT add_dimension('metric_bpchar', by_hash('device', 4));
CREATE INDEX ON metric_bpchar(device, time);
INSERT INTO metric_bpchar
SELECT '2025-01-01'::timestamptz + i * INTERVAL '6 hours', 'device-' || d
FROM generate_series(0, 359) i, generate_series(1, 8) d;
ANALYZE metric_bpchar;

SELECT exclusion_info($$
  SELECT count(*) FROM (VALUES ('device-1'::char(16)), ('device-2')) v(device),
  LATERAL (SELECT 1 FROM metric_bpchar m WHERE m.device = v.device) x
$$);

SELECT count(*) FROM (VALUES ('device-1'::char(16)), ('device-2')) v(device),
LATERAL (SELECT 1 FROM metric_bpchar m WHERE m.device = v.device) x;

SELECT count(*) FROM metric_bpchar WHERE device IN ('device-1', 'device-2');

RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_material;
RESET enable_memoize;
RESET max_parallel_workers_per_gather;

DROP FUNCTION exclusion_info;
DROP TABLE metric;
DROP TABLE metric_text;
DROP TABLE metric_varchar;
DROP TABLE metric_bpchar;
