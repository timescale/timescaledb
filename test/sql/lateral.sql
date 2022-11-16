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
