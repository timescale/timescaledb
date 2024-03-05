-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SET timescaledb.enable_chunk_append TO false;
SET timescaledb.enable_constraint_aware_append TO false;
SET timescaledb.current_timestamp_mock TO '1990-01-01';

\set PREFIX 'EXPLAIN (COSTS OFF, SUMMARY OFF, TIMING OFF)'


-- test SELECT FOR UPDATE
:PREFIX SELECT FROM metrics_space WHERE device_id = 1 FOR UPDATE;
:PREFIX SELECT FROM metrics_space WHERE device_id IN (1) FOR UPDATE;
:PREFIX SELECT FROM metrics_space WHERE device_id IN (1,3) FOR UPDATE;

-- check mismatching datatypes
:PREFIX SELECT FROM metrics_space WHERE device_id = smallint '2' FOR UPDATE;
:PREFIX SELECT FROM metrics_space WHERE device_id = int '2' FOR UPDATE;
:PREFIX SELECT FROM metrics_space WHERE device_id = bigint '3' FOR UPDATE;
:PREFIX SELECT FROM metrics_space WHERE device_id IN (smallint '1', smallint '1') FOR UPDATE;
:PREFIX SELECT FROM metrics_space WHERE device_id IN (int '1', int '1') FOR UPDATE;
:PREFIX SELECT FROM metrics_space WHERE device_id IN (bigint '1', bigint '1') FOR UPDATE;

-- test valid variants we are optimizing
:PREFIX DELETE FROM metrics_space WHERE device_id = 1;
:PREFIX DELETE FROM metrics_space WHERE device_id IN (1);
:PREFIX DELETE FROM metrics_space WHERE device_id IN (1,1);
:PREFIX DELETE FROM metrics_space WHERE device_id IN (1,3);

-- test multiple constraints
:PREFIX DELETE FROM metrics_space WHERE device_id = 1 AND device_id = 1;
:PREFIX DELETE FROM metrics_space WHERE device_id = 1 AND device_id = 2;
:PREFIX DELETE FROM metrics_space WHERE device_id = 1 OR device_id = 2;
:PREFIX DELETE FROM metrics_space WHERE device_id IN (1) OR device_id IN (2);
:PREFIX DELETE FROM metrics_space WHERE device_id IN (1) OR device_id = 2;
:PREFIX DELETE FROM metrics_space WHERE (time > '2000-01-01' OR device_id = 1) OR (time < '3000-01-01' OR device_id = 2);

-- variants we don't optimize
:PREFIX DELETE FROM metrics_space WHERE device_id > 1;
:PREFIX DELETE FROM metrics_space WHERE device_id < 10;

-- CTE
:PREFIX WITH q1 AS (
  DELETE FROM metrics_space WHERE device_id IN (1,3) RETURNING device_id
) SELECT FROM q1;

:PREFIX WITH q1 AS (
  DELETE FROM metrics_space WHERE device_id = 2 RETURNING device_id
) DELETE FROM metrics_space WHERE device_id IN (1,3) RETURNING device_id;

-- JOIN
:PREFIX DELETE FROM metrics_space m1 using metrics_space m2 WHERE m1.device_id = 1 AND m2.device_id = 2;
:PREFIX UPDATE metrics_space m1 set v0 = 0.1 FROM metrics_space m2 WHERE m2.device_id=1 AND m1.device_id=2;

-- test multiple space dimensions and different datatypes
CREATE TABLE space_constraint(time timestamptz, s1 text, s2 numeric, s3 int, v float);
SELECT table_name FROM create_hypertable('space_constraint','time');
SELECT column_name FROM add_dimension('space_constraint','s1',number_partitions:=3);
SELECT column_name FROM add_dimension('space_constraint','s2',number_partitions:=3);
SELECT column_name FROM add_dimension('space_constraint','s3',number_partitions:=3);

INSERT INTO space_constraint
SELECT t,s1,s2,s3,0.12345 FROM
  (VALUES ('2000-01-01'::timestamptz),('2001-01-01')) v1(t),
  (VALUES ('s1_1'),('s1_2')) v2(s1),
  (VALUES (1.23),(4.56)) v3(s2),
  (VALUES (1),(2)) v4(s3);

\set PREFIX 'EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)'

BEGIN;
:PREFIX DELETE FROM space_constraint WHERE s1 = 's1_2';
ROLLBACK;
BEGIN;
:PREFIX DELETE FROM space_constraint WHERE s1 = 's1_2' AND s2 = 1.23 AND s3 = 2;
ROLLBACK;
BEGIN;
:PREFIX DELETE FROM space_constraint WHERE time > '2000-06-01' AND s1 = 's1_2' AND s2 = 1.23 AND s3 = 2;
ROLLBACK;
BEGIN;
:PREFIX DELETE FROM space_constraint WHERE s1 IN ('s1_2','s1_2') AND s2 = 1.23 AND s3 = 2;
ROLLBACK;
BEGIN;
:PREFIX DELETE FROM space_constraint WHERE s1 IN ('s1_1','s1_3') AND s2 IN (1.23,4.44) AND s3 IN (1,100);
ROLLBACK;

BEGIN;
:PREFIX UPDATE space_constraint SET v=0.7 WHERE s1 = 's1_2';
ROLLBACK;
BEGIN;
:PREFIX UPDATE space_constraint SET v=0.7 WHERE s1 = 's1_2' AND s2 = 1.23 AND s3 = 2;
ROLLBACK;
BEGIN;
:PREFIX UPDATE space_constraint SET v=0.7 WHERE time > '2000-06-01' AND s1 = 's1_2' AND s2 = 1.23 AND s3 = 2;
ROLLBACK;
BEGIN;
:PREFIX UPDATE space_constraint SET v=0.7 WHERE s1 IN ('s1_2','s1_2') AND s2 = 1.23 AND s3 = 2;
ROLLBACK;
BEGIN;
:PREFIX UPDATE space_constraint SET v=0.7 WHERE s1 IN ('s1_1','s1_3') AND s2 IN (1.23,4.44) AND s3 IN (1,100);
ROLLBACK;

DROP TABLE space_constraint;
