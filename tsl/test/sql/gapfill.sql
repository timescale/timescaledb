-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE TABLE metrics_int(time int,device_id int, sensor_id int, value float);

INSERT INTO metrics_int VALUES
(-100,1,1,0.0),
(-100,1,2,-100.0),
(0,1,1,5.0),
(5,1,2,10.0),
(100,1,1,0.0),
(100,1,2,-100.0)
;

CREATE TABLE devices(device_id INT, name TEXT);
INSERT INTO devices VALUES (1,'Device 1'),(2,'Device 2'),(3,'Device 3');

CREATE TABLE sensors(sensor_id INT, name TEXT);
INSERT INTO sensors VALUES (1,'Sensor 1'),(2,'Sensor 2'),(3,'Sensor 3');

-- test locf and interpolate call without gapfill
SELECT locf(1);
SELECT interpolate(1);
-- test locf and interpolate call with NULL input
SELECT locf(NULL::int);
SELECT interpolate(NULL);

\set ON_ERROR_STOP 0
-- test time_bucket_gapfill not top level function call
SELECT
  1 + time_bucket_gapfill(1,time,1,11)
FROM (VALUES (1),(2)) v(time)
GROUP BY 1;

-- test locf with treat_null_as_missing not BOOL
SELECT
  time_bucket_gapfill(1,time,1,11),
  locf(min(time),treat_null_as_missing:=1)
FROM (VALUES (1),(2)) v(time)
GROUP BY 1;

-- test locf with treat_null_as_missing not literal
SELECT
  time_bucket_gapfill(1,time,1,11),
  locf(min(time),treat_null_as_missing:=random()>0)
FROM (VALUES (1),(2)) v(time)
GROUP BY 1;

-- test interpolate lookup query with 1 element in record
SELECT
  time_bucket_gapfill(1,time,1,11),
  interpolate(min(time),next=>(SELECT ROW(10)))
FROM (VALUES (1),(2)) v(time)
GROUP BY 1;

SELECT
  time_bucket_gapfill(1,time,1,11),
  interpolate(min(time),prev=>(SELECT ROW(10)))
FROM (VALUES (2),(3)) v(time)
GROUP BY 1;

-- test interpolate lookup query with 3 elements in record
SELECT
  time_bucket_gapfill(1,time,1,11),
  interpolate(min(time),next=>(SELECT (10,10,10)))
FROM (VALUES (1),(2)) v(time)
GROUP BY 1;

SELECT
  time_bucket_gapfill(1,time,1,11),
  interpolate(min(time),prev=>(SELECT (10,10,10)))
FROM (VALUES (2),(3)) v(time)
GROUP BY 1;

-- test interpolate lookup query with mismatching time datatype
SELECT
  time_bucket_gapfill(1,time,1,11),
  interpolate(min(time),next=>(SELECT (10::float,10)))
FROM (VALUES (1),(2)) v(time)
GROUP BY 1;

SELECT
  time_bucket_gapfill(1,time,1,11),
  interpolate(min(time),prev=>(SELECT (10::float,10)))
FROM (VALUES (2),(3)) v(time)
GROUP BY 1;

-- test interpolate lookup query with mismatching value datatype
SELECT
  time_bucket_gapfill(1,time,1,11),
  interpolate(min(time),next=>(SELECT (10,10::float)))
FROM (VALUES (1),(2)) v(time)
GROUP BY 1;

SELECT
  time_bucket_gapfill(1,time,1,11),
  interpolate(min(time),prev=>(SELECT (10,10::float)))
FROM (VALUES (2),(3)) v(time)
GROUP BY 1;

-- test interpolate with unsupported datatype
SELECT
  time_bucket_gapfill(1,time,1,11),
  interpolate(text 'text')
FROM (VALUES (1),(2)) v(time)
GROUP BY 1;

SELECT
  time_bucket_gapfill(1,time,1,11),
  interpolate(interval '1d')
FROM (VALUES (2),(3)) v(time)
GROUP BY 1;

-- test multiple time_bucket_gapfill calls
SELECT
  time_bucket_gapfill(1,time,1,11),time_bucket_gapfill(1,time,1,11)
FROM (VALUES (1),(2)) v(time)
GROUP BY 1;

-- test nested time_bucket_gapfill calls
SELECT
  time_bucket_gapfill(1,time_bucket_gapfill(1,time,1,11),1,11)
FROM (VALUES (1),(2)) v(time)
GROUP BY 1;

-- test nested locf calls
SELECT
  time_bucket_gapfill(1,time,1,11),
  locf(locf(min(time)))
FROM (VALUES (1),(2)) v(time)
GROUP BY 1;

-- test nested interpolate calls
SELECT
  time_bucket_gapfill(1,time,1,11),
  interpolate(interpolate(min(time)))
FROM (VALUES (1),(2)) v(time)
GROUP BY 1;

-- test mixed locf/interpolate calls
SELECT
  time_bucket_gapfill(1,time,1,11),
  locf(interpolate(min(time)))
FROM (VALUES (1),(2)) v(time)
GROUP BY 1;

-- test window function inside locf
SELECT
  time_bucket_gapfill(1,time,1,11),
  locf(avg(min(time)) OVER ())
FROM (VALUES (1),(2)) v(time)
GROUP BY 1;

-- test nested window functions
-- prevented by postgres
SELECT
  time_bucket_gapfill(1,time,1,11),
  avg(avg(min(time)) OVER ()) OVER ()
FROM (VALUES (1),(2)) v(time)
GROUP BY 1;

-- test multiple window functions in single column
SELECT
  time_bucket_gapfill(1,time,1,11),
  avg(min(time)) OVER () + avg(min(time)) OVER ()
FROM (VALUES (1),(2)) v(time)
GROUP BY 1;

-- test window functions with multiple column references
SELECT
  time_bucket_gapfill(1,time,1,2),
  first(min(time),min(time)) OVER ()
FROM metrics_int
GROUP BY 1;

-- test locf not toplevel
SELECT
  time_bucket_gapfill(1,time,1,11),
  1 + locf(min(time))
FROM (VALUES (1),(2)) v(time)
GROUP BY 1;

-- test locf inside aggregate
SELECT
  time_bucket_gapfill(1,time,1,11),
  min(min(locf(time))) OVER ()
FROM (VALUES (1),(2)) v(time)
GROUP BY 1;

-- test NULL args
SELECT
  time_bucket_gapfill(NULL,time,1,11)
FROM (VALUES (1),(2)) v(time)
GROUP BY 1;

SELECT
  time_bucket_gapfill(1,NULL,1,11)
FROM (VALUES (1),(2)) v(time)
GROUP BY 1;

SELECT
  time_bucket_gapfill(1,time,NULL,11)
FROM (VALUES (1),(2)) v(time)
GROUP BY 1;

SELECT
  time_bucket_gapfill(1,time,1,NULL)
FROM (VALUES (1),(2)) v(time)
GROUP BY 1;

-- test 0 bucket_width
SELECT
  time_bucket_gapfill(0,time,1,11)
FROM (VALUES (1),(2)) v(time)
GROUP BY 1;

SELECT
  time_bucket_gapfill('0d',time,'2000-01-01','2000-02-01')
FROM (VALUES ('2000-01-01'::date),('2000-02-01'::date)) v(time)
GROUP BY 1;

SELECT
  time_bucket_gapfill('0d',time,'2000-01-01','2000-02-01')
FROM (VALUES ('2000-01-01'::timestamptz),('2000-02-01'::timestamptz)) v(time)
GROUP BY 1;

-- test negative bucket_width
SELECT
  time_bucket_gapfill(-1,time,1,11)
FROM (VALUES (1),(2)) v(time)
GROUP BY 1;

SELECT
  time_bucket_gapfill('-1d',time,'2000-01-01','2000-02-01')
FROM (VALUES ('2000-01-01'::date),('2000-02-01'::date)) v(time)
GROUP BY 1;

SELECT
  time_bucket_gapfill('-1d',time,'2000-01-01','2000-02-01')
FROM (VALUES ('2000-01-01'::timestamptz),('2000-02-01'::timestamptz)) v(time)
GROUP BY 1;

-- test subqueries as interval, start and stop (not supported atm)
SELECT
  time_bucket_gapfill((SELECT 1),time,1,11)
FROM (VALUES (1),(2)) v(time)
GROUP BY 1;

SELECT
  time_bucket_gapfill(1,time,(SELECT 1),11)
FROM (VALUES (1),(2)) v(time)
GROUP BY 1;

SELECT
  time_bucket_gapfill(1,time,1,(SELECT 11))
FROM (VALUES (1),(2)) v(time)
GROUP BY 1;


\set ON_ERROR_STOP 1

-- test time_bucket_gapfill without aggregation
-- this will not trigger gapfilling
SELECT
  time_bucket_gapfill(1,time,1,11)
FROM (VALUES (1),(2)) v(time);

SELECT
  time_bucket_gapfill(1,time,1,11),
  avg(time) OVER ()
FROM (VALUES (1),(2)) v(time);

-- test int int2/4/8
SELECT
  time_bucket_gapfill(1::int2,time::int2,0::int2,6::int2)
FROM (VALUES (1),(4)) v(time)
GROUP BY 1;

SELECT
  time_bucket_gapfill(1::int4,time::int4,0::int4,6::int4)
FROM (VALUES (1),(4)) v(time)
GROUP BY 1;

SELECT
  time_bucket_gapfill(1::int8,time::int8,0::int8,6::int8)
FROM (VALUES (1),(4)) v(time)
GROUP BY 1;

-- test non-aligned bucket start
SELECT
  time_bucket_gapfill(10,time,5,40)
FROM (VALUES (11),(22)) v(time)
GROUP BY 1;

-- simple gapfill query
SELECT
  time_bucket_gapfill(10,time,0,50) AS time,
  min(value) AS value
FROM (values (-10,1),(10,2),(11,3),(12,4),(22,5),(30,6),(66,7)) v(time,value)
GROUP BY 1 ORDER BY 1;

-- test references to different columns
SELECT
  time_bucket_gapfill(1,t,0,5) as t,
  min(t),max(t),min(v),max(v)
FROM(VALUES (1,3),(2,5)) tb(t,v)
GROUP BY 1 ORDER BY 1;

-- test passing of values outside boundaries
SELECT
  time_bucket_gapfill(1,time,0,5),
  min(time)
FROM (VALUES (-1),(1),(3),(6)) v(time)
GROUP BY 1 ORDER BY 1;

-- test gap fill before first row and after last row
SELECT
  time_bucket_gapfill(1,time,0,5),
  min(time)
FROM (VALUES (1),(2),(3)) v(time)
GROUP BY 1 ORDER BY 1;

-- test gap fill without rows in resultset
SELECT
  time_bucket_gapfill(1,time,0,5),
  min(time)
FROM (VALUES (1),(2),(3)) v(time)
WHERE false
GROUP BY 1 ORDER BY 1;

-- test coalesce
SELECT
  time_bucket_gapfill(1,time,0,5),
  coalesce(min(time),0),
  coalesce(min(value),0),
  coalesce(min(value),7)
FROM (VALUES (1,1),(2,2),(3,3)) v(time,value)
GROUP BY 1 ORDER BY 1;

-- test case
SELECT
  time_bucket_gapfill(1,time,0,5),
  min(time),
  CASE WHEN min(time) IS NOT NULL THEN min(time) ELSE -1 END,
  CASE WHEN min(time) IS NOT NULL THEN min(time) + 7 ELSE 0 END,
  CASE WHEN 1 = 1 THEN 1 ELSE 0 END
FROM (VALUES (1,1),(2,2),(3,3)) v(time,value)
GROUP BY 1 ORDER BY 1;

-- test constants
SELECT
  time_bucket_gapfill(1,time,0,5),
  min(time), min(time), 4 as c
FROM (VALUES (1),(2),(3)) v(time)
GROUP BY 1 ORDER BY 1;

-- test column reordering
SELECT
  1 as c1, '2' as c2,
  time_bucket_gapfill(1,time,0,5),
  3.0 as c3,
  min(time), min(time), 4 as c4
FROM (VALUES (1),(2),(3)) v(time)
GROUP BY 3 ORDER BY 3;

-- test timestamptz
SELECT
  time_bucket_gapfill(INTERVAL '6h',time,TIMESTAMPTZ '2000-01-01',TIMESTAMPTZ '2000-01-02'),
  min(time)
FROM (VALUES (TIMESTAMPTZ '2000-01-01 9:00:00'),(TIMESTAMPTZ '2000-01-01 18:00:00')) v(time)
GROUP BY 1 ORDER BY 1;

-- test timestamp
SELECT
  time_bucket_gapfill(INTERVAL '6h',time,TIMESTAMP '2000-01-01',TIMESTAMP '2000-01-02'),
  min(time)
FROM (VALUES (TIMESTAMP '2000-01-01 9:00:00'),(TIMESTAMP '2000-01-01 18:00:00')) v(time)
GROUP BY 1 ORDER BY 1;

-- test date
SELECT
  time_bucket_gapfill(INTERVAL '1w',time,DATE '2000-01-01',DATE '2000-02-10'),
  min(time)
FROM (VALUES (DATE '2000-01-08'),(DATE '2000-01-22')) v(time)
GROUP BY 1 ORDER BY 1;

-- test grouping by non-time columns
SELECT
  time_bucket_gapfill(1,time,0,5) as time,
  id,
  min(value) as m
FROM (VALUES (1,1,1),(2,2,2)) v(time,id,value)
GROUP BY 1,id ORDER BY 2,1;

-- test grouping by non-time columns with no rows in resultset
SELECT
  time_bucket_gapfill(1,time,0,5) as time,
  id,
  min(value) as m
FROM (VALUES (1,1,1),(2,2,2)) v(time,id,value)
WHERE false
GROUP BY 1,id ORDER BY 2,1;

-- test duplicate columns in GROUP BY
SELECT
  time_bucket_gapfill(1,time,0,5) as time,
  id,
  id,
  min(value) as m
FROM (VALUES (1,1,1),(2,2,2)) v(time,id,value)
GROUP BY 1,2,3 ORDER BY 2,1;

-- test grouping by columns not in resultset
SELECT
  time_bucket_gapfill(1,time,0,5) as time,
  min(value) as m
FROM (VALUES (1,1,1),(2,2,2)) v(time,id,value)
GROUP BY 1,id ORDER BY id,1;

-- test grouping by non-time columns with text columns
SELECT
  time_bucket_gapfill(1,time,0,5) as time,
  color,
  min(value) as m
FROM (VALUES (1,'blue',1),(2,'red',2)) v(time,color,value)
GROUP BY 1,color ORDER BY 2,1;

-- test grouping by non-time columns with text columns with no rows in resultset
SELECT
  time_bucket_gapfill(1,time,0,5) as time,
  color,
  min(value) as m
FROM (VALUES (1,'blue',1),(2,'red',2)) v(time,color,value)
WHERE false
GROUP BY 1,color ORDER BY 2,1;

-- test JOINs
SELECT
  time_bucket_gapfill(1,time,0,5) as time,
  device_id,
  d.name,
  sensor_id,
  s.name,
  avg(m.value)
FROM metrics_int m
INNER JOIN devices d USING(device_id)
INNER JOIN sensors s USING(sensor_id)
WHERE time BETWEEN 0 AND 5
GROUP BY 1,2,3,4,5;

-- test insert into SELECT
CREATE TABLE insert_test(id INT);
INSERT INTO insert_test SELECT time_bucket_gapfill(1,time,1,5) FROM (VALUES (1),(2)) v(time) GROUP BY 1 ORDER BY 1;
SELECT * FROM insert_test;

-- test join
SELECT t1.*,t2.m FROM
(
  SELECT
    time_bucket_gapfill(1,time,0,5) as time, color, min(value) as m
  FROM
    (VALUES (1,'red',1),(2,'blue',2)) v(time,color,value)
  GROUP BY 1,color ORDER BY 2,1
) t1 INNER JOIN
(
  SELECT
    time_bucket_gapfill(1,time,0,5) as time, color, min(value) as m
  FROM
    (VALUES (3,'red',1),(4,'blue',2)) v(time,color,value)
  GROUP BY 1,color ORDER BY 2,1
) t2 ON t1.time = t2.time AND t1.color=t2.color;

-- test join with locf
SELECT t1.*,t2.m FROM
(
  SELECT
    time_bucket_gapfill(1,time,0,5) as time,
    color,
    locf(min(value)) as locf
  FROM
    (VALUES (0,'red',1),(0,'blue',2)) v(time,color,value)
  GROUP BY 1,color ORDER BY 2,1
) t1 INNER JOIN
(
  SELECT
    time_bucket_gapfill(1,time,0,5) as time,
    color,
    locf(min(value)) as m
  FROM
    (VALUES (3,'red',1),(4,'blue',2)) v(time,color,value)
  GROUP BY 1,color ORDER BY 2,1
) t2 ON t1.time = t2.time AND t1.color=t2.color;

-- test locf
SELECT
  time_bucket_gapfill(10,time,0,50) AS time,
  locf(min(value)) AS value
FROM (values (10,9),(20,3),(50,6)) v(time,value)
GROUP BY 1 ORDER BY 1;

-- test locf with NULLs in resultset
SELECT
  time_bucket_gapfill(10,time,0,50) AS time,
  locf(min(value)) AS value
FROM (values (10,9),(20,3),(30,NULL),(50,6)) v(time,value)
GROUP BY 1 ORDER BY 1;

SELECT
  time_bucket_gapfill(10,time,0,50) AS time,
  locf(min(value),treat_null_as_missing:=false) AS value
FROM (values (10,9),(20,3),(30,NULL),(50,6)) v(time,value)
GROUP BY 1 ORDER BY 1;

SELECT
  time_bucket_gapfill(10,time,0,50) AS time,
  locf(min(value),treat_null_as_missing:=NULL) AS value
FROM (values (10,9),(20,3),(30,NULL),(50,6)) v(time,value)
GROUP BY 1 ORDER BY 1;

-- test locf with NULLs in resultset and treat_null_as_missing
SELECT
  time_bucket_gapfill(10,time,0,50) AS time,
  locf(min(value),treat_null_as_missing:=true) AS value
FROM (values (10,9),(20,3),(30,NULL),(50,6)) v(time,value)
GROUP BY 1 ORDER BY 1;

-- test locf with NULLs in first row of resultset and treat_null_as_missing with lookup query
SELECT
  time_bucket_gapfill(10,time,0,50) AS time,
  locf(min(value),treat_null_as_missing:=false, prev := (SELECT 100)) AS v1,
  locf(min(value),treat_null_as_missing:=true, prev := (SELECT 100)) AS v2
FROM (values (0,NULL),(30,NULL),(50,6)) v(time,value)
GROUP BY 1 ORDER BY 1;

-- test locf with NULLs in resultset and treat_null_as_missing with resort
SELECT
  time_bucket_gapfill(10,time,0,50) AS time,
  locf(min(value),treat_null_as_missing:=true) AS value
FROM (values (10,9),(20,3),(30,NULL),(50,6)) v(time,value)
GROUP BY 1 ORDER BY 1 DESC;

-- test locf with constants
SELECT
  time_bucket_gapfill(1,time,0,5),
  2,
  locf(min(value))
FROM (VALUES (0,1,3),(4,2,3)) v(time,value)
GROUP BY 1;

-- test expressions inside locf
SELECT
  time_bucket_gapfill(1,time,0,5),
  locf(min(value)),
  locf(4),
  locf(4 + min(value))
FROM (VALUES (0,1,3),(4,2,3)) v(time,value)
GROUP BY 1;

-- test locf with out of boundary lookup
SELECT
  time_bucket_gapfill(10,time,0,70) AS time,
  locf(min(value),(SELECT 100)) AS value
FROM (values (20,9),(40,6)) v(time,value)
GROUP BY 1 ORDER BY 1;

-- test locf with different datatypes
SELECT
  time_bucket_gapfill(1,time,0,5) as time,
  locf(min(v1)) AS text,
  locf(min(v2)) AS "int[]",
  locf(min(v3)) AS "text 4/8k"
FROM (VALUES
  (1,'foo',ARRAY[1,2,3],repeat('4k',2048)),
  (3,'bar',ARRAY[3,4,5],repeat('8k',4096))
) v(time,v1,v2,v3)
GROUP BY 1;

-- test locf with different datatypes and treat_null_as_missing
SELECT
  time_bucket_gapfill(1,time,0,5) as time,
  locf(min(v1),treat_null_as_missing:=true) AS text,
  locf(min(v2),treat_null_as_missing:=true) AS "int[]",
  locf(min(v3),treat_null_as_missing:=true) AS "text 4/8k"
FROM (VALUES
  (1,'foo',ARRAY[1,2,3],repeat('4k',2048)),
  (2,NULL,NULL,NULL),
  (3,'bar',ARRAY[3,4,5],repeat('8k',4096))
) v(time,v1,v2,v3)
GROUP BY 1;

-- test locf lookup query does not trigger when not needed
--
-- 1/(SELECT 0) will throw an error in the lookup query but in order to not
-- always trigger evaluation it needs to be correlated otherwise postgres will
-- always run it once even if the value is never used
SELECT
  time_bucket_gapfill(5,time,0,11) AS time,
  device_id,
  sensor_id,
  locf(min(value)::int,(SELECT 1/(SELECT 0) FROM metrics_int m2 WHERE m2.device_id=m1.device_id AND m2.sensor_id=m1.sensor_id ORDER BY time DESC LIMIT 1)) AS locf3
FROM metrics_int m1
WHERE time >= 0 AND time < 5
GROUP BY 1,2,3 ORDER BY 2,3,1;

\set ON_ERROR_STOP 0
-- inverse of previous test query to confirm an error is actually thrown
SELECT
  time_bucket_gapfill(5,time,0,11) AS time,
  device_id,
  sensor_id,
  locf(min(value)::int,(SELECT 1/(SELECT 0) FROM metrics_int m2 WHERE m2.device_id=m1.device_id AND m2.sensor_id=m1.sensor_id ORDER BY time DESC LIMIT 1)) AS locf3
FROM metrics_int m1
WHERE time = 5
GROUP BY 1,2,3 ORDER BY 2,3,1;
\set ON_ERROR_STOP 1

-- test locf with correlated subquery
SELECT
  time_bucket_gapfill(5,time,0,11) AS time,
  device_id,
  sensor_id,
  avg(value),
  locf(min(value)) AS locf,
  locf(min(value)::int,23) AS locf1,
  locf(min(value)::int,(SELECT 42)) AS locf2,
  locf(min(value),(SELECT value FROM metrics_int m2 WHERE time<0 AND m2.device_id=m1.device_id AND m2.sensor_id=m1.sensor_id ORDER BY time DESC LIMIT 1)) AS locf3
FROM metrics_int m1
WHERE time >= 0 AND time < 10
GROUP BY 1,2,3 ORDER BY 2,3,1;

-- test locf with correlated subquery and "wrong order"
SELECT
  time_bucket_gapfill(5,time,0,11) AS time,
  device_id,
  sensor_id,
  avg(value),
  locf(min(value)) AS locf,
  locf(min(value),23::float) AS locf1,
  locf(min(value),(SELECT 42::float)) AS locf2,
  locf(min(value),(SELECT value FROM metrics_int m2 WHERE time<0 AND m2.device_id=m1.device_id AND m2.sensor_id=m1.sensor_id ORDER BY time DESC LIMIT 1)) AS locf3
FROM metrics_int m1
WHERE time >= 0 AND time < 10
GROUP BY 1,2,3 ORDER BY 1,2,3;

-- test locf with correlated subquery and window functions
SELECT
  time_bucket_gapfill(5,time,0,11) AS time,
  device_id,
  sensor_id,
  locf(min(value),(SELECT value FROM metrics_int m2 WHERE time<0 AND m2.device_id=m1.device_id AND m2.sensor_id=m1.sensor_id ORDER BY time DESC LIMIT 1)),
  sum(locf(min(value),(SELECT value FROM metrics_int m2 WHERE time<0 AND m2.device_id=m1.device_id AND m2.sensor_id=m1.sensor_id ORDER BY time DESC LIMIT 1))) OVER (PARTITION BY device_id, sensor_id ROWS 1 PRECEDING)
FROM metrics_int m1
WHERE time >= 0 AND time < 10
GROUP BY 1,2,3;

-- test interpolate
SELECT
  time_bucket_gapfill(10,time,0,50) AS time,
  interpolate(min(value)) AS value
FROM (values (0,1),(50,6)) v(time,value)
GROUP BY 1 ORDER BY 1;

-- test interpolate with NULL values
SELECT
  time_bucket_gapfill(1,time,0,5) AS time,
  interpolate(avg(temp)) AS temp
FROM (VALUES (0,0),(2,NULL),(5,5)) v(time,temp)
GROUP BY 1;

-- test interpolate datatypes
SELECT
  time_bucket_gapfill(10,time,0,50) AS time,
  interpolate(min(v1)) AS "smallint",
  interpolate(min(v2)) AS "int",
  interpolate(min(v3)) AS "bigint",
  interpolate(min(v4)) AS "float4",
  interpolate(min(v5)) AS "float8"
FROM (values (0,-3::smallint,-3::int,-3::bigint,-3::float4,-3::float8),(50,3::smallint,3::int,3::bigint,3::float4,3::float8)) v(time,v1,v2,v3,v4,v5)
GROUP BY 1 ORDER BY 1;

-- test interpolate datatypes with negative time
SELECT
  time_bucket_gapfill(10,time,-40,30) AS time,
  interpolate(min(v1)) AS "smallint",
  interpolate(min(v2)) AS "int",
  interpolate(min(v3)) AS "bigint",
  interpolate(min(v4)) AS "float4",
  interpolate(min(v5)) AS "float8"
FROM (values (-40,-3::smallint,-3::int,-3::bigint,-3::float4,-3::float8),(20,3::smallint,3::int,3::bigint,3::float4,3::float8)) v(time,v1,v2,v3,v4,v5)
GROUP BY 1 ORDER BY 1;

-- test interpolate with multiple groupings
SELECT
  time_bucket_gapfill(5,time,0,11),
  device,
  interpolate(min(v1),(SELECT (-10,-10)),(SELECT (20,10)))
FROM (VALUES (5,1,0),(5,2,0)) as v(time,device,v1)
GROUP BY 1,2 ORDER BY 2,1;

-- test interpolate with correlated subquery
SELECT
  time_bucket_gapfill(5,time,0,11) AS time,
  device_id,
  sensor_id,
  avg(value),
  interpolate(min(value)) AS ip,
  interpolate(min(value),(-5,-5.0::float),(15,20.0::float)) AS ip1,
  interpolate(min(value),(SELECT (-10,-10.0::float)),(SELECT (15,20.0::float))) AS ip2,
  interpolate(
    min(value),
    (SELECT (time,value) FROM metrics_int m2
     WHERE time<0 AND m2.device_id=m1.device_id AND m2.sensor_id=m1.sensor_id
     ORDER BY time DESC LIMIT 1),
    (SELECT (time,value) FROM metrics_int m2
     WHERE time>10 AND m2.device_id=m1.device_id AND m2.sensor_id=m1.sensor_id
     ORDER BY time LIMIT 1)
  ) AS ip3
FROM metrics_int m1
WHERE time >= 0 AND time < 10
GROUP BY 1,2,3 ORDER BY 2,3,1;

-- test interpolate with correlated subquery and window function
SELECT
  time_bucket_gapfill(5,time,0,11) AS time,
  device_id,
  sensor_id,
  interpolate(
    min(value),
    (SELECT (time,value) FROM metrics_int m2
     WHERE time<0 AND m2.device_id=m1.device_id AND m2.sensor_id=m1.sensor_id
     ORDER BY time DESC LIMIT 1),
    (SELECT (time,value) FROM metrics_int m2
     WHERE time>10 AND m2.device_id=m1.device_id AND m2.sensor_id=m1.sensor_id
     ORDER BY time LIMIT 1)
  ),
  sum(interpolate(
    min(value),
    (SELECT (time,value) FROM metrics_int m2
     WHERE time<0 AND m2.device_id=m1.device_id AND m2.sensor_id=m1.sensor_id
     ORDER BY time DESC LIMIT 1),
    (SELECT (time,value) FROM metrics_int m2
     WHERE time>10 AND m2.device_id=m1.device_id AND m2.sensor_id=m1.sensor_id
     ORDER BY time LIMIT 1)
  )) OVER (PARTITION BY device_id, sensor_id ROWS 1 PRECEDING)
FROM metrics_int m1
WHERE time >= 0 AND time < 10
GROUP BY 1,2,3 ORDER BY 2,3,1;

-- test cte with gap filling in outer query
WITH data AS (
  SELECT * FROM (VALUES (1,1,1),(2,2,2)) v(time,id,value)
)
SELECT
  time_bucket_gapfill(1,time,0,5) as time,
  id,
  min(value) as m
FROM data
GROUP BY 1,id;

-- test cte with gap filling in inner query
WITH gapfill AS (
  SELECT
    time_bucket_gapfill(1,time,0,5) as time,
    id,
    min(value) as m
  FROM (VALUES (1,1,1),(2,2,2)) v(time,id,value)
  GROUP BY 1,id
)
SELECT * FROM gapfill;

-- test window functions
SELECT
  time_bucket_gapfill(10,time,0,60),
  interpolate(min(time)),
  lag(min(time)) OVER ()
FROM (VALUES (0),(50)) v(time)
GROUP BY 1;

-- test window functions with multiple windows
SELECT
  time_bucket_gapfill(1,time,0,10),
  interpolate(min(time)),
  row_number() OVER (),
  locf(min(time)),
  sum(interpolate(min(time))) OVER (ROWS 1 PRECEDING),
  sum(interpolate(min(time))) OVER (ROWS 2 PRECEDING),
  sum(interpolate(min(time))) OVER (ROWS 3 PRECEDING),
  sum(interpolate(min(time))) OVER (ROWS 4 PRECEDING)
FROM (VALUES (0),(9)) v(time)
GROUP BY 1;

-- test window functions with constants
SELECT
	time_bucket_gapfill(1,time,0,5),
  min(time),
  4 as c,
  lag(min(time)) OVER ()
FROM (VALUES (1),(2),(3)) v(time)
GROUP BY 1;

--test window functions with locf
SELECT
  time_bucket_gapfill(1,time,0,5),
  min(time) AS "min",
  lag(min(time)) over () AS lag_min,
  lead(min(time)) over () AS lead_min,
  locf(min(time)) AS locf,
  lag(locf(min(time))) over () AS lag_locf,
  lead(locf(min(time))) over () AS lead_locf
FROM (VALUES (1),(2)) v(time)
GROUP BY 1;

--test window functions with interpolate
SELECT
  time_bucket_gapfill(1,time,0,5),
  min(time) AS "min",
  lag(min(time)) over () AS lag_min,
  lead(min(time)) over () AS lead_min,
  interpolate(min(time)) AS interpolate,
  lag(interpolate(min(time))) over () AS lag_interpolate,
  lead(interpolate(min(time))) over () AS lead_interpolate
FROM (VALUES (1),(3)) v(time)
GROUP BY 1;

--test window functions with expressions
SELECT
  time_bucket_gapfill(1,time,0,5),
  min(time) AS "min",
  lag(min(time)) over () AS lag_min,
  1 + lag(min(time)) over () AS lag_min,
  interpolate(min(time)) AS interpolate,
  lag(interpolate(min(time))) over () AS lag_interpolate,
  1 + lag(interpolate(min(time))) over () AS lag_interpolate
FROM (VALUES (1),(3)) v(time)
GROUP BY 1;

--test row_number/rank/percent_rank/... window functions with gapfill reference
SELECT
  time_bucket_gapfill(1,time,0,5),
  ntile(2) OVER () AS ntile_2,
  ntile(3) OVER () AS ntile_3,
  ntile(5) OVER () AS ntile_5,
  row_number() OVER (),
  cume_dist() OVER (ORDER BY time_bucket_gapfill(1,time,0,5)),
  rank() OVER (),
  rank() OVER (ORDER BY time_bucket_gapfill(1,time,0,5)),
  percent_rank() OVER (ORDER BY time_bucket_gapfill(1,time,0,5))
FROM (VALUES (1),(3)) v(time)
GROUP BY 1;

-- test first_value/last_value/nth_value
SELECT
  time_bucket_gapfill(1,time,0,5),
  first_value(min(time)) OVER (),
  nth_value(min(time),3) OVER (),
  last_value(min(time)) OVER ()
FROM (VALUES (0),(2),(5)) v(time)
GROUP BY 1;

-- test window functions with PARTITION BY
SELECT
  time_bucket_gapfill(1,time,0,5) as time,
  color,
  row_number() OVER (),
  row_number() OVER (PARTITION BY color)
FROM (VALUES (1,'blue',1),(2,'red',2)) v(time,color,value)
GROUP BY 1,color ORDER BY 2,1;

-- test multiple windows
\set ON_ERROR_STOP 0
SELECT
  time_bucket_gapfill(1,time,0,11),
  first_value(interpolate(min(time))) OVER (ROWS 1 PRECEDING),
  interpolate(min(time)),
  last_value(interpolate(min(time))) OVER (ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
FROM (VALUES (0),(10)) v(time)
GROUP BY 1;

-- test reorder
SELECT
  time_bucket_gapfill(1,time,0,5) as time,
  id,
  min(value) as m
FROM
  (VALUES (1,1,1),(2,2,2)) v(time,id,value)
GROUP BY 1,id ORDER BY 1,id;

-- test order by locf
SELECT
  time_bucket_gapfill(1,time,1,6),
  locf(min(time))
FROM
  (VALUES (2),(3)) v(time)
GROUP BY 1 ORDER BY 1,2;

SELECT
  time_bucket_gapfill(1,time,1,6),
  locf(min(time))
FROM
  (VALUES (2),(3)) v(time)
GROUP BY 1 ORDER BY 2 NULLS FIRST,1;

SELECT
  time_bucket_gapfill(1,time,1,6),
  locf(min(time))
FROM
  (VALUES (2),(3)) v(time)
GROUP BY 1 ORDER BY 2 NULLS LAST,1;

-- test order by interpolate
SELECT
  time_bucket_gapfill(1,time,1,6),
  interpolate(min(time),prev:=(0,0)::record)
FROM
  (VALUES (2),(3)) v(time)
GROUP BY 1 ORDER BY 1,2;

SELECT
  time_bucket_gapfill(1,time,1,6),
  interpolate(min(time),prev:=(0,0)::record)
FROM
  (VALUES (2),(3)) v(time)
GROUP BY 1 ORDER BY 2 NULLS FIRST,1;

SELECT
  time_bucket_gapfill(1,time,1,6),
  interpolate(min(time),prev:=(0,0)::record)
FROM
  (VALUES (2),(3)) v(time)
GROUP BY 1 ORDER BY 2 NULLS LAST,1;

-- test queries on hypertable
CREATE TABLE metrics_tstz(time timestamptz, device_id INT, v1 float, v2 int);
SELECT create_hypertable('metrics_tstz','time');
INSERT INTO metrics_tstz VALUES
(timestamptz '2018-01-01 05:00:00 PST', 1, 0.5, 10),
(timestamptz '2018-01-01 05:00:00 PST', 2, 0.7, 20),
(timestamptz '2018-01-01 05:00:00 PST', 3, 0.9, 30),
(timestamptz '2018-01-01 07:00:00 PST', 1, 0.0, 0),
(timestamptz '2018-01-01 07:00:00 PST', 2, 1.4, 40),
(timestamptz '2018-01-01 07:00:00 PST', 3, 0.9, 30)
;

-- test locf and interpolate together
SELECT
  time_bucket_gapfill(interval '1h',time,timestamptz '2018-01-01 05:00:00-8', timestamptz '2018-01-01 07:00:00-8'),
  device_id,
  locf(avg(v1)) AS locf_v1,
  locf(min(v2)) AS locf_v2,
  interpolate(avg(v1)) AS interpolate_v1,
  interpolate(avg(v2)) AS interpolate_v2
FROM metrics_tstz
GROUP BY 1,2;

SELECT
  time_bucket_gapfill('12h'::interval,time,'2017-01-01'::timestamptz, '2017-01-02'::timestamptz),
  interpolate(
    avg(v1),
    (SELECT ('2017-01-01'::timestamptz,1::float)),
    (SELECT ('2017-01-02'::timestamptz,2::float))
  )
FROM metrics_tstz WHERE time < '2017-01-01' GROUP BY 1;

-- interpolation with correlated subquery lookup before interval
SELECT
  time_bucket_gapfill('1h'::interval,time,'2018-01-01 3:00 PST'::timestamptz, '2018-01-01 8:00 PST'::timestamptz),
  device_id,
  interpolate(
    avg(v1),
    (SELECT (time,0.5::float) FROM metrics_tstz m2 WHERE m1.device_id=m2.device_id ORDER BY time DESC LIMIT 1)
  ),
  avg(v1)
FROM metrics_tstz m1
WHERE device_id=1 GROUP BY 1,2;

-- interpolation with correlated subquery lookup after interval
SELECT
  time_bucket_gapfill('1h'::interval,time,'2018-01-01 5:00 PST'::timestamptz, '2018-01-01 9:00 PST'::timestamptz),
  device_id,
  interpolate(
    avg(v1),
    next=>(SELECT (time,v2::float) FROM metrics_tstz m2 WHERE m1.device_id=m2.device_id ORDER BY time LIMIT 1)
  ),avg(v1)
FROM metrics_tstz m1 WHERE device_id=1 GROUP BY 1,2;

\set ON_ERROR_STOP 0
-- bucket_width non simple expression
SELECT
  time_bucket_gapfill(t,t)
FROM (VALUES (1),(2)) v(t)
WHERE true AND true
GROUP BY 1;

-- no start/finish and no usable time constraints
SELECT
  time_bucket_gapfill(1,t)
FROM (VALUES (1),(2)) v(t)
WHERE true AND true
GROUP BY 1;

-- NULL start/finish and no usable time constraints
SELECT
  time_bucket_gapfill(1,t,NULL,NULL)
FROM (VALUES (1),(2)) v(t)
WHERE true AND true
GROUP BY 1;

-- no start and no usable time constraints
SELECT
  time_bucket_gapfill(1,t,finish:=1)
FROM (VALUES (1),(2)) v(t)
WHERE true AND true
GROUP BY 1;

-- NULL start expression and no usable time constraints
SELECT
  time_bucket_gapfill(1,t,CASE WHEN length(version())>0 THEN NULL::int ELSE NULL::int END,1)
FROM (VALUES (1),(2)) v(t)
WHERE true AND true
GROUP BY 1;

-- unsupported start expression and no usable time constraints
SELECT
  time_bucket_gapfill(1,t,t,1)
FROM (VALUES (1),(2)) v(t)
WHERE true AND true
GROUP BY 1;

-- NULL start and no usable time constraints
SELECT
  time_bucket_gapfill(1,t,NULL,1)
FROM (VALUES (1),(2)) v(t)
WHERE true AND true
GROUP BY 1;

-- NULL finish expression and no usable time constraints
SELECT
  time_bucket_gapfill(1,t,1,CASE WHEN length(version())>0 THEN NULL::int ELSE NULL::int END)
FROM (VALUES (1),(2)) v(t)
WHERE true AND true
GROUP BY 1;

-- unsupported finish expression and no usable time constraints
SELECT
  time_bucket_gapfill(1,t,1,t)
FROM (VALUES (1),(2)) v(t)
WHERE true AND true
GROUP BY 1;

-- no finish and no usable time constraints
SELECT
  time_bucket_gapfill(1,t,1)
FROM (VALUES (1),(2)) v(t)
WHERE true AND true
GROUP BY 1;

-- NULL finish and no usable time constraints
SELECT
  time_bucket_gapfill(1,t,1,NULL)
FROM (VALUES (1),(2)) v(t)
WHERE true AND true
GROUP BY 1;

-- expression with column reference on right side
SELECT
  time_bucket_gapfill(1,t)
FROM (VALUES (1),(2)) v(t)
WHERE t > t AND t < 2
GROUP BY 1;

-- expression with cast
SELECT
  time_bucket_gapfill(1,t1::int8)
FROM (VALUES (1,2),(2,2)) v(t1,t2)
WHERE t1 >= 1 AND t1 <= 2
GROUP BY 1;

-- expression with multiple column references
SELECT
  time_bucket_gapfill(1,t1+t2)
FROM (VALUES (1,2),(2,2)) v(t1,t2)
WHERE t1 > 1 AND t1 < 2
GROUP BY 1;

-- expression with NULL start in WHERE clause, we use CASE to wrap the NULL so it doesnt get folded
SELECT
  time_bucket_gapfill(1,t1)
FROM (VALUES (1,2),(2,2)) v(t1,t2)
WHERE t1 > CASE WHEN length(version()) > 0 THEN NULL::int ELSE NULL::int END AND t1 < 4
GROUP BY 1;

-- expression with NULL finish in WHERE clause, we use CASE to wrap the NULL so it doesnt get folded
SELECT
  time_bucket_gapfill(1,t1)
FROM (VALUES (1,2),(2,2)) v(t1,t2)
WHERE t1 > 0 AND t1 < CASE WHEN length(version()) > 0 THEN NULL::int ELSE NULL::int END
GROUP BY 1;

-- non-Const NULL as start argument, we use CASE to wrap the NULL so it doesnt get folded
SELECT
  time_bucket_gapfill(1,t1,CASE WHEN length(version())>0 THEN NULL::int ELSE NULL::int END)
FROM (VALUES (1,2),(2,2)) v(t1,t2)
WHERE t1 > 0 AND t1 < 2
GROUP BY 1;

-- non-Const NULL as finish argument, we use CASE to wrap the NULL so it doesnt get folded
SELECT
  time_bucket_gapfill(1,t1,NULL,CASE WHEN length(version())>0 THEN NULL::int ELSE NULL::int END)
FROM (VALUES (1,2),(2,2)) v(t1,t2)
WHERE t1 > 0 AND t1 < 2
GROUP BY 1;

-- test with unsupported operator
SELECT
  time_bucket_gapfill(1,time)
FROM metrics_int
WHERE time =0 AND time < 2
GROUP BY 1;

-- time_bucket_gapfill with constraints ORed
SELECT
 time_bucket_gapfill(1::int8,t::int8)
FROM (VALUES (1),(2)) v(t)
WHERE
 t >= -1 OR t < 3
GROUP BY 1;

-- test with 2 tables and where clause doesnt match gapfill argument
SELECT
  time_bucket_gapfill(1,m2.time)
FROM metrics_int m, metrics_int m2
WHERE m.time >=0 AND m.time < 2
GROUP BY 1;

-- test inner join and where clause doesnt match gapfill argument
SELECT
  time_bucket_gapfill(1,m2.time)
FROM metrics_int m1 INNER JOIN metrics_int m2 ON m1.time=m2.time
WHERE m1.time >=0 AND m1.time < 2
GROUP BY 1;

-- test inner join with constraints in join condition
-- only toplevel where clause constraints are supported atm
SELECT
  time_bucket_gapfill(1,m2.time)
FROM metrics_int m1 INNER JOIN metrics_int m2 ON m1.time=m2.time AND m2.time >=0 AND m2.time < 2
GROUP BY 1;

\set ON_ERROR_STOP 1

-- int32 time_bucket_gapfill with no start/finish
SELECT
  time_bucket_gapfill(1,t)
FROM (VALUES (1),(2)) v(t)
WHERE
  t >= -1 AND t < 3
GROUP BY 1;

-- same query with less or equal as finish
SELECT
  time_bucket_gapfill(1,t)
FROM (VALUES (1),(2)) v(t)
WHERE
  t >= -1 AND t <= 3
GROUP BY 1;

-- int32 time_bucket_gapfill with start column and value switched
SELECT
  time_bucket_gapfill(1,t)
FROM (VALUES (1),(2)) v(t)
WHERE
  -1 < t AND t < 3
GROUP BY 1;

-- int32 time_bucket_gapfill with finish column and value switched
SELECT
  time_bucket_gapfill(1,t)
FROM (VALUES (1),(2)) v(t)
WHERE
  t >= 0 AND 3 >= t
GROUP BY 1;

-- int16 time_bucket_gapfill with no start/finish
SELECT
  time_bucket_gapfill(1::int2,t)
FROM (VALUES (1::int2),(2::int2)) v(t)
WHERE
  t >= -1 AND t < 3
GROUP BY 1;

-- int64 time_bucket_gapfill with no start/finish
SELECT
  time_bucket_gapfill(1::int8,t)
FROM (VALUES (1::int8),(2::int8)) v(t)
WHERE
  t >= -1 AND t < 3
GROUP BY 1;

-- date time_bucket_gapfill with no start/finish
SELECT
  time_bucket_gapfill('1d'::interval,t)
FROM (VALUES ('1999-12-30'::date),('2000-01-01'::date)) v(t)
WHERE
  t >= '1999-12-29' AND t < '2000-01-03'
GROUP BY 1;

-- timestamp time_bucket_gapfill with no start/finish
SELECT
  time_bucket_gapfill('12h'::interval,t)
FROM (VALUES ('1999-12-30'::timestamp),('2000-01-01'::timestamp)) v(t)
WHERE
  t >= '1999-12-29' AND t < '2000-01-03'
GROUP BY 1;

-- timestamptz time_bucket_gapfill with no start/finish
SELECT
  time_bucket_gapfill('12h'::interval,t)
FROM (VALUES ('1999-12-30'::timestamptz),('2000-01-01'::timestamptz)) v(t)
WHERE
  t >= '1999-12-29' AND t < '2000-01-03'
GROUP BY 1;

-- timestamptz time_bucket_gapfill with more complex expression
SELECT
  time_bucket_gapfill('12h'::interval,t)
FROM (VALUES ('1999-12-30'::timestamptz),('2000-01-01'::timestamptz)) v(t)
WHERE
  t >= '2000-01-03'::timestamptz - '4d'::interval AND t < '2000-01-03'
GROUP BY 1;

-- timestamptz time_bucket_gapfill with different datatype in finish constraint
SELECT
  time_bucket_gapfill('12h'::interval,t)
FROM (VALUES ('1999-12-30'::timestamptz),('2000-01-01'::timestamptz)) v(t)
WHERE
  t >= '2000-01-03'::timestamptz - '4d'::interval AND t < '2000-01-03'::date
GROUP BY 1;

-- time_bucket_gapfill with now() as start
SELECT
 time_bucket_gapfill('1h'::interval,t)
FROM (VALUES (now()),(now())) v(t)
WHERE
 t >= now() AND t < now() - '1h'::interval
GROUP BY 1;

-- time_bucket_gapfill with multiple constraints
SELECT
 time_bucket_gapfill(1,t)
FROM (VALUES (1),(2)) v(t)
WHERE
 t >= -1 AND t < 3 and t>1 AND t <=4 AND length(version()) > 0
GROUP BY 1;

-- int32 time_bucket_gapfill with greater for start
SELECT
  time_bucket_gapfill(1,t)
FROM (VALUES (1),(2)) v(t)
WHERE
  t > -2 AND t < 3
GROUP BY 1;

-- test actual table
SELECT
  time_bucket_gapfill(1,time)
FROM metrics_int
WHERE time >=0 AND time < 2
GROUP BY 1;

-- test with table alias
SELECT
  time_bucket_gapfill(1,time)
FROM metrics_int m
WHERE m.time >=0 AND m.time < 2
GROUP BY 1;

-- test with 2 tables
SELECT
  time_bucket_gapfill(1,m.time)
FROM metrics_int m, metrics_int m2
WHERE m.time >=0 AND m.time < 2
GROUP BY 1;

-- test DISTINCT
SELECT DISTINCT ON (color)
  time_bucket_gapfill(1,time,0,5) as time,
  color,
  min(value) as m
FROM (VALUES (1,'blue',1),(2,'red',2)) v(time,color,value)
GROUP BY 1,color ORDER BY 2,1;

-- test DISTINCT with window functions
SELECT DISTINCT ON (row_number() OVER ())
  time_bucket_gapfill(1,time,0,5) as time,
  color,
  row_number() OVER ()
FROM (VALUES (1,'blue',1),(2,'red',2)) v(time,color,value)
GROUP BY 1,color;

-- test DISTINCT with window functions and PARTITION BY
SELECT DISTINCT ON (color,row_number() OVER (PARTITION BY color))
  time_bucket_gapfill(1,time,0,5) as time,
  color,
  row_number() OVER (PARTITION BY color)
FROM (VALUES (1,'blue',1),(2,'red',2)) v(time,color,value)
GROUP BY 1,color;

-- test DISTINCT with window functions not in targetlist
SELECT DISTINCT ON (row_number() OVER ())
  time_bucket_gapfill(1,time,0,5) as time,
  color,
  row_number() OVER (PARTITION BY color)
FROM (VALUES (1,'blue',1),(2,'red',2)) v(time,color,value)
GROUP BY 1,color;

-- test column references
SELECT
  row_number() OVER (PARTITION BY color),
  locf(min(time)),
  color,
  time_bucket_gapfill(1,time,0,5) as time
FROM (VALUES (1,'blue',1),(2,'red',2)) v(time,color,value)
GROUP BY 3,4;

-- test with Nested Loop
SELECT l.id, bucket, data_value FROM
    (VALUES (1), (2), (3), (4)) a(id)
    INNER JOIN LATERAL (
        SELECT b.id id, time_bucket_gapfill('1'::int, time, start=>'1'::int, finish=> '5'::int) bucket, locf(last(data, time)) data_value
        FROM (VALUES (1, 1, 1), (1, 4, 4), (2, 1, -1), (2, 4, -4)) b(id, time, data)
        WHERE a.id = b.id
        GROUP BY b.id, bucket
    ) as l on (true);

-- test prepared statement
PREPARE prep_gapfill AS
SELECT
  time_bucket_gapfill(1,time,0,5) as time,
  locf(min(value))
FROM (VALUES (1,1),(2,2)) v(time,value)
GROUP BY 1;

-- execute 10 times to make sure turning it into generic plan works
EXECUTE prep_gapfill;
EXECUTE prep_gapfill;
EXECUTE prep_gapfill;
EXECUTE prep_gapfill;
EXECUTE prep_gapfill;
EXECUTE prep_gapfill;
EXECUTE prep_gapfill;
EXECUTE prep_gapfill;
EXECUTE prep_gapfill;
EXECUTE prep_gapfill;

DEALLOCATE prep_gapfill;

-- test prepared statement with locf with lookup query
PREPARE prep_gapfill AS
SELECT
  time_bucket_gapfill(5,time,0,11) AS time,
  device_id,
  sensor_id,
  locf(min(value)::int,(SELECT 1/(SELECT 0) FROM metrics_int m2 WHERE m2.device_id=m1.device_id AND m2.sensor_id=m1.sensor_id ORDER BY time DESC LIMIT 1))
FROM metrics_int m1
WHERE time >= 0 AND time < 5
GROUP BY 1,2,3;

-- execute 10 times to make sure turning it into generic plan works
EXECUTE prep_gapfill;
EXECUTE prep_gapfill;
EXECUTE prep_gapfill;
EXECUTE prep_gapfill;
EXECUTE prep_gapfill;
EXECUTE prep_gapfill;
EXECUTE prep_gapfill;
EXECUTE prep_gapfill;
EXECUTE prep_gapfill;
EXECUTE prep_gapfill;

DEALLOCATE prep_gapfill;

-- test prepared statement with interpolate with lookup query
PREPARE prep_gapfill AS
SELECT
  time_bucket_gapfill(5,time,0,11) AS time,
  device_id,
  sensor_id,
  interpolate(
    min(value),
    (SELECT (time,value) FROM metrics_int m2
     WHERE time<0 AND m2.device_id=m1.device_id AND m2.sensor_id=m1.sensor_id
     ORDER BY time DESC LIMIT 1),
    (SELECT (time,value) FROM metrics_int m2
     WHERE time>10 AND m2.device_id=m1.device_id AND m2.sensor_id=m1.sensor_id
     ORDER BY time LIMIT 1)
  )
FROM metrics_int m1
WHERE time >= 0 AND time < 10
GROUP BY 1,2,3 ORDER BY 2,3,1;

-- execute 10 times to make sure turning it into generic plan works
EXECUTE prep_gapfill;
EXECUTE prep_gapfill;
EXECUTE prep_gapfill;
EXECUTE prep_gapfill;
EXECUTE prep_gapfill;
EXECUTE prep_gapfill;
EXECUTE prep_gapfill;
EXECUTE prep_gapfill;
EXECUTE prep_gapfill;
EXECUTE prep_gapfill;

DEALLOCATE prep_gapfill;

-- test prepared statement with variable gapfill arguments
PREPARE prep_gapfill(int,int,int) AS

SELECT
  time_bucket_gapfill($1,time,$2,$3) AS time,
  device_id,
  sensor_id,
  min(value)
FROM metrics_int m1
WHERE time >= $2 AND time < $3 AND device_id=1 AND sensor_id=1
GROUP BY 1,2,3 ORDER BY 2,3,1;

-- execute 10 times to make sure turning it into generic plan works
EXECUTE prep_gapfill(5,0,10);
EXECUTE prep_gapfill(4,100,110);
EXECUTE prep_gapfill(5,0,10);
EXECUTE prep_gapfill(4,100,110);
EXECUTE prep_gapfill(5,0,10);
EXECUTE prep_gapfill(4,100,110);
EXECUTE prep_gapfill(5,0,10);
EXECUTE prep_gapfill(4,100,110);
EXECUTE prep_gapfill(5,0,10);
EXECUTE prep_gapfill(4,100,110);

DEALLOCATE prep_gapfill;

-- test column references with TIME_COLUMN last
SELECT
  row_number() OVER (PARTITION BY color),
  locf(min(time)),
  color,
  time_bucket_gapfill(1,time,0,5) as time
FROM (VALUES (1,'blue',1),(2,'red',2)) v(time,color,value)
GROUP BY 3,4;

-- test expressions on GROUP BY columns
SELECT
  row_number() OVER (PARTITION BY color),
  locf(min(time)),
  color,
  length(color),
  time_bucket_gapfill(1,time,0,5) as time
FROM (VALUES (1,'blue',1),(2,'red',2)) v(time,color,value)
GROUP BY 3,5;

-- test columns derived from GROUP BY columns with cast
SELECT
  time_bucket_gapfill(1,time,0,5) as time,
  device_id::text
FROM (VALUES (1,1),(2,2)) v(time,device_id)
GROUP BY 1,device_id;

-- test columns derived from GROUP BY columns with expression
SELECT
  time_bucket_gapfill(1,time,0,5) as time,
  'Device ' || device_id::text
FROM (VALUES (1,1),(2,2)) v(time,device_id)
GROUP BY 1,device_id;




--test interpolation with big diifferences in values (test overflows in calculations)
--we use the biggest possible difference in time(x) and the value(y).
--For bigints we also test values of smaller than bigintmax/min to avoid
--the symmetry where x=y (which catches more errors)
SELECT  9223372036854775807 as big_int_max \gset
SELECT -9223372036854775808	 as big_int_min \gset

SELECT
  time_bucket_gapfill(1,time,0,1) AS time,
  interpolate(min(s)) AS "smallint",
  interpolate(min(i)) AS "int",
  interpolate(min(b)) AS "bigint",
  interpolate(min(b2)) AS "bigint2",
  interpolate(min(d)) AS "double"
FROM (values (:big_int_min,(-32768)::smallint,(-2147483648)::int,:big_int_min,-2147483648::bigint, '-Infinity'::double precision),
             (:big_int_max, 32767::smallint, 2147483647::int,:big_int_max, 2147483647::bigint, 'Infinity'::double precision)) v(time,s,i,b,b2,d)
GROUP BY 1 ORDER BY 1;
