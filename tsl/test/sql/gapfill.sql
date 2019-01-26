-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set ON_ERROR_STOP 0
-- test locf and interpolate call errors out when used outside gapfill context
SELECT locf(1);
SELECT interpolate(1);
-- test locf and interpolate call errors out when used outside gapfill context with NULL arguments
SELECT locf(NULL::int);
SELECT interpolate(NULL);

-- test time_bucket_gapfill not top level function call
SELECT
  1 + time_bucket_gapfill(1,time,1,11)
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
\set ON_ERROR_STOP 1

-- test time_bucket_gapfill without aggregation
SELECT
  time_bucket_gapfill(1,time,1,11)
FROM (VALUES (1),(2)) v(time);

-- test NULL args
\set ON_ERROR_STOP 0
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
\set ON_ERROR_STOP 1

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

-- test locf with constants
SELECT
  time_bucket_gapfill(1,time,0,5),
  2,
  locf(min(value))
FROM (VALUES (0,1,3),(4,2,3)) v(time,value)
GROUP BY 1;

-- test locf with out of boundary lookup
SELECT
  time_bucket_gapfill(10,time,0,70) AS time,
  locf(min(value),(SELECT 100)) AS value
FROM (values (20,9),(40,6)) v(time,value)
GROUP BY 1 ORDER BY 1;

CREATE TABLE metrics_int(time int,device_id int, sensor_id int, value float);

INSERT INTO metrics_int VALUES
(-100,1,1,0.0),
(-100,1,2,-100.0),
(0,1,1,5.0),
(5,1,2,10.0),
(100,1,1,0.0),
(100,1,2,-100.0)
;

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

-- test interpolate
SELECT
  time_bucket_gapfill(10,time,0,50) AS time,
  interpolate(min(value)) AS value
FROM (values (0,1),(50,6)) v(time,value)
GROUP BY 1 ORDER BY 1;

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

\set ON_ERROR_STOP 0
-- test window functions
SELECT
  time_bucket_gapfill(10,time,0,60),
  interpolate(min(time)),
  lag(min(time)) OVER ()
FROM (VALUES (0),(50)) v(time)
GROUP BY 1;

SELECT
	time_bucket_gapfill(1,time,0,5),
  min(time),
  4 as c,
  lag(min(time)) OVER ()
FROM (VALUES (1),(2),(3)) v(time)
GROUP BY 1;

SELECT
	time_bucket_gapfill(1,time,0,5),
  min(time),
  4 as c,
  lag(min(time)) OVER ()
FROM (VALUES (1),(3)) v(time)
GROUP BY 1;

SELECT
  time_bucket_gapfill(1,time,1,5),
  locf(min(time)) AS locf,
  lag(locf(min(time))) over () AS lag
FROM (VALUES (1),(2)) v(time)
GROUP BY 1;

SELECT
  time_bucket_gapfill(1,time,1,5),
  min(time) AS value,
  locf(min(time)) AS locf,
  lag(locf(min(time))) over () AS lag
FROM (VALUES (1),(2)) v(time)
GROUP BY 1;
\set ON_ERROR_STOP 1

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

-- int16 time_bucket_gapfill with no start/finish
SELECT
  time_bucket_gapfill(1::int2,t::int2)
FROM (VALUES (1),(2)) v(t)
WHERE
  t >= -1 AND t < 3
GROUP BY 1;

-- int64 time_bucket_gapfill with no start/finish
SELECT
  time_bucket_gapfill(1::int8,t::int8)
FROM (VALUES (1),(2)) v(t)
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

-- time_bucket_gapfill with multiple constraints
-- first one with matching operator wins
SELECT
 time_bucket_gapfill(1::int8,t::int8)
FROM (VALUES (1),(2)) v(t)
WHERE
 t >= -1 AND t < 3 and t>1 AND t <=4
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
