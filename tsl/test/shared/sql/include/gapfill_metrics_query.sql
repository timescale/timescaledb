-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- test locf lookup query does not trigger when not needed
--
-- 1/(SELECT 0) will throw an error in the lookup query but in order to not
-- always trigger evaluation it needs to be correlated otherwise postgres will
-- always run it once even if the value is never used
SELECT
  time_bucket_gapfill(5,time,0,11) AS time,
  device_id,
  sensor_id,
  locf(min(value)::int,(SELECT 1/(SELECT 0) FROM :METRICS m2 WHERE m2.device_id=m1.device_id AND m2.sensor_id=m1.sensor_id ORDER BY time DESC LIMIT 1)) AS locf3
FROM :METRICS m1
WHERE time >= 0 AND time < 5
GROUP BY 1,2,3 ORDER BY 2,3,1;

-- test locf with correlated subquery
SELECT
  time_bucket_gapfill(5,time,0,11) AS time,
  device_id,
  sensor_id,
  avg(value),
  locf(min(value)) AS locf,
  locf(min(value)::int,23) AS locf1,
  locf(min(value)::int,(SELECT 42)) AS locf2,
  locf(min(value),(SELECT value FROM :METRICS m2 WHERE time<0 AND m2.device_id=m1.device_id AND m2.sensor_id=m1.sensor_id ORDER BY time DESC LIMIT 1)) AS locf3
FROM :METRICS m1
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
  locf(min(value),(SELECT value FROM :METRICS m2 WHERE time<0 AND m2.device_id=m1.device_id AND m2.sensor_id=m1.sensor_id ORDER BY time DESC LIMIT 1)) AS locf3
FROM :METRICS m1
WHERE time >= 0 AND time < 10
GROUP BY 1,2,3 ORDER BY 1,2,3;

-- test locf with correlated subquery and window functions
SELECT
  time_bucket_gapfill(5,time,0,11) AS time,
  device_id,
  sensor_id,
  locf(min(value),(SELECT value FROM :METRICS m2 WHERE time<0 AND m2.device_id=m1.device_id AND m2.sensor_id=m1.sensor_id ORDER BY time DESC LIMIT 1)),
  sum(locf(min(value),(SELECT value FROM :METRICS m2 WHERE time<0 AND m2.device_id=m1.device_id AND m2.sensor_id=m1.sensor_id ORDER BY time DESC LIMIT 1))) OVER (PARTITION BY device_id, sensor_id ROWS 1 PRECEDING)
FROM :METRICS m1
WHERE time >= 0 AND time < 10
GROUP BY 1,2,3;

-- test JOINs
SELECT
  time_bucket_gapfill(1,time,0,5) as time,
  device_id,
  d.name,
  sensor_id,
  s.name,
  avg(m.value)
FROM :METRICS m
INNER JOIN devices d USING(device_id)
INNER JOIN sensors s USING(sensor_id)
WHERE time BETWEEN 0 AND 5
GROUP BY 1,2,3,4,5;

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
    (SELECT (time,value) FROM :METRICS m2
     WHERE time<0 AND m2.device_id=m1.device_id AND m2.sensor_id=m1.sensor_id
     ORDER BY time DESC LIMIT 1),
    (SELECT (time,value) FROM :METRICS m2
     WHERE time>10 AND m2.device_id=m1.device_id AND m2.sensor_id=m1.sensor_id
     ORDER BY time LIMIT 1)
  ) AS ip3
FROM :METRICS m1
WHERE time >= 0 AND time < 10
GROUP BY 1,2,3 ORDER BY 2,3,1;

-- test interpolate with correlated subquery and window function
SELECT
  time_bucket_gapfill(5,time,0,11) AS time,
  device_id,
  sensor_id,
  interpolate(
    min(value),
    (SELECT (time,value) FROM :METRICS m2
     WHERE time<0 AND m2.device_id=m1.device_id AND m2.sensor_id=m1.sensor_id
     ORDER BY time DESC LIMIT 1),
    (SELECT (time,value) FROM :METRICS m2
     WHERE time>10 AND m2.device_id=m1.device_id AND m2.sensor_id=m1.sensor_id
     ORDER BY time LIMIT 1)
  ),
  sum(interpolate(
    min(value),
    (SELECT (time,value) FROM :METRICS m2
     WHERE time<0 AND m2.device_id=m1.device_id AND m2.sensor_id=m1.sensor_id
     ORDER BY time DESC LIMIT 1),
    (SELECT (time,value) FROM :METRICS m2
     WHERE time>10 AND m2.device_id=m1.device_id AND m2.sensor_id=m1.sensor_id
     ORDER BY time LIMIT 1)
  )) OVER (PARTITION BY device_id, sensor_id ROWS 1 PRECEDING)
FROM :METRICS m1
WHERE time >= 0 AND time < 10
GROUP BY 1,2,3 ORDER BY 2,3,1;

-- test subqueries
-- subqueries will alter the shape of the plan and top-level constraints
-- might not end up in top-level of jointree
SELECT
  time_bucket_gapfill(1,m1.time)
FROM :METRICS m1
WHERE m1.time >=0 AND m1.time < 2 AND device_id IN (SELECT device_id FROM :METRICS)
GROUP BY 1;

-- test inner join with constraints in join condition
SELECT
  time_bucket_gapfill(1,m2.time)
FROM :METRICS m1 INNER JOIN :METRICS m2 ON m1.time=m2.time AND m2.time >=0 AND m2.time < 2
GROUP BY 1;

-- test actual table
SELECT
  time_bucket_gapfill(1,time)
FROM :METRICS
WHERE time >=0 AND time < 2
GROUP BY 1;

-- test with table alias
SELECT
  time_bucket_gapfill(1,time)
FROM :METRICS m
WHERE m.time >=0 AND m.time < 2
GROUP BY 1;

-- test with 2 tables
SELECT
  time_bucket_gapfill(1,m.time)
FROM :METRICS m, :METRICS m2
WHERE m.time >=0 AND m.time < 2
GROUP BY 1;

-- test prepared statement with locf with lookup query
PREPARE prep_gapfill AS
SELECT
  time_bucket_gapfill(5,time,0,11) AS time,
  device_id,
  sensor_id,
  locf(min(value)::int,(SELECT 1/(SELECT 0) FROM :METRICS m2 WHERE m2.device_id=m1.device_id AND m2.sensor_id=m1.sensor_id ORDER BY time DESC LIMIT 1))
FROM :METRICS m1
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
    (SELECT (time,value) FROM :METRICS m2
     WHERE time<0 AND m2.device_id=m1.device_id AND m2.sensor_id=m1.sensor_id
     ORDER BY time DESC LIMIT 1),
    (SELECT (time,value) FROM :METRICS m2
     WHERE time>10 AND m2.device_id=m1.device_id AND m2.sensor_id=m1.sensor_id
     ORDER BY time LIMIT 1)
  )
FROM :METRICS m1
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
FROM :METRICS m1
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
