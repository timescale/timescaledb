-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- (cagg1)
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature) AS avg,
   device_id
FROM conditions
GROUP BY device_id, bucket
ORDER BY 1, 2, 3;

-- (cagg2)
SELECT time_bucket(INTERVAL '2 day', day) AS bucket,
   count(device_id)
FROM conditions
GROUP BY bucket
ORDER BY 1, 2
LIMIT 3;

-- (cagg3)
SELECT time_bucket(INTERVAL '3 days', day) AS bucket,
   AVG(temperature) AS avg
FROM conditions
GROUP BY bucket
HAVING count(device_id) > 1
ORDER BY 1, 2;

-- (cagg3) with equivalent Having
SELECT time_bucket(INTERVAL '3 days', day) AS bucket,
   AVG(temperature) AS avg,
   count(device_id)
FROM conditions
GROUP BY bucket
HAVING (2-1) < count(device_id)
ORDER BY 1, 2;

-- (cagg_join), flip join order
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   name
FROM devices, conditions
WHERE devices.device_id = conditions.device_id
GROUP BY name, bucket
ORDER BY 1, 2, 3;

-- (cagg_more_conds) w/o one target
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   COUNT(location_id),
   devices.name
FROM conditions LEFT JOIN devices ON conditions.device_id = devices.device_id
JOIN location ON conditions.city = location.name
WHERE location_id > 1 AND
      conditions.temperature > 28
GROUP BY devices.name, bucket, devices.device_id
ORDER BY 1,2,3,4
LIMIT 2;

-- (cagg_more_conds) with expressions in conditions and targets
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   COUNT(location_id),
   (devices.device_id * 2) + 1,
   devices.name
FROM conditions LEFT JOIN devices ON conditions.device_id = devices.device_id
JOIN location ON conditions.city = location.name
WHERE 1+0 < location_id AND
      conditions.temperature > 29-1
GROUP BY devices.name, bucket, devices.device_id
ORDER BY 1,2,3,4
LIMIT 2;

-- Hierarchical Caggs and Caggs on views

-- (cagg_on_cagg1)
SELECT time_bucket(INTERVAL '1 day', bucket) AS bucket,
       SUM(avg) AS temperature
FROM cagg1, devices
WHERE devices.device_id = cagg1.device_id
GROUP BY 1 ORDER BY 1, 2;

-- (cagg_view)
ALTER MATERIALIZED VIEW cagg_view SET (timescaledb.materialized_only=false);
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   devices_view.device_id,
   name
FROM conditions, devices_view
WHERE conditions.device_id = devices_view.device_id
GROUP BY name, bucket, devices_view.device_id
ORDER BY 1, 2, 3, 4;

-- (cagg_lateral)
SELECT time_bucket(INTERVAL '1 day', day) AS bucket, q.name, avg(temperature)  from conditions,
LATERAL (SELECT * FROM devices WHERE devices.device_id = conditions.device_id) q
GROUP BY bucket, q.name
ORDER BY 1, 2, 3;

-- Cagg rewrites in subqueries/SET ops

-- (cagg1) CTE
WITH con AS (SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature) AS avg,
   device_id
FROM conditions
GROUP BY device_id, bucket)
SELECT * FROM con
ORDER BY 1, 2, 3;

-- (cagg1) subquery
SELECT * FROM (SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature) AS avg,
   device_id
FROM conditions
GROUP BY device_id, bucket) con
ORDER BY 1, 2, 3;

-- (cagg2) sublink
SELECT * from conditions
WHERE EXISTS (SELECT time_bucket(INTERVAL '2 days', day) AS bucket,
   AVG(temperature) AS avg_temp
FROM conditions
GROUP BY bucket)
ORDER BY 1, 2, 3, 4;

-- Union of cagg2 and cagg3
SELECT time_bucket(INTERVAL '2 days', day) AS bucket,
   AVG(temperature),
   count(device_id)
FROM conditions
GROUP BY bucket
UNION ALL
SELECT time_bucket(INTERVAL '3 days', day) AS bucket,
   AVG(temperature) AS avg,
   count(device_id)
FROM conditions
GROUP BY bucket
HAVING count(device_id) > 1
ORDER BY 1, 2, 3;
