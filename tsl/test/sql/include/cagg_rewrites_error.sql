-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Queries generally not eligible for rewrite

-- No FROM
SELECT 1;

-- Not SELECT
BEGIN;
DELETE FROM conditions WHERE device_id = 4;
ROLLBACK;

-- Has window function
SELECT device_id, AVG(temperature) OVER (ORDER BY device_id) FROM conditions ORDER BY 1 LIMIT 1;

-- Has DISTINCT
SELECT DISTINCT location FROM devices ORDER BY 1;
SELECT DISTINCT ON(device_id) device_id, location FROM devices ORDER BY 1;

-- Has CTE/sublinks which subqueries are also not eligible for rewrite (has grouping sets; no groupby)
WITH con as (SELECT device_id, AVG(temperature) FROM conditions GROUP BY ROLLUP (device_id)) SELECT * FROM con ORDER BY 1 LIMIT 1;
SELECT * FROM conditions WHERE temperature = (SELECT AVG(temperature) FROM conditions);

-- Has row-level security
ALTER TABLE location ENABLE ROW LEVEL SECURITY;
SELECT location_id, max(name) from location group by location_id;
ALTER TABLE location DISABLE ROW LEVEL SECURITY;

-- ineligible because of relations used in a query

-- no hypertable
SELECT location, count(device_id)
FROM devices
GROUP BY location
ORDER BY 1 LIMIT 1;

-- two hypertables
SELECT time_bucket(INTERVAL '3 days', conditions.day) AS bucket,
   AVG(conditions.temperature),
   count(conditions_dup.device_id)
FROM conditions_dup, conditions
WHERE conditions_dup.device_id = conditions.device_id
GROUP BY bucket
ORDER BY 1 LIMIT 1;

-- Full Outer Join
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   name
FROM devices FULL OUTER JOIN conditions
ON conditions.device_id = devices.device_id
GROUP BY name, bucket
ORDER BY 1 LIMIT 1;

-- Has non-lateral subquery; the subquery has TABLESAMPLE
SELECT time_bucket(INTERVAL '1 day', d) AS bucket,
   AVG(temp) AS avg,
   device_id
FROM (SELECT device_id, max (day) as d, avg(temperature) as temp FROM conditions TABLESAMPLE SYSTEM (0) group by device_id) con
GROUP BY device_id, bucket
ORDER BY 1 LIMIT 1;

-- Query over Cagg materialization table
SELECT time_bucket(INTERVAL '1 day', bucket) AS bucket,
   device_id
FROM :"MAT_SCHEMA_NAME".:"MAT_TABLE_NAME"
GROUP BY device_id, bucket
ORDER BY 1 LIMIT 1;

-- Query over hypertable with custom partitioning
SELECT time_bucket('5', text_part_func(city)), avg(temperature)
  FROM conditions_custom
GROUP BY 1;

-- ineligible time buckets

-- no time bucket
SELECT day,
   AVG(temperature),
   count(device_id)
FROM conditions
GROUP BY day
ORDER BY 1 LIMIT 1;

-- several time buckets
SELECT time_bucket(INTERVAL '1 day', day) AS bucket1, time_bucket(INTERVAL '3 days', day) AS bucket3,
   AVG(temperature),
   count(device_id)
FROM conditions
GROUP BY 1,2
ORDER BY 1,2 LIMIT 1;

-- bucket time zone doesn't match
SELECT time_bucket(INTERVAL '1 day', day, 'Europe/Berlin') AS bucket,
   AVG(temperature) AS avg,
   device_id
FROM conditions
GROUP BY device_id, bucket
ORDER BY 1, 2, 3;

-- time bucket not on primary dimension
SELECT time_bucket(1, temperature) AS bucket,
   AVG(temperature),
   count(device_id)
FROM conditions
GROUP BY bucket
ORDER BY 1 LIMIT 1;

-- time bucket with non-const 1st argument
SELECT time_bucket(('2026-01-08 15:00:00'::timestamptz - day), day) AS bucket,
   AVG(temperature) AS avg,
   count(device_id)
FROM conditions
GROUP BY bucket
HAVING count(device_id) > 0
ORDER BY 1 LIMIT 1;

-- infinity origin
SELECT time_bucket(INTERVAL '3 days', day, 'infinity'::timestamptz) AS bucket,
   AVG(temperature),
   count(device_id)
FROM conditions
GROUP BY bucket
ORDER BY 1 LIMIT 1;

-- origin and offset at the same time
SELECT time_bucket(INTERVAL '3 days', day, "offset"=>'30m'::interval, origin=>'2000-01-01 01:00:00 PST'::timestamptz, timezone=>'UTC') AS bucket,
   AVG(temperature),
   count(device_id)
FROM conditions
GROUP BY bucket
ORDER BY 1 LIMIT 1;

-- No Caggs match a candidate query

-- no Caggs
SELECT time_bucket(INTERVAL '3 days', day) AS bucket,
   AVG(temperature),
   count(device_id)
FROM conditions_dup
GROUP BY bucket
ORDER BY 1 LIMIT 1;

-- no matching Caggs
ALTER MATERIALIZED VIEW cagg_view SET (timescaledb.materialized_only=true);

-- Almost cagg1 but unmatched groupby
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature) AS avg
FROM conditions
GROUP BY bucket
ORDER BY 1 LIMIT 1;

SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature) AS avg
FROM conditions
GROUP BY bucket, city
ORDER BY 1 LIMIT 1;

-- Almost cagg1 but unmatched aggregate
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature) AS avg, max(city) as city, device_id
FROM conditions
GROUP BY device_id, bucket
ORDER BY 1 LIMIT 1;

-- Almost cagg3 but unmatched having
SELECT time_bucket(INTERVAL '3 days', day) AS bucket,
   AVG(temperature) AS avg,
   count(device_id)
FROM conditions
GROUP BY bucket
HAVING count(device_id) > 0
ORDER BY 1 LIMIT 1;

-- Almost caggs_more_conds but target doesn't match
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   COUNT(location_id),
   devices.name,
   devices.device_id
FROM conditions LEFT JOIN devices ON conditions.device_id = devices.device_id
JOIN location ON conditions.city = location.name
WHERE location_id > 1 AND
      conditions.temperature > 28
GROUP BY devices.name, bucket, devices.device_id
ORDER BY 1 LIMIT 1;

-- Almost caggs_more_conds but a qual does not match
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   COUNT(location_id),
   devices.name
FROM conditions LEFT JOIN devices ON conditions.device_id = devices.device_id
JOIN location ON conditions.city = location.name
WHERE location_id > 1 AND
      conditions.temperature < 20
GROUP BY devices.name, bucket, devices.device_id
ORDER BY 1 LIMIT 1;

-- Almost caggs_more_conds but only one qual matches
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   COUNT(location_id),
   devices.name
FROM conditions LEFT JOIN devices ON conditions.device_id = devices.device_id
JOIN location ON conditions.city = location.name
WHERE location_id > 1 AND
      location_id > 1
GROUP BY devices.name, bucket, devices.device_id
ORDER BY 1,2,3,4
LIMIT 2;

-- Almost caggs_more_conds but quals do not match due to commuted operand
-- while OpExpr arguments are the same
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   COUNT(location_id),
   devices.name
FROM conditions LEFT JOIN devices ON conditions.device_id = devices.device_id
JOIN location ON conditions.city = location.name
WHERE location_id < 1 AND
      conditions.temperature < 28
GROUP BY devices.name, bucket, devices.device_id
ORDER BY 1,2,3,4
LIMIT 2;

-- Report on a query ineligible for hierarchical Cagg
SELECT time_bucket(INTERVAL '1 day', bucket) AS bucket,
       SUM(avg) AS temperature
FROM cagg1, devices
WHERE devices.device_id = cagg1.device_id AND devices.device_id > 1
GROUP BY 1 ORDER BY 1 LIMIT 1;

-- External params from prepared statements cannot be used in place of Consts
PREPARE prep1 AS
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   COUNT(location_id),
   devices.name
FROM conditions LEFT JOIN devices ON conditions.device_id = devices.device_id
JOIN location ON conditions.city = location.name
WHERE location_id > 1 AND
      conditions.temperature > $1
GROUP BY devices.name, bucket, devices.device_id
ORDER BY 1,2,3,4
LIMIT 2;

EXECUTE prep1(28);
DEALLOCATE prep1;

PREPARE prep2 AS
SELECT time_bucket($1, day) AS bucket,
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

EXECUTE prep2(INTERVAL '1 day');
DEALLOCATE prep2;

-- Internal params cannot be used in place of Consts
SELECT * FROM (VALUES (1), (1)) a(v),
  LATERAL
  (SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   COUNT(location_id),
   devices.name
   FROM conditions LEFT JOIN devices ON conditions.device_id = devices.device_id
   JOIN location ON conditions.city = location.name
   WHERE location_id > a.v AND
      conditions.temperature > 28
   GROUP BY devices.name, bucket, devices.device_id) q
ORDER BY 1,2,3,4
LIMIT 2;

-- Backfilled data makes all Caggs on a hypertable ineligible for rewrites until they are refreshed
INSERT INTO conditions (day, city, temperature, device_id) VALUES
  ('2021-06-15', 'Moscow', 23,1),
  ('2021-06-17', 'Berlin', 24,2),
  ('2021-06-17', 'Stockholm', 21,3);

-- (cagg1) is eligible but invalidated
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature) AS avg,
   device_id
FROM conditions
GROUP BY device_id, bucket
ORDER BY 1, 2, 3;

-- After (cagg2) is refreshed it's eligible again,
-- the rest of the caggs have their materialization invalidation logs updated
-- while hypertable invalidation logs are cleared
SET timescaledb.cagg_rewrites_debug_info = 0;
CALL refresh_continuous_aggregate('cagg2', NULL, NULL);
SET timescaledb.cagg_rewrites_debug_info = 1;

SELECT time_bucket(INTERVAL '2 day', day) AS bucket,
   count(device_id)
FROM conditions
GROUP BY bucket
ORDER BY 1, 2
LIMIT 3;

-- (cagg1) is still not eligible as it's not refreshed since last backfill
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature) AS avg,
   device_id
FROM conditions
GROUP BY device_id, bucket
ORDER BY 1, 2, 3;

-- Caggs with pending materialization ranges are not eligible.
-- Insert new row into hypertable, remove (cagg2) watermark and then refresh it.
-- It will create pending materialization range on (cagg2) as refresh will error out.
SET timescaledb.cagg_rewrites_debug_info = 0;
INSERT INTO conditions (day, city, temperature, device_id) VALUES
  ('2021-06-16', 'Berlin', 23, 2);

SELECT ca.mat_hypertable_id AS "CAGG2_ID", watermark AS "CAGG2_WATERMARK"
FROM _timescaledb_catalog.continuous_agg ca INNER JOIN _timescaledb_catalog.continuous_aggs_watermark wm
ON (ca.mat_hypertable_id = wm.mat_hypertable_id) WHERE user_view_name = 'cagg2';
\gset

-- This will create pending materialization range
\c :TEST_DBNAME :ROLE_SUPERUSER
DELETE FROM _timescaledb_catalog.continuous_aggs_watermark WHERE mat_hypertable_id = :CAGG2_ID;
\set ON_ERROR_STOP 0
CALL refresh_continuous_aggregate('cagg2', NULL, NULL);
\set ON_ERROR_STOP 1

-- Restore deleted watermark so that we can query (cagg2) again
INSERT INTO _timescaledb_catalog.continuous_aggs_watermark values(:CAGG2_ID, :CAGG2_WATERMARK);

SET timescaledb.cagg_rewrites_debug_info = 1;
SELECT time_bucket(INTERVAL '2 day', day) AS bucket,
   count(device_id)
FROM conditions
GROUP BY bucket
ORDER BY 1, 2
LIMIT 3;

-- cleanup materialization ranges
TRUNCATE TABLE _timescaledb_catalog.continuous_aggs_materialization_ranges;
