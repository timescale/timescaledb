-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Tests for rewrites with Caggs with different degrees of materialization

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
