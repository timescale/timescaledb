-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- CAGG on hypertable (1st level)
CREATE MATERIALIZED VIEW :CAGG_NAME_1ST_LEVEL
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT
  time_bucket(:BUCKET_WIDTH_1ST, "time") AS bucket,
  SUM(temperature) AS temperature,
  device_id
FROM conditions
GROUP BY 1,3
WITH NO DATA;

-- CAGG on CAGG (2th level)
CREATE MATERIALIZED VIEW :CAGG_NAME_2TH_LEVEL
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT
  time_bucket(:BUCKET_WIDTH_2TH, "bucket") AS bucket,
  SUM(temperature) AS temperature
\if :IS_JOIN
  , :CAGG_NAME_1ST_LEVEL.device_id
  FROM :CAGG_NAME_1ST_LEVEL, devices
  WHERE devices.device_id = :CAGG_NAME_1ST_LEVEL.device_id
  GROUP BY 1,3
\else
  FROM :CAGG_NAME_1ST_LEVEL
  GROUP BY 1
\endif

WITH NO DATA;

-- CAGG on CAGG (3th level)
CREATE MATERIALIZED VIEW :CAGG_NAME_3TH_LEVEL
  WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
  SELECT
    time_bucket(:BUCKET_WIDTH_3TH, "bucket") AS bucket,
    SUM(temperature) AS temperature
  \if :IS_JOIN
    , :CAGG_NAME_2TH_LEVEL.device_id
    FROM :CAGG_NAME_2TH_LEVEL, devices
    WHERE devices.device_id = :CAGG_NAME_2TH_LEVEL.device_id
    GROUP BY 1,3
  \else
    FROM :CAGG_NAME_2TH_LEVEL
    GROUP BY 1
  \endif
  WITH NO DATA;

-- Check chunk_interval
\if :IS_TIME_DIMENSION
  SELECT h.table_name AS name, _timescaledb_functions.to_interval(d.interval_length) AS chunk_interval
  FROM _timescaledb_catalog.hypertable h
  LEFT JOIN _timescaledb_catalog.dimension d on d.hypertable_id = h.id
  WHERE h.table_name = 'conditions'
  UNION ALL
  SELECT c.user_view_name AS name, _timescaledb_functions.to_interval(d.interval_length) AS chunk_interval
  FROM _timescaledb_catalog.continuous_agg c
  LEFT JOIN _timescaledb_catalog.dimension d on d.hypertable_id = c.mat_hypertable_id
  WHERE c.user_view_name IN (:'CAGG_NAME_1ST_LEVEL', :'CAGG_NAME_2TH_LEVEL', :'CAGG_NAME_3TH_LEVEL')
  ORDER BY 1, 2;
\else
  SELECT h.table_name AS name, d.interval_length AS chunk_interval
  FROM _timescaledb_catalog.hypertable h
  LEFT JOIN _timescaledb_catalog.dimension d on d.hypertable_id = h.id
  WHERE h.table_name = 'conditions'
  UNION ALL
  SELECT c.user_view_name AS name, d.interval_length AS chunk_interval
  FROM _timescaledb_catalog.continuous_agg c
  LEFT JOIN _timescaledb_catalog.dimension d on d.hypertable_id = c.mat_hypertable_id
  WHERE c.user_view_name IN (:'CAGG_NAME_1ST_LEVEL', :'CAGG_NAME_2TH_LEVEL', :'CAGG_NAME_3TH_LEVEL')
  ORDER BY 1, 2;
\endif


-- No data because the CAGGs are just for materialized data
SELECT * FROM :CAGG_NAME_1ST_LEVEL ORDER BY bucket;
SELECT * FROM :CAGG_NAME_2TH_LEVEL ORDER BY bucket, temperature;
--SELECT * FROM :CAGG_NAME_3TH_LEVEL ORDER BY bucket;

-- Turn CAGGs into Realtime
ALTER MATERIALIZED VIEW :CAGG_NAME_1ST_LEVEL SET (timescaledb.materialized_only=false);
ALTER MATERIALIZED VIEW :CAGG_NAME_2TH_LEVEL SET (timescaledb.materialized_only=false);
--ALTER MATERIALIZED VIEW :CAGG_NAME_3TH_LEVEL SET (timescaledb.materialized_only=false);

-- Realtime data
SELECT * FROM :CAGG_NAME_1ST_LEVEL ORDER BY bucket;
SELECT * FROM :CAGG_NAME_2TH_LEVEL ORDER BY bucket, temperature;
--SELECT * FROM :CAGG_NAME_3TH_LEVEL ORDER BY bucket;

-- Turn CAGGs into materialized only again
ALTER MATERIALIZED VIEW :CAGG_NAME_1ST_LEVEL SET (timescaledb.materialized_only=true);
ALTER MATERIALIZED VIEW :CAGG_NAME_2TH_LEVEL SET (timescaledb.materialized_only=true);
ALTER MATERIALIZED VIEW :CAGG_NAME_3TH_LEVEL SET (timescaledb.materialized_only=true);

-- Refresh all data
CALL refresh_continuous_aggregate(:'CAGG_NAME_1ST_LEVEL', NULL, NULL);
CALL refresh_continuous_aggregate(:'CAGG_NAME_2TH_LEVEL', NULL, NULL);
CALL refresh_continuous_aggregate(:'CAGG_NAME_3TH_LEVEL', NULL, NULL);

-- Materialized data
SELECT * FROM :CAGG_NAME_1ST_LEVEL ORDER BY bucket;
SELECT * FROM :CAGG_NAME_2TH_LEVEL ORDER BY bucket, temperature;
SELECT * FROM :CAGG_NAME_3TH_LEVEL ORDER BY bucket;

\if :IS_TIME_DIMENSION
-- Invalidate an old region
INSERT INTO conditions ("time", temperature) VALUES ('2022-01-01 01:00:00-00'::timestamptz, 2);
-- New region
INSERT INTO conditions ("time", temperature) VALUES ('2022-01-03 01:00:00-00'::timestamptz, 2);
\else
-- Invalidate an old region
INSERT INTO conditions ("time", temperature) VALUES (2,  2);
-- New region
INSERT INTO conditions ("time", temperature) VALUES (10, 2);
\endif

-- No changes
SELECT * FROM :CAGG_NAME_1ST_LEVEL ORDER BY bucket;
SELECT * FROM :CAGG_NAME_2TH_LEVEL ORDER BY bucket, temperature;
SELECT * FROM :CAGG_NAME_3TH_LEVEL ORDER BY bucket;

-- Turn CAGGs into Realtime
ALTER MATERIALIZED VIEW :CAGG_NAME_1ST_LEVEL SET (timescaledb.materialized_only=false);
ALTER MATERIALIZED VIEW :CAGG_NAME_2TH_LEVEL SET (timescaledb.materialized_only=false);
ALTER MATERIALIZED VIEW :CAGG_NAME_3TH_LEVEL SET (timescaledb.materialized_only=false);

-- Realtime changes, just new region
SELECT * FROM :CAGG_NAME_1ST_LEVEL ORDER BY bucket;
SELECT * FROM :CAGG_NAME_2TH_LEVEL ORDER BY bucket, temperature;
SELECT * FROM :CAGG_NAME_3TH_LEVEL ORDER BY bucket;

-- Turn CAGGs into materialized only again
ALTER MATERIALIZED VIEW :CAGG_NAME_1ST_LEVEL SET (timescaledb.materialized_only=true);
ALTER MATERIALIZED VIEW :CAGG_NAME_2TH_LEVEL SET (timescaledb.materialized_only=true);
ALTER MATERIALIZED VIEW :CAGG_NAME_3TH_LEVEL SET (timescaledb.materialized_only=true);

-- Refresh all data
CALL refresh_continuous_aggregate(:'CAGG_NAME_1ST_LEVEL', NULL, NULL);
CALL refresh_continuous_aggregate(:'CAGG_NAME_2TH_LEVEL', NULL, NULL);
CALL refresh_continuous_aggregate(:'CAGG_NAME_3TH_LEVEL', NULL, NULL);

-- All changes are materialized
SELECT * FROM :CAGG_NAME_1ST_LEVEL ORDER BY bucket;
SELECT * FROM :CAGG_NAME_2TH_LEVEL ORDER BY bucket, temperature;
SELECT * FROM :CAGG_NAME_3TH_LEVEL ORDER BY bucket;

-- TRUNCATE tests
TRUNCATE :CAGG_NAME_2TH_LEVEL;
-- This full refresh will remove all the data from the 3TH level cagg
CALL refresh_continuous_aggregate(:'CAGG_NAME_3TH_LEVEL', NULL, NULL);
-- Should return no rows
SELECT * FROM :CAGG_NAME_2TH_LEVEL ORDER BY bucket;
SELECT * FROM :CAGG_NAME_3TH_LEVEL ORDER BY bucket;
-- If we have all the data in the bottom levels caggs we can rebuild
CALL refresh_continuous_aggregate(:'CAGG_NAME_2TH_LEVEL', NULL, NULL);
CALL refresh_continuous_aggregate(:'CAGG_NAME_3TH_LEVEL', NULL, NULL);
-- Now we have all the data
SELECT * FROM :CAGG_NAME_2TH_LEVEL ORDER BY bucket, temperature;
SELECT * FROM :CAGG_NAME_3TH_LEVEL ORDER BY bucket;

-- DROP tests
\set ON_ERROR_STOP 0
-- should error because it depends of other CAGGs
DROP MATERIALIZED VIEW :CAGG_NAME_1ST_LEVEL;
DROP MATERIALIZED VIEW :CAGG_NAME_2TH_LEVEL;
CALL refresh_continuous_aggregate(:'CAGG_NAME_1ST_LEVEL', NULL, NULL);
CALL refresh_continuous_aggregate(:'CAGG_NAME_2TH_LEVEL', NULL, NULL);
\set ON_ERROR_STOP 1

-- DROP the 3TH level CAGG don't affect others
DROP MATERIALIZED VIEW :CAGG_NAME_3TH_LEVEL;
\set ON_ERROR_STOP 0
-- should error because it was dropped
SELECT * FROM :CAGG_NAME_3TH_LEVEL ORDER BY bucket;
\set ON_ERROR_STOP 1
-- should work because dropping the top level CAGG
-- don't affect the down level CAGGs
TRUNCATE :CAGG_NAME_2TH_LEVEL,:CAGG_NAME_1ST_LEVEL;
CALL refresh_continuous_aggregate(:'CAGG_NAME_2TH_LEVEL', NULL, NULL);
CALL refresh_continuous_aggregate(:'CAGG_NAME_1ST_LEVEL', NULL, NULL);
SELECT * FROM :CAGG_NAME_1ST_LEVEL ORDER BY bucket;
SELECT * FROM :CAGG_NAME_2TH_LEVEL ORDER BY bucket;

-- DROP the 2TH level CAGG don't affect others
DROP MATERIALIZED VIEW :CAGG_NAME_2TH_LEVEL;
\set ON_ERROR_STOP 0
-- should error because it was dropped
SELECT * FROM :CAGG_NAME_2TH_LEVEL ORDER BY bucket;
\set ON_ERROR_STOP 1
-- should work because dropping the top level CAGG
-- don't affect the down level CAGGs
SELECT * FROM :CAGG_NAME_1ST_LEVEL ORDER BY bucket;

-- DROP the first CAGG should work
DROP MATERIALIZED VIEW :CAGG_NAME_1ST_LEVEL;
\set ON_ERROR_STOP 0
-- should error because it was dropped
SELECT * FROM :CAGG_NAME_1ST_LEVEL ORDER BY bucket;
\set ON_ERROR_STOP 1
