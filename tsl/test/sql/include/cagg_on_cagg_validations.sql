-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set CAGG_NAME_1ST_LEVEL conditions_summary_1
\set CAGG_NAME_2TH_LEVEL conditions_summary_2
\set CAGG_NAME_3TH_LEVEL conditions_summary_3

--
-- CAGG on hypertable (1st level)
--
CREATE MATERIALIZED VIEW :CAGG_NAME_1ST_LEVEL
WITH (timescaledb.continuous) AS
SELECT
  \if :IS_TIME_DIMENSION_WITH_TIMEZONE_1ST
    time_bucket(:BUCKET_WIDTH_1ST, "time", :'BUCKET_TZNAME_1ST') AS bucket,
  \else
    time_bucket(:BUCKET_WIDTH_1ST, "time") AS bucket,
  \endif
  SUM(temperature) AS temperature
FROM conditions
GROUP BY 1
WITH NO DATA;

\d+ :CAGG_NAME_1ST_LEVEL

--
-- CAGG on CAGG (2th level)
--
\set VERBOSITY default
\set ON_ERROR_STOP 0
\echo :WARNING_MESSAGE
CREATE MATERIALIZED VIEW :CAGG_NAME_2TH_LEVEL
WITH (timescaledb.continuous) AS
SELECT
  \if :IS_TIME_DIMENSION_WITH_TIMEZONE_2TH
    time_bucket(:BUCKET_WIDTH_2TH, "bucket", :'BUCKET_TZNAME_2TH') AS bucket,
  \else
    time_bucket(:BUCKET_WIDTH_2TH, "bucket") AS bucket,
  \endif
  SUM(temperature) AS temperature
FROM :CAGG_NAME_1ST_LEVEL
GROUP BY 1
WITH NO DATA;

\d+ :CAGG_NAME_2TH_LEVEL

\set ON_ERROR_STOP 1
\set VERBOSITY terse

-- Check for incorrect CAGGs
\if :INTERVAL_TEST

  INSERT INTO conditions ("time", temperature, device_id) VALUES ('2022-01-01 00:00:00-00', 10, 4);
  INSERT INTO conditions ("time", temperature, device_id) VALUES ('2022-01-15 01:00:00-00', 20, 4);
  INSERT INTO conditions ("time", temperature, device_id) VALUES ('2022-01-31 01:00:00-00', 30, 4);

  CALL refresh_continuous_aggregate(:'CAGG_NAME_1ST_LEVEL', NULL, NULL);
  CALL refresh_continuous_aggregate(:'CAGG_NAME_2TH_LEVEL', NULL, NULL);

  CREATE MATERIALIZED VIEW :CAGG_NAME_3TH_LEVEL
  WITH (timescaledb.continuous) AS
  SELECT
    \if :IS_TIME_DIMENSION_WITH_TIMEZONE_2TH
      time_bucket(:BUCKET_WIDTH_3TH, "bucket", :'BUCKET_TZNAME_2TH') AS bucket
    \else
      time_bucket(:BUCKET_WIDTH_3TH, "bucket") AS bucket
    \endif
  FROM :CAGG_NAME_2TH_LEVEL
  GROUP BY 1
  WITH DATA;

  \d+ :CAGG_NAME_3TH_LEVEL

  --There should never be dulpicates in the output of the following query

  SELECT * from :CAGG_NAME_3TH_LEVEL;

  DROP MATERIALIZED VIEW IF EXISTS :CAGG_NAME_3TH_LEVEL;
  DELETE FROM conditions WHERE device_id = 4;
\endif

--
-- Cleanup
--
DROP MATERIALIZED VIEW IF EXISTS :CAGG_NAME_2TH_LEVEL;
DROP MATERIALIZED VIEW IF EXISTS :CAGG_NAME_1ST_LEVEL;
