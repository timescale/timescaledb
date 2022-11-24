-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set CAGG_NAME_1ST_LEVEL conditions_summary_1
\set CAGG_NAME_2TH_LEVEL conditions_summary_2

--
-- CAGG on hypertable (1st level)
--
CREATE MATERIALIZED VIEW :CAGG_NAME_1ST_LEVEL
WITH (timescaledb.continuous) AS
SELECT
  time_bucket(:BUCKET_WIDTH_1ST, "time") AS bucket,
  SUM(temperature) AS temperature
FROM conditions
GROUP BY 1
WITH NO DATA;

--
-- CAGG on CAGG (2th level)
--
\set VERBOSITY default
\set ON_ERROR_STOP 0
\echo :WARNING_MESSAGE
CREATE MATERIALIZED VIEW :CAGG_NAME_2TH_LEVEL
WITH (timescaledb.continuous) AS
SELECT
  time_bucket(:BUCKET_WIDTH_2TH, "bucket") AS bucket,
  SUM(temperature) AS temperature
FROM :CAGG_NAME_1ST_LEVEL
GROUP BY 1
WITH NO DATA;
\set ON_ERROR_STOP 0
\set VERBOSITY terse

--
-- Cleanup
--
DROP MATERIALIZED VIEW IF EXISTS :CAGG_NAME_2TH_LEVEL;
DROP MATERIALIZED VIEW IF EXISTS :CAGG_NAME_1ST_LEVEL;
