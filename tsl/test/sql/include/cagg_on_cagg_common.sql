-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\if :IS_DISTRIBUTED
\echo 'Running distributed hypertable tests'
\else
\echo 'Running local hypertable tests'
\endif

SET ROLE :ROLE_DEFAULT_PERM_USER;

-- CAGGs on CAGGs tests
CREATE TABLE conditions (
  time :TIME_DIMENSION_DATATYPE NOT NULL,
  temperature NUMERIC
);

\if :IS_DISTRIBUTED
  \if :IS_TIME_DIMENSION
    SELECT table_name FROM create_distributed_hypertable('conditions', 'time', replication_factor => 2);
  \else
    SELECT table_name FROM create_distributed_hypertable('conditions', 'time', chunk_time_interval => 10, replication_factor => 2);
  \endif
\else
  \if :IS_TIME_DIMENSION
    SELECT table_name FROM create_hypertable('conditions', 'time');
  \else
    SELECT table_name FROM create_hypertable('conditions', 'time', chunk_time_interval => 10);
  \endif
\endif

\if :IS_TIME_DIMENSION
  INSERT INTO conditions VALUES ('2022-01-01 00:00:00-00', 10);
  INSERT INTO conditions VALUES ('2022-01-01 01:00:00-00',  5);
  INSERT INTO conditions VALUES ('2022-01-02 01:00:00-00', 20);
\else
  CREATE OR REPLACE FUNCTION integer_now()
  RETURNS :TIME_DIMENSION_DATATYPE LANGUAGE SQL STABLE AS
  $$
    SELECT coalesce(max(time), 0)
    FROM conditions
  $$;

  \if :IS_DISTRIBUTED
    SELECT
      'CREATE OR REPLACE FUNCTION integer_now() RETURNS '||:'TIME_DIMENSION_DATATYPE'||' LANGUAGE SQL STABLE AS $$ SELECT coalesce(max(time), 0) FROM conditions $$;' AS "STMT"
      \gset
    CALL distributed_exec (:'STMT');
  \endif

  SELECT set_integer_now_func('conditions', 'integer_now');

  INSERT INTO conditions VALUES (1, 10);
  INSERT INTO conditions VALUES (2,  5);
  INSERT INTO conditions VALUES (5, 20);
\endif

-- CAGG on hypertable (1st level)
CREATE MATERIALIZED VIEW :CAGG_NAME_1ST_LEVEL
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT
  time_bucket(:BUCKET_WIDTH_1ST, "time") AS bucket,
  SUM(temperature) AS temperature
FROM conditions
GROUP BY 1
WITH NO DATA;

-- CAGG on CAGG (2th level)
CREATE MATERIALIZED VIEW :CAGG_NAME_2TH_LEVEL
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT
  time_bucket(:BUCKET_WIDTH_2TH, "bucket") AS bucket,
  SUM(temperature) AS temperature
FROM :CAGG_NAME_1ST_LEVEL
GROUP BY 1
WITH NO DATA;

-- CAGG on CAGG (3th level)
CREATE MATERIALIZED VIEW :CAGG_NAME_3TH_LEVEL
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT
  time_bucket(:BUCKET_WIDTH_3TH, "bucket") AS bucket,
  SUM(temperature) AS temperature
FROM :CAGG_NAME_2TH_LEVEL
GROUP BY 1
WITH NO DATA;

-- No data because the CAGGs are just for materialized data
SELECT * FROM :CAGG_NAME_1ST_LEVEL ORDER BY bucket;
SELECT * FROM :CAGG_NAME_2TH_LEVEL ORDER BY bucket;
SELECT * FROM :CAGG_NAME_3TH_LEVEL ORDER BY bucket;

-- Turn CAGGs into Realtime
ALTER MATERIALIZED VIEW :CAGG_NAME_1ST_LEVEL SET (timescaledb.materialized_only=false);
ALTER MATERIALIZED VIEW :CAGG_NAME_2TH_LEVEL SET (timescaledb.materialized_only=false);
ALTER MATERIALIZED VIEW :CAGG_NAME_3TH_LEVEL SET (timescaledb.materialized_only=false);

-- Realtime data
SELECT * FROM :CAGG_NAME_1ST_LEVEL ORDER BY bucket;
SELECT * FROM :CAGG_NAME_2TH_LEVEL ORDER BY bucket;
SELECT * FROM :CAGG_NAME_3TH_LEVEL ORDER BY bucket;

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
SELECT * FROM :CAGG_NAME_2TH_LEVEL ORDER BY bucket;
SELECT * FROM :CAGG_NAME_3TH_LEVEL ORDER BY bucket;

\if :IS_TIME_DIMENSION
-- Invalidate an old region
INSERT INTO conditions VALUES ('2022-01-01 01:00:00-00'::timestamptz, 2);
-- New region
INSERT INTO conditions VALUES ('2022-01-03 01:00:00-00'::timestamptz, 2);
\else
-- Invalidate an old region
INSERT INTO conditions VALUES (2,  2);
-- New region
INSERT INTO conditions VALUES (10, 2);
\endif

-- No changes
SELECT * FROM :CAGG_NAME_1ST_LEVEL ORDER BY bucket;
SELECT * FROM :CAGG_NAME_2TH_LEVEL ORDER BY bucket;
SELECT * FROM :CAGG_NAME_3TH_LEVEL ORDER BY bucket;

-- Turn CAGGs into Realtime
ALTER MATERIALIZED VIEW :CAGG_NAME_1ST_LEVEL SET (timescaledb.materialized_only=false);
ALTER MATERIALIZED VIEW :CAGG_NAME_2TH_LEVEL SET (timescaledb.materialized_only=false);
ALTER MATERIALIZED VIEW :CAGG_NAME_3TH_LEVEL SET (timescaledb.materialized_only=false);

-- Realtime changes, just new region
SELECT * FROM :CAGG_NAME_1ST_LEVEL ORDER BY bucket;
SELECT * FROM :CAGG_NAME_2TH_LEVEL ORDER BY bucket;
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
SELECT * FROM :CAGG_NAME_2TH_LEVEL ORDER BY bucket;
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
SELECT * FROM :CAGG_NAME_2TH_LEVEL ORDER BY bucket;
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
