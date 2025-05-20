-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Enable MERGE statements for continuous aggregate refresh
SET timescaledb.enable_merge_on_cagg_refresh TO ON;
SET timezone TO PST8PDT;

\ir include/cagg_refresh_common.sql

-- Additional tests for MERGE refresh
DROP TABLE conditions CASCADE;

CREATE TABLE conditions (
    time TIMESTAMPTZ NOT NULL,
    location TEXT NOT NULL,
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION
);

SELECT FROM create_hypertable( 'conditions', 'time');

INSERT INTO conditions
VALUES
    ('2018-01-01 09:20:00-08', 'SFO', 55, 45),
    ('2018-01-02 09:30:00-08', 'POR', 100, 100),
    ('2018-01-02 09:20:00-08', 'SFO', 65, 45),
    ('2018-01-02 09:10:00-08', 'NYC', 65, 45),
    ('2018-11-01 09:20:00-08', 'NYC', 45, 30),
    ('2018-11-01 10:40:00-08', 'NYC', 55, 35),
    ('2018-11-01 11:50:00-08', 'NYC', 65, 40),
    ('2018-11-01 12:10:00-08', 'NYC', 75, 45),
    ('2018-11-01 13:10:00-08', 'NYC', 85, 50),
    ('2018-11-02 09:20:00-08', 'NYC', 10, 10),
    ('2018-11-02 10:30:00-08', 'NYC', 20, 15),
    ('2018-11-02 11:40:00-08', 'NYC', null, null),
    ('2018-11-03 09:50:00-08', 'NYC', null, null);

CREATE MATERIALIZED VIEW conditions_daily
WITH (timescaledb.continuous) AS
SELECT
   time_bucket(INTERVAL '1 day', time) AS bucket,
   location,
   AVG(temperature),
   MAX(temperature),
   MIN(temperature)
FROM conditions
GROUP BY bucket, location
WITH NO DATA;

-- First refresh using MERGE should fall back to INSERT
SET client_min_messages TO LOG;
CALL refresh_continuous_aggregate('conditions_daily', NULL, '2018-11-01 23:59:59-08');
SELECT * FROM conditions_daily ORDER BY 1, 2, 3 NULLS LAST, 4 NULLS LAST, 5 NULLS LAST;

-- Second refresh using MERGE should also fall back to INSERT since there's no data in the materialization hypertable
CALL refresh_continuous_aggregate('conditions_daily', '2018-11-01', NULL);
SELECT * FROM conditions_daily ORDER BY 1, 2, 3 NULLS LAST, 4 NULLS LAST, 5 NULLS LAST;

-- All data should be in the materialization hypertable
CALL refresh_continuous_aggregate('conditions_daily', NULL, NULL);
SELECT * FROM conditions_daily ORDER BY 1, 2, 3 NULLS LAST, 4 NULLS LAST, 5 NULLS LAST;

-- Changing past data that is not part of the cagg
UPDATE conditions SET humidity = humidity + 100 WHERE time = '2018-01-02 09:20:00-08' AND location = 'SFO';
-- Shoudn't affect the materialization hypertable (merged=0 and deleted=0)
CALL refresh_continuous_aggregate('conditions_daily', NULL, NULL);
SELECT * FROM conditions_daily ORDER BY 1, 2, 3 NULLS LAST, 4 NULLS LAST, 5 NULLS LAST;

-- Backfill some data in the past
INSERT INTO conditions
VALUES
    ('2017-01-01 09:20:00-08', 'GRU', 55, 45),
    ('2017-01-01 09:30:00-08', 'POA', 100, 100),
    ('2017-01-02 09:20:00-08', 'CNF', 65, 45);

-- There's no data in the affected range so the refresh should fall back to INSERT
CALL refresh_continuous_aggregate('conditions_daily', NULL, NULL);
SELECT * FROM conditions_daily ORDER BY 1, 2, 3 NULLS LAST, 4 NULLS LAST, 5 NULLS LAST;

-- Update already materilized data in the past
UPDATE conditions SET temperature = temperature + 100 WHERE time = '2018-11-02 10:30:00-08' AND location = 'NYC';
-- Should merge 1 bucket (merged=1 and deleted=0)
CALL refresh_continuous_aggregate('conditions_daily', NULL, NULL);
SELECT * FROM conditions_daily ORDER BY 1, 2, 3 NULLS LAST, 4 NULLS LAST, 5 NULLS LAST;

-- Delete one entire bucket
DELETE FROM conditions WHERE time >= '2018-11-02' AND time < '2018-11-03' AND location = 'NYC';
-- Should not merge any bucket but delete one bucket (merged=0 and deleted=1)
CALL refresh_continuous_aggregate('conditions_daily', NULL, NULL);
SELECT * FROM conditions_daily ORDER BY 1, 2, 3 NULLS LAST, 4 NULLS LAST, 5 NULLS LAST;

--
-- A nullable conditions test
--
CREATE TABLE conditions_nullable (
    time TIMESTAMPTZ NOT NULL,
    location TEXT,
    temperature DOUBLE PRECISION
);

DROP TABLE conditions CASCADE;

CREATE TABLE conditions (
    time TIMESTAMPTZ NOT NULL,
    location TEXT,
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION
);

SELECT FROM create_hypertable( 'conditions', 'time');

INSERT INTO conditions
VALUES
    ('2018-01-01 09:20:00-08', 'SFO', 55),
    ('2018-01-02 09:30:00-08', null, 100);

CREATE MATERIALIZED VIEW conditions_nullable_daily
WITH (timescaledb.continuous) AS
SELECT
   time_bucket(INTERVAL '1 day', time) AS bucket,
   location,
   AVG(temperature)
FROM conditions
GROUP BY bucket, location
WITH NO DATA;

-- First refresh using MERGE should fall back to INSERT
SET client_min_messages TO LOG;
CALL refresh_continuous_aggregate('conditions_nullable_daily', NULL, '2018-11-01 23:59:59-08');
SELECT * FROM conditions_nullable_daily ORDER BY 1, 2 NULLS LAST, 3 NULLS LAST;

-- Inserting a new data should ensure we get correct results
INSERT INTO conditions
VALUES
    ('2018-01-01 19:20:00-08', 'SFO', 65),
    ('2018-01-02 19:30:00-08', null, 200);

-- Second refresh *should* use the merge, and return correct results.
SET client_min_messages TO LOG;
CALL refresh_continuous_aggregate('conditions_nullable_daily', NULL, '2018-11-01 23:59:59-08');
SELECT * FROM conditions_nullable_daily ORDER BY 1, 2 NULLS LAST, 3 NULLS LAST;
