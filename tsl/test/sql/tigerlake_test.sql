-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

--  These tests work for PG17 or greater

CREATE TABLE ht_try(timec timestamptz NOT NULL, acq_id bigint, value bigint);
SELECT create_hypertable('ht_try', 'timec', chunk_time_interval => interval '1 day');
INSERT INTO ht_try VALUES ('2022-05-05 01:00', 222, 222);

SELECT * FROM ht_try ORDER BY 1;

\set ON_ERROR_STOP 0
-- will error out
CREATE MATERIALIZED VIEW ht_try_weekly
WITH (timescaledb.continuous, tigerlake.newoption = true) AS
SELECT time_bucket(interval '1 week', timec) AS ts_bucket, avg(value)
FROM ht_try
GROUP BY 1;
\set ON_ERROR_STOP 1

-- create works without tigerlake option
CREATE MATERIALIZED VIEW ht_try_weekly
WITH (timescaledb.continuous) AS
SELECT time_bucket(interval '1 week', timec) AS ts_bucket, avg(value)
FROM ht_try
GROUP BY 1;

SELECT * from ht_try_weekly order by 1;
--caught by Postgres now
ALTER MATERIALIZED VIEW ht_try_weekly SET (tigerlake.newoption = true);
