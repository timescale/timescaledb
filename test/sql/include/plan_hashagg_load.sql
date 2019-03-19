-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE TABLE metric (id SERIAL PRIMARY KEY, value INT);
CREATE TABLE hyper(time TIMESTAMP NOT NULL, time_int BIGINT, time_broken DATE, metricid int, value double precision);
CREATE TABLE regular(time TIMESTAMP NOT NULL, time_int BIGINT, time_date DATE, metricid int, value double precision);

SELECT create_hypertable('hyper', 'time', chunk_time_interval => interval '20 day', create_default_indexes=>FALSE);

ALTER TABLE hyper
DROP COLUMN time_broken,
ADD COLUMN time_date DATE;

INSERT INTO metric(value) SELECT random()*100 FROM generate_series(0,10);

INSERT INTO hyper SELECT t,  EXTRACT(EPOCH FROM t), (EXTRACT(EPOCH FROM t)::int % 10)+1, 1.0, t::date FROM generate_series('2001-01-01', '2001-01-10', INTERVAL '1 second') t;
INSERT INTO regular(time, time_int, time_date, metricid, value)
  SELECT t,  EXTRACT(EPOCH FROM t), t::date, (EXTRACT(EPOCH FROM t)::int % 10) + 1, 1.0 FROM generate_series('2001-01-01', '2001-01-02', INTERVAL '1 second') t;

--test some queries before analyze;
EXPLAIN (costs off) SELECT time_bucket('1 minute', time) AS MetricMinuteTs, AVG(value) as avg
FROM hyper
WHERE time >= '2001-01-04T00:00:00' AND time <= '2001-01-05T01:00:00'
GROUP BY MetricMinuteTs
ORDER BY MetricMinuteTs DESC;

EXPLAIN (costs off) SELECT date_trunc('minute', time) AS MetricMinuteTs, AVG(value) as avg
FROM hyper
WHERE time >= '2001-01-04T00:00:00' AND time <= '2001-01-05T01:00:00'
GROUP BY MetricMinuteTs
ORDER BY MetricMinuteTs DESC;

-- Test partitioning function on an open (time) dimension
CREATE OR REPLACE FUNCTION unix_to_timestamp(unixtime float8)
    RETURNS TIMESTAMPTZ LANGUAGE SQL IMMUTABLE AS
$BODY$
    SELECT to_timestamp(unixtime);
$BODY$;
CREATE TABLE hyper_timefunc(time float8 NOT NULL, metricid int, VALUE double precision, time_date DATE);

SELECT create_hypertable('hyper_timefunc', 'time', chunk_time_interval => interval '20 day', create_default_indexes=>FALSE, time_partitioning_func => 'unix_to_timestamp');
INSERT INTO hyper_timefunc SELECT time_int, metricid, VALUE, time_date FROM hyper;

ANALYZE metric;
ANALYZE hyper;
ANALYZE regular;
ANALYZE hyper_timefunc;
