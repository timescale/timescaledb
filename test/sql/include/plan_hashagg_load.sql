CREATE TABLE metric (id SERIAL PRIMARY KEY, value INT);
CREATE TABLE hyper(time TIMESTAMP, time_int BIGINT, time_broken DATE, metricid int REFERENCES metric(id), VALUE double precision);
CREATE TABLE regular(time TIMESTAMP, time_int BIGINT, time_date DATE, metricid int REFERENCES metric(id), VALUE double precision);

SELECT create_hypertable('hyper', 'time', chunk_time_interval => interval '20 day', create_default_indexes=>FALSE);

ALTER TABLE hyper
DROP COLUMN time_broken,
ADD COLUMN time_date DATE;

INSERT INTO metric(value) SELECT random()*100 FROM generate_series(0,10);

INSERT INTO hyper SELECT t,  EXTRACT(EPOCH FROM t), (random() * 10)+1, 1.0, t::date FROM generate_series('2001-01-01', '2001-01-10', INTERVAL '1 second') t;
INSERT INTO regular(time, time_int, time_date, metricid, value)
  SELECT t,  EXTRACT(EPOCH FROM t), t::date, (random() * 10)+1, 1.0 FROM generate_series('2001-01-01', '2001-01-02', INTERVAL '1 second') t;

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

SET client_min_messages = 'error';
ANALYZE;
RESET client_min_messages;
