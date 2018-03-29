:PREFIX SELECT time_bucket('1 minute', time) AS MetricMinuteTs, AVG(value) as avg
FROM hyper
WHERE time >= '2001-01-04T00:00:00' AND time <= '2001-01-05T01:00:00'
GROUP BY MetricMinuteTs
ORDER BY MetricMinuteTs DESC;

:PREFIX SELECT time_bucket('1 hour', time) AS MetricMinuteTs, metricid, AVG(value) as avg
FROM hyper
WHERE time >= '2001-01-04T00:00:00' AND time <= '2001-01-05T01:00:00'
GROUP BY MetricMinuteTs, metricid
ORDER BY MetricMinuteTs DESC, metricid;

--should be too many groups will not hashaggregate
:PREFIX SELECT time_bucket('1 second', time) AS MetricMinuteTs, metricid, AVG(value) as avg
FROM hyper
WHERE time >= '2001-01-04T00:00:00' AND time <= '2001-01-05T01:00:00'
GROUP BY MetricMinuteTs, metricid
ORDER BY MetricMinuteTs DESC, metricid;

:PREFIX SELECT time_bucket('1 minute', time, '30 seconds') AS MetricMinuteTs, AVG(value) as avg
FROM hyper
WHERE time >= '2001-01-04T00:00:00' AND time <= '2001-01-05T01:00:00'
GROUP BY MetricMinuteTs
ORDER BY MetricMinuteTs DESC;

:PREFIX SELECT time_bucket(60, time_int) AS MetricMinuteTs, AVG(value) as avg
FROM hyper
WHERE time >= '2001-01-04T00:00:00' AND time <= '2001-01-05T01:00:00'
GROUP BY MetricMinuteTs
ORDER BY MetricMinuteTs DESC;

:PREFIX SELECT time_bucket(60, time_int, 10) AS MetricMinuteTs, AVG(value) as avg
FROM hyper
WHERE time >= '2001-01-04T00:00:00' AND time <= '2001-01-05T01:00:00'
GROUP BY MetricMinuteTs
ORDER BY MetricMinuteTs DESC;

:PREFIX SELECT time_bucket('1 day', time_date) AS MetricMinuteTs, AVG(value) as avg
FROM hyper
WHERE time >= '2001-01-04T00:00:00' AND time <= '2001-01-05T01:00:00'
GROUP BY MetricMinuteTs
ORDER BY MetricMinuteTs DESC;

:PREFIX SELECT date_trunc('minute', time) AS MetricMinuteTs, AVG(value) as avg
FROM hyper
WHERE time >= '2001-01-04T00:00:00' AND time <= '2001-01-05T01:00:00'
GROUP BY MetricMinuteTs
ORDER BY MetricMinuteTs DESC;

\set ON_ERROR_STOP 0
--can't optimize invalid time unit
:PREFIX SELECT date_trunc('invalid', time) AS MetricMinuteTs, AVG(value) as avg
FROM hyper
WHERE time >= '2001-01-04T00:00:00' AND time <= '2001-01-05T01:00:00'
GROUP BY MetricMinuteTs
ORDER BY MetricMinuteTs DESC;
\set ON_ERROR_STOP 1

:PREFIX SELECT date_trunc('day', time_date) AS MetricMinuteTs, AVG(value) as avg
FROM hyper
WHERE time >= '2001-01-04T00:00:00' AND time <= '2001-01-05T01:00:00'
GROUP BY MetricMinuteTs
ORDER BY MetricMinuteTs DESC;

--joins
--with hypertable, optimize
:PREFIX SELECT time_bucket(3600, time_int, 10) AS MetricMinuteTs, metric.value, AVG(hyper.value) as avg
FROM hyper
JOIN metric ON (hyper.metricid = metric.id)
WHERE time >= '2001-01-04T00:00:00' AND time <= '2001-01-05T01:00:00'
GROUP BY MetricMinuteTs, metric.id
ORDER BY MetricMinuteTs DESC, metric.id;

--no hypertable involved, no optimization
:PREFIX SELECT time_bucket(3600, time_int, 10) AS MetricMinuteTs, metric.value, AVG(regular.value) as avg
FROM regular
JOIN metric ON (regular.metricid = metric.id)
WHERE time >= '2001-01-04T00:00:00' AND time <= '2001-01-05T01:00:00'
GROUP BY MetricMinuteTs, metric.id
ORDER BY MetricMinuteTs DESC, metric.id;


