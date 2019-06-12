-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

SHOW timescaledb.disable_optimizations;

--non-aggregates use MergeAppend in both optimized and non-optimized
:PREFIX SELECT * FROM hyper_1 ORDER BY "time" DESC limit 2;

:PREFIX SELECT * FROM hyper_timefunc ORDER BY unix_to_timestamp("time") DESC limit 2;

--Aggregates use MergeAppend only in optimized
:PREFIX SELECT date_trunc('minute', time) t, avg(series_0), min(series_1), avg(series_2) FROM hyper_1 GROUP BY t ORDER BY t DESC limit 2;
:PREFIX SELECT date_trunc('minute', time) t, avg(series_0), min(series_1), avg(series_2) FROM hyper_1_date GROUP BY t ORDER BY t DESC limit 2;

--the minute and second results should be diff
:PREFIX SELECT date_trunc('minute', time) t, avg(series_0), min(series_1), avg(series_2) FROM hyper_1 GROUP BY t ORDER BY t DESC limit 2;
:PREFIX SELECT date_trunc('second', time) t, avg(series_0), min(series_1), avg(series_2) FROM hyper_1 GROUP BY t ORDER BY t DESC limit 2;

--test that when index on time used by constraint, still works correctly
:PREFIX
SELECT date_trunc('minute', time) t, avg(series_0), min(series_1), avg(series_2)
FROM hyper_1
WHERE time < to_timestamp(900)
GROUP BY t
ORDER BY t DESC
LIMIT 2;

--test on table with time partitioning function. Currently not
--optimized to use index for ordering since the index is an expression
--on time (e.g., timefunc(time)), and we currently don't handle that
--case.
:PREFIX
SELECT date_trunc('minute', to_timestamp(time)) t, avg(series_0), min(series_1), avg(series_2)
FROM hyper_timefunc
WHERE to_timestamp(time) < to_timestamp(900)
GROUP BY t
ORDER BY t DESC
LIMIT 2;

BEGIN;
  --test that still works with an expression index on data_trunc.
  DROP INDEX "time_plain";
  CREATE INDEX "time_trunc" ON PUBLIC.hyper_1 (date_trunc('minute', time));
  ANALYZE hyper_1;

  :PREFIX SELECT date_trunc('minute', time) t, avg(series_0), min(series_1), avg(series_2) FROM hyper_1 GROUP BY t ORDER BY t DESC limit 2;

  --test that works with both indexes
  CREATE INDEX "time_plain" ON PUBLIC.hyper_1 (time DESC, series_0);
  ANALYZE hyper_1;

  :PREFIX SELECT date_trunc('minute', time) t, avg(series_0), min(series_1), avg(series_2) FROM hyper_1 GROUP BY t ORDER BY t DESC limit 2;

  :PREFIX SELECT time_bucket('1 minute', time) t, avg(series_0), min(series_1), trunc(avg(series_2)::numeric, 5)
  FROM hyper_1 GROUP BY t ORDER BY t DESC limit 2;

  :PREFIX SELECT time_bucket('1 minute', time, INTERVAL '30 seconds') t, avg(series_0), min(series_1), trunc(avg(series_2)::numeric,5)
  FROM hyper_1 GROUP BY t ORDER BY t DESC limit 2;

  :PREFIX SELECT time_bucket('1 minute', time - INTERVAL '30 seconds') t, avg(series_0), min(series_1), trunc(avg(series_2)::numeric,5)
  FROM hyper_1 GROUP BY t ORDER BY t DESC limit 2;

  :PREFIX SELECT time_bucket('1 minute', time - INTERVAL '30 seconds') + INTERVAL '30 seconds' t, avg(series_0), min(series_1), trunc(avg(series_2)::numeric,5)
  FROM hyper_1 GROUP BY t ORDER BY t DESC limit 2;

  :PREFIX SELECT time_bucket('1 minute', time) t, avg(series_0), min(series_1), avg(series_2)
  FROM hyper_1_tz GROUP BY t ORDER BY t DESC limit 2;

  :PREFIX SELECT time_bucket('1 minute', time::timestamp) t, avg(series_0), min(series_1), avg(series_2)
  FROM hyper_1_tz GROUP BY t ORDER BY t DESC limit 2;

  :PREFIX SELECT time_bucket(10, time) t, avg(series_0), min(series_1), avg(series_2)
  FROM hyper_1_int GROUP BY t ORDER BY t DESC limit 2;

  :PREFIX SELECT time_bucket(10, time, 2) t, avg(series_0), min(series_1), avg(series_2)
  FROM hyper_1_int GROUP BY t ORDER BY t DESC limit 2;
ROLLBACK;

--plain tables shouldnt be optimized by default
:PREFIX
SELECT date_trunc('minute', time) t, avg(series_0), min(series_1), avg(series_2)
FROM plain_table
WHERE time < to_timestamp(900)
GROUP BY t
ORDER BY t DESC
LIMIT 2;

--can turn on plain table optimizations
BEGIN;
    SET LOCAL timescaledb.optimize_non_hypertables = 'on';
    :PREFIX
    SELECT date_trunc('minute', time) t, avg(series_0), min(series_1), avg(series_2)
    FROM plain_table
    WHERE time < to_timestamp(900)
    GROUP BY t
    ORDER BY t DESC
    LIMIT 2;
ROLLBACK;
