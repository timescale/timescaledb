CREATE TABLE PUBLIC.hyper_1 (
  time TIMESTAMP NOT NULL,
  series_0 DOUBLE PRECISION NULL,
  series_1 DOUBLE PRECISION NULL,
  series_2 DOUBLE PRECISION NULL
);

CREATE INDEX "time_plain" ON PUBLIC.hyper_1 (time DESC, series_0);
SELECT * FROM create_hypertable('"public"."hyper_1"'::regclass, 'time'::name, number_partitions => 1, create_default_indexes=>false);
INSERT INTO hyper_1 SELECT to_timestamp(ser), ser, ser+10000, sqrt(ser::numeric) FROM generate_series(0,10000) ser;
INSERT INTO hyper_1 SELECT to_timestamp(ser), ser, ser+10000, sqrt(ser::numeric) FROM generate_series(10001,20000) ser;

CREATE TABLE PUBLIC.hyper_1_tz (
  time TIMESTAMPTZ NOT NULL,
  series_0 DOUBLE PRECISION NULL,
  series_1 DOUBLE PRECISION NULL,
  series_2 DOUBLE PRECISION NULL
);

CREATE INDEX "time_plain_tz" ON PUBLIC.hyper_1_tz (time DESC, series_0);
SELECT * FROM create_hypertable('"public"."hyper_1_tz"'::regclass, 'time'::name, number_partitions => 1, create_default_indexes=>false);
INSERT INTO hyper_1_tz SELECT to_timestamp(ser), ser, ser+10000, sqrt(ser::numeric) FROM generate_series(0,10000) ser;
INSERT INTO hyper_1_tz SELECT to_timestamp(ser), ser, ser+10000, sqrt(ser::numeric) FROM generate_series(10001,20000) ser;

CREATE TABLE PUBLIC.hyper_1_int (
  time int NOT NULL,
  series_0 DOUBLE PRECISION NULL,
  series_1 DOUBLE PRECISION NULL,
  series_2 DOUBLE PRECISION NULL
);

CREATE INDEX "time_plain_int" ON PUBLIC.hyper_1_int (time DESC, series_0);
SELECT * FROM create_hypertable('"public"."hyper_1_int"'::regclass, 'time'::name, number_partitions => 1, chunk_time_interval=>10000, create_default_indexes=>FALSE);
INSERT INTO hyper_1_int SELECT ser, ser, ser+10000, sqrt(ser::numeric) FROM generate_series(0,10000) ser;
INSERT INTO hyper_1_int SELECT ser, ser, ser+10000, sqrt(ser::numeric) FROM generate_series(10001,20000) ser;

CREATE TABLE PUBLIC.plain_table (
  time TIMESTAMPTZ NOT NULL,
  series_0 DOUBLE PRECISION NULL,
  series_1 DOUBLE PRECISION NULL,
  series_2 DOUBLE PRECISION NULL
);

CREATE INDEX "time_plain_plain_table" ON PUBLIC.plain_table (time DESC, series_0);
INSERT INTO plain_table SELECT to_timestamp(ser), ser, ser+10000, sqrt(ser::numeric) FROM generate_series(0,10000) ser;
INSERT INTO plain_table SELECT to_timestamp(ser), ser, ser+10000, sqrt(ser::numeric) FROM generate_series(10001,20000) ser;
ANALYZE;



--non-aggregates use MergeAppend in both optimized and non-optimized
EXPLAIN (costs off) SELECT * FROM hyper_1 ORDER BY "time" DESC limit 2;
SELECT * FROM hyper_1 ORDER BY "time" DESC limit 2;

--aggregates use MergeAppend only in optimized
EXPLAIN (costs off) SELECT date_trunc('minute', time) t, avg(series_0), min(series_1), avg(series_2) FROM hyper_1 GROUP BY t ORDER BY t DESC limit 2;

--the minute and second results should be diff
SELECT date_trunc('minute', time) t, avg(series_0), min(series_1), avg(series_2) FROM hyper_1 GROUP BY t ORDER BY t DESC limit 2;
SELECT date_trunc('second', time) t, avg(series_0), min(series_1), avg(series_2) FROM hyper_1 GROUP BY t ORDER BY t DESC limit 2;

--test that when index on time used by constraint, still works correctly
EXPLAIN (costs off)
SELECT date_trunc('minute', time) t, avg(series_0), min(series_1), avg(series_2) 
FROM hyper_1 
WHERE time < to_timestamp(900) 
GROUP BY t 
ORDER BY t DESC 
LIMIT 2;

SELECT date_trunc('minute', time) t, avg(series_0), min(series_1), avg(series_2) 
FROM hyper_1 
WHERE time < to_timestamp(900) 
GROUP BY t 
ORDER BY t DESC 
LIMIT 2;

--test that still works with an expression index on data_trunc.
DROP INDEX "time_plain";
CREATE INDEX "time_trunc" ON PUBLIC.hyper_1 (date_trunc('minute', time));
ANALYZE;
EXPLAIN (costs off) SELECT date_trunc('minute', time) t, avg(series_0), min(series_1), avg(series_2) FROM hyper_1 GROUP BY t ORDER BY t DESC limit 2;
SELECT date_trunc('minute', time) t, avg(series_0), min(series_1), avg(series_2) FROM hyper_1 GROUP BY t ORDER BY t DESC limit 2;

--test that works with both indexes
CREATE INDEX "time_plain" ON PUBLIC.hyper_1 (time DESC, series_0);
ANALYZE;
EXPLAIN (costs off) SELECT date_trunc('minute', time) t, avg(series_0), min(series_1), avg(series_2) FROM hyper_1 GROUP BY t ORDER BY t DESC limit 2;
SELECT date_trunc('minute', time) t, avg(series_0), min(series_1), avg(series_2) FROM hyper_1 GROUP BY t ORDER BY t DESC limit 2;

EXPLAIN (costs off) SELECT time_bucket('1 minute', time) t, avg(series_0), min(series_1), avg(series_2) 
FROM hyper_1 GROUP BY t ORDER BY t DESC limit 2;
SELECT time_bucket('1 minute', time) t, avg(series_0), min(series_1), avg(series_2) 
FROM hyper_1 GROUP BY t ORDER BY t DESC limit 2;

EXPLAIN (costs off) SELECT time_bucket('1 minute', time, INTERVAL '30 seconds') t, avg(series_0), min(series_1), avg(series_2) 
FROM hyper_1 GROUP BY t ORDER BY t DESC limit 2;
SELECT time_bucket('1 minute', time, INTERVAL '30 seconds') t, avg(series_0), min(series_1), avg(series_2) 
FROM hyper_1 GROUP BY t ORDER BY t DESC limit 2;

EXPLAIN (costs off) SELECT time_bucket('1 minute', time - INTERVAL '30 seconds') t, avg(series_0), min(series_1), avg(series_2) 
FROM hyper_1 GROUP BY t ORDER BY t DESC limit 2;
SELECT time_bucket('1 minute', time - INTERVAL '30 seconds') t, avg(series_0), min(series_1), avg(series_2) 
FROM hyper_1 GROUP BY t ORDER BY t DESC limit 2;


EXPLAIN (costs off) SELECT time_bucket('1 minute', time - INTERVAL '30 seconds') + INTERVAL '30 seconds' t, avg(series_0), min(series_1), avg(series_2) 
FROM hyper_1 GROUP BY t ORDER BY t DESC limit 2;
SELECT time_bucket('1 minute', time - INTERVAL '30 seconds') + INTERVAL '30 seconds' t, avg(series_0), min(series_1), avg(series_2) 
FROM hyper_1 GROUP BY t ORDER BY t DESC limit 2;


EXPLAIN (costs off) SELECT time_bucket('1 minute', time) t, avg(series_0), min(series_1), avg(series_2) 
FROM hyper_1_tz GROUP BY t ORDER BY t DESC limit 2;
SELECT time_bucket('1 minute', time) t, avg(series_0), min(series_1), avg(series_2) 
FROM hyper_1_tz GROUP BY t ORDER BY t DESC limit 2;

EXPLAIN (costs off) SELECT time_bucket('1 minute', time::timestamp) t, avg(series_0), min(series_1), avg(series_2) 
FROM hyper_1_tz GROUP BY t ORDER BY t DESC limit 2;
SELECT time_bucket('1 minute', time::timestamp) t, avg(series_0), min(series_1), avg(series_2) 
FROM hyper_1_tz GROUP BY t ORDER BY t DESC limit 2;

EXPLAIN (costs off) SELECT time_bucket(10, time) t, avg(series_0), min(series_1), avg(series_2) 
FROM hyper_1_int GROUP BY t ORDER BY t DESC limit 2;
SELECT time_bucket(10, time) t, avg(series_0), min(series_1), avg(series_2) 
FROM hyper_1_int GROUP BY t ORDER BY t DESC limit 2;

EXPLAIN (costs off) SELECT time_bucket(10, time, 2) t, avg(series_0), min(series_1), avg(series_2) 
FROM hyper_1_int GROUP BY t ORDER BY t DESC limit 2;
SELECT time_bucket(10, time) t, avg(series_0), min(series_1), avg(series_2) 
FROM hyper_1_int GROUP BY t ORDER BY t DESC limit 2;


--plain tables shouldnt be optimized by default
EXPLAIN (costs off)
SELECT date_trunc('minute', time) t, avg(series_0), min(series_1), avg(series_2) 
FROM plain_table 
WHERE time < to_timestamp(900) 
GROUP BY t 
ORDER BY t DESC 
LIMIT 2;

SELECT date_trunc('minute', time) t, avg(series_0), min(series_1), avg(series_2) 
FROM plain_table 
WHERE time < to_timestamp(900) 
GROUP BY t 
ORDER BY t DESC 
LIMIT 2;


--can turn on plain table optimizations
BEGIN;
    SET LOCAL timescaledb.optimize_non_hypertables = 'on';
    EXPLAIN (costs off)
    SELECT date_trunc('minute', time) t, avg(series_0), min(series_1), avg(series_2) 
    FROM plain_table 
    WHERE time < to_timestamp(900) 
    GROUP BY t 
    ORDER BY t DESC 
    LIMIT 2;

    SELECT date_trunc('minute', time) t, avg(series_0), min(series_1), avg(series_2) 
    FROM plain_table 
    WHERE time < to_timestamp(900) 
    GROUP BY t 
    ORDER BY t DESC 
    LIMIT 2;
ROLLBACK;
