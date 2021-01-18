-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\set PREFIX 'EXPLAIN (VERBOSE, COSTS OFF)'

-- Create a two dimensional hypertable
CREATE TABLE hyper (time timestamptz, device int, temp float);
SELECT * FROM create_hypertable('hyper', 'time', 'device', 2);

-- Create a similar PostgreSQL partitioned table
CREATE TABLE pg2dim (time timestamptz, device int, temp float) PARTITION BY HASH (device);
CREATE TABLE pg2dim_h1 PARTITION OF pg2dim FOR VALUES WITH (MODULUS 2, REMAINDER 0) PARTITION BY RANGE(time);
CREATE TABLE pg2dim_h2 PARTITION OF pg2dim FOR VALUES WITH (MODULUS 2, REMAINDER 1) PARTITION BY RANGE(time);
CREATE TABLE pg2dim_h1_t1 PARTITION OF pg2dim_h1 FOR VALUES FROM ('2018-01-01 00:00') TO ('2018-09-01 00:00');
CREATE TABLE pg2dim_h1_t2 PARTITION OF pg2dim_h1 FOR VALUES FROM ('2018-09-01 00:00') TO ('2018-12-01 00:00');
CREATE TABLE pg2dim_h2_t1 PARTITION OF pg2dim_h2 FOR VALUES FROM ('2018-01-01 00:00') TO ('2018-09-01 00:00');
CREATE TABLE pg2dim_h2_t2 PARTITION OF pg2dim_h2 FOR VALUES FROM ('2018-09-01 00:00') TO ('2018-12-01 00:00');

-- Create a 1-dimensional partitioned table for comparison
CREATE TABLE pg1dim (time timestamptz, device int, temp float) PARTITION BY HASH (device);
CREATE TABLE pg1dim_h1 PARTITION OF pg1dim FOR VALUES WITH (MODULUS 2, REMAINDER 0);
CREATE TABLE pg1dim_h2 PARTITION OF pg1dim FOR VALUES WITH (MODULUS 2, REMAINDER 1);

INSERT INTO hyper VALUES
       ('2018-02-19 13:01', 1, 2.3),
       ('2018-02-19 13:02', 3, 3.1),
       ('2018-10-19 13:01', 1, 7.6),
       ('2018-10-19 13:02', 3, 9.0);

INSERT INTO pg2dim VALUES
       ('2018-02-19 13:01', 1, 2.3),
       ('2018-02-19 13:02', 3, 3.1),
       ('2018-10-19 13:01', 1, 7.6),
       ('2018-10-19 13:02', 3, 9.0);

INSERT INTO pg1dim VALUES
       ('2018-02-19 13:01', 1, 2.3),
       ('2018-02-19 13:02', 3, 3.1),
       ('2018-10-19 13:01', 1, 7.6),
       ('2018-10-19 13:02', 3, 9.0);

SELECT * FROM test.show_subtables('hyper');

SELECT * FROM pg2dim_h1_t1;
SELECT * FROM pg2dim_h1_t2;
SELECT * FROM pg2dim_h2_t1;
SELECT * FROM pg2dim_h2_t2;


-- Compare partitionwise aggreate enabled/disabled. First run queries
-- on PG partitioned tables for reference.

-- All partition keys covered by GROUP BY
SET enable_partitionwise_aggregate = 'off';
:PREFIX
SELECT device, avg(temp)
FROM pg1dim
GROUP BY 1
ORDER BY 1;

SET enable_partitionwise_aggregate = 'on';
:PREFIX
SELECT device, avg(temp)
FROM pg1dim
GROUP BY 1
ORDER BY 1;

-- All partition keys not covered by GROUP BY (partial partitionwise)
SET enable_partitionwise_aggregate = 'off';
:PREFIX
SELECT device, avg(temp)
FROM pg2dim
GROUP BY 1
ORDER BY 1;

SET enable_partitionwise_aggregate = 'on';
:PREFIX
SELECT device, avg(temp)
FROM pg2dim
GROUP BY 1
ORDER BY 1;

-- All partition keys covered by GROUP BY (full partitionwise)
SET enable_partitionwise_aggregate = 'off';
:PREFIX
SELECT time, device, avg(temp)
FROM pg2dim
GROUP BY 1, 2
ORDER BY 1, 2;

SET enable_partitionwise_aggregate = 'on';
:PREFIX
SELECT time, device, avg(temp)
FROM pg2dim
GROUP BY 1, 2
ORDER BY 1, 2;

-- All partition keys not covered by GROUP BY because of date_trunc
-- expression on time (partial partitionwise)
SET enable_partitionwise_aggregate = 'off';
:PREFIX
SELECT date_trunc('month', time), device, avg(temp)
FROM pg2dim
GROUP BY 1, 2
ORDER BY 1, 2;

SET enable_partitionwise_aggregate = 'on';
:PREFIX
SELECT date_trunc('month', time), device, avg(temp)
FROM pg2dim
GROUP BY 1, 2
ORDER BY 1, 2;

-- Now run on hypertable

-- All partition keys not covered by GROUP BY (partial partitionwise)
SET enable_partitionwise_aggregate = 'off';
:PREFIX
SELECT device, avg(temp)
FROM hyper
GROUP BY 1
ORDER BY 1;

SET enable_partitionwise_aggregate = 'on';
:PREFIX
SELECT device, avg(temp)
FROM hyper
GROUP BY 1
ORDER BY 1;

-- All partition keys covered (full partitionwise)
SET enable_partitionwise_aggregate = 'off';
:PREFIX
SELECT time, device, avg(temp)
FROM hyper
GROUP BY 1, 2
ORDER BY 1, 2;

SET enable_partitionwise_aggregate = 'on';
:PREFIX
SELECT time, device, avg(temp)
FROM hyper
GROUP BY 1, 2
ORDER BY 1, 2;

-- Partial aggregation since date_trunc(time) is not a partition key
SET enable_partitionwise_aggregate = 'off';
:PREFIX
SELECT date_trunc('month', time), device, avg(temp)
FROM hyper
GROUP BY 1, 2
ORDER BY 1, 2;

SET enable_partitionwise_aggregate = 'on';
:PREFIX
SELECT date_trunc('month', time), device, avg(temp)
FROM hyper
GROUP BY 1, 2
ORDER BY 1, 2;

-- Also test time_bucket
SET enable_partitionwise_aggregate = 'off';
:PREFIX
SELECT time_bucket('1 month', time), device, avg(temp)
FROM hyper
GROUP BY 1, 2
ORDER BY 1, 2;

SET enable_partitionwise_aggregate = 'on';
:PREFIX
SELECT time_bucket('1 month', time), device, avg(temp)
FROM hyper
GROUP BY 1, 2
ORDER BY 1, 2;

-- Test partitionwise joins, mostly to see that we do not break
-- anything
CREATE TABLE hyper_meta (time timestamptz, device int, info text);
SELECT * FROM create_hypertable('hyper_meta', 'time', 'device', 2);

INSERT INTO hyper_meta VALUES
       ('2018-02-19 13:01', 1, 'device_1'),
       ('2018-02-19 13:02', 3, 'device_3');

SET enable_partitionwise_join = 'off';

:PREFIX
SELECT h.time, h.device, h.temp, hm.info
FROM hyper h, hyper_meta hm
WHERE h.device = hm.device;

:PREFIX
SELECT pg2.time, pg2.device, pg2.temp, pg1.temp
FROM pg2dim pg2, pg1dim pg1
WHERE pg2.device = pg1.device;

SET enable_partitionwise_join = 'on';

:PREFIX
SELECT h.time, h.device, h.temp, hm.info
FROM hyper h, hyper_meta hm
WHERE h.device = hm.device;

:PREFIX
SELECT pg2.time, pg2.device, pg2.temp, pg1.temp
FROM pg2dim pg2, pg1dim pg1
WHERE pg2.device = pg1.device;

-- Test hypertable with time partitioning function
CREATE OR REPLACE FUNCTION time_func(unixtime float8)
    RETURNS TIMESTAMPTZ LANGUAGE PLPGSQL IMMUTABLE AS
$BODY$
DECLARE
    retval TIMESTAMPTZ;
BEGIN
    retval := to_timestamp(unixtime);
    RETURN retval;
END
$BODY$;

CREATE TABLE hyper_timepart (time float8, device int, temp float);
SELECT * FROM create_hypertable('hyper_timepart', 'time', 'device', 2, time_partitioning_func => 'time_func');

-- Planner won't pick push-down aggs on table with time function
-- unless a certain amount of data
SELECT setseed(1);
INSERT INTO hyper_timepart
SELECT x, ceil(random() * 8), random() * 20
FROM generate_series(0,5000-1) AS x;

-- All partition keys covered (full partitionwise)
SET enable_partitionwise_aggregate = 'off';
:PREFIX
SELECT time, device, avg(temp)
FROM hyper_timepart
GROUP BY 1, 2
ORDER BY 1, 2
LIMIT 10;

:PREFIX
SELECT time_func(time), device, avg(temp)
FROM hyper_timepart
GROUP BY 1, 2
ORDER BY 1, 2
LIMIT 10;

-- Grouping on original time column should be pushed-down
SET enable_partitionwise_aggregate = 'on';
:PREFIX
SELECT time, device, avg(temp)
FROM hyper_timepart
GROUP BY 1, 2
ORDER BY 1, 2
LIMIT 10;

-- Applying the time partitioning function should also allow push-down
-- on open dimensions
:PREFIX
SELECT time_func(time), device, avg(temp)
FROM hyper_timepart
GROUP BY 1, 2
ORDER BY 1, 2
LIMIT 10;

-- Should also work to use partitioning function on closed dimensions
:PREFIX
SELECT time_func(time), _timescaledb_internal.get_partition_hash(device), avg(temp)
FROM hyper_timepart
GROUP BY 1, 2
ORDER BY 1, 2
LIMIT 10;
