-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE TABLE  many_partitions_test(time timestamp, temp float8, device text NOT NULL);
SELECT create_hypertable('many_partitions_test', 'time', 'device', 1000);
--NOTE: how much slower the first two queries are -- they are creating chunks
INSERT INTO many_partitions_test
    SELECT to_timestamp(ser), ser, ser::text FROM generate_series(1,100) ser;
INSERT INTO many_partitions_test
    SELECT to_timestamp(ser), ser, ser::text FROM generate_series(101,200) ser;
INSERT INTO many_partitions_test
    SELECT to_timestamp(ser), ser, (ser-201)::text FROM generate_series(201,300) ser;

SELECT * FROM  many_partitions_test ORDER BY time DESC LIMIT 2;
SELECT count(*) FROM  many_partitions_test;

CREATE TABLE many_partitions_test_1m (time timestamp, temp float8, device text NOT NULL);
SELECT create_hypertable('many_partitions_test_1m', 'time', 'device', 1000);

EXPLAIN (verbose on, costs off)
INSERT INTO many_partitions_test_1m(time, temp, device)
SELECT time_bucket('1 minute', time) AS period, avg(temp), device
FROM many_partitions_test
GROUP BY period, device;

INSERT INTO many_partitions_test_1m(time, temp, device)
SELECT time_bucket('1 minute', time) AS period, avg(temp), device
FROM many_partitions_test
GROUP BY period, device;

SELECT * FROM many_partitions_test_1m ORDER BY time, device LIMIT 10;
