-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\set PREFIX 'EXPLAIN (COSTS OFF) '

CREATE TABLE order_test(time int NOT NULL, device_id int, value float);
CREATE INDEX ON order_test(time,device_id);
CREATE INDEX ON order_test(device_id,time);

SELECT create_hypertable('order_test','time',chunk_time_interval:=1000);

INSERT INTO order_test SELECT 0,10,0.5;
INSERT INTO order_test SELECT 1,9,0.5;
INSERT INTO order_test SELECT 2,8,0.5;

-- we want to see here that index scans are possible for the chosen expressions
-- so we disable seqscan so we dont need to worry about other factors which would
-- make PostgreSQL prefer seqscan over index scan
SET enable_seqscan TO off;

-- test sort optimization with single member order by
SELECT time_bucket(10,time),device_id,value FROM order_test ORDER BY 1;
-- should use index scan
:PREFIX SELECT time_bucket(10,time),device_id,value FROM order_test ORDER BY 1;

-- test sort optimization with ordering by multiple columns and time_bucket not last
SELECT time_bucket(10,time),device_id,value FROM order_test ORDER BY 1,2;
-- must not use index scan
:PREFIX SELECT time_bucket(10,time),device_id,value FROM order_test ORDER BY 1,2;

-- test sort optimization with ordering by multiple columns and time_bucket as last member
SELECT time_bucket(10,time),device_id,value FROM order_test ORDER BY 2,1;
-- should use index scan
:PREFIX SELECT time_bucket(10,time),device_id,value FROM order_test ORDER BY 2,1;

