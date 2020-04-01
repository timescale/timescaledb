-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- github issue #1262
--
:PREFIX
INSERT INTO jit_test VALUES('2017-01-20T09:00:01', 22.5) RETURNING *;

:PREFIX
INSERT INTO jit_test VALUES ('2017-01-20T09:00:02', 2),
                            ('2017-01-20T09:00:03', 5),
                            ('2017-01-20T09:00:04', 10);

:PREFIX
SELECT * FROM jit_test WHERE temp > 5 and temp <= 10 ORDER BY time;

-- update with iteration over chunks
--
:PREFIX
INSERT INTO jit_test_interval (SELECT x, x / 2.3 FROM generate_series(0, 100) x) RETURNING *;

:PREFIX
SELECT * FROM jit_test_interval WHERE id >= 23 and id < 73 ORDER BY id;

:PREFIX
UPDATE jit_test_interval SET temp = temp * 2.3 WHERE id >= 23 and id < 73;

:PREFIX
SELECT * FROM jit_test_interval ORDER BY id;

:PREFIX
SELECT time_bucket(10, id), avg(temp)
FROM jit_test_interval
GROUP BY 1
ORDER BY 1;

-- test continuous aggregates usage with forced jit (based on continuous_aggs_usage.sql)
--
:PREFIX
SELECT * FROM jit_device_summary WHERE metric_spread = 1800 ORDER BY bucket DESC, device_id LIMIT 10;
