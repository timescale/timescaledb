-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SELECT value FROM metrics ORDER BY time LIMIT 5;
SELECT time, device, value FROM metrics ORDER BY time DESC LIMIT 3;
SELECT value * 2 AS v2, device FROM metrics ORDER BY time LIMIT 4;  -- expression
SELECT device, value FROM metrics ORDER BY time OFFSET 3 LIMIT 4;  -- offset
SELECT metrics FROM metrics ORDER BY time LIMIT 2;  -- whole row
SELECT value FROM metrics ORDER BY time LIMIT (SELECT 5);  -- non-const limit
SELECT value FROM metrics_compressed ORDER BY time LIMIT 5;  -- compressed
SELECT metrics_compressed FROM metrics_compressed ORDER BY time LIMIT 2;  -- compressed whole row

-- unordered full reads, compared through an order-independent aggregate
SELECT count(*), sum(value) FROM (SELECT value FROM metrics LIMIT 1000) x;
SELECT count(*), sum(value) FROM (SELECT value FROM metrics_space LIMIT 1000) x;
