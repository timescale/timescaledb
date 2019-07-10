-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Bucketing not safe to push down on repartitioned table
:PREFIX
SELECT time_bucket('2 days', time) AS time, device, avg(temp)
FROM :TABLE_NAME
WHERE time BETWEEN '2019-01-01' AND '2019-01-03'
GROUP BY 1,2
ORDER BY 1,2;

-- Query doesn't cover repartitioning boundary, so safe to push down
-- bucketing
:PREFIX
SELECT time_bucket('2 days', time) AS time, device, avg(temp)
FROM :TABLE_NAME
WHERE time BETWEEN '2019-01-01' AND '2019-01-01 15:00'
GROUP BY 1,2
ORDER BY 1,2;
