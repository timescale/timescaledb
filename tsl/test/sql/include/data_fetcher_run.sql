-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

ANALYZE disttable;

SELECT count(*) FROM disttable;

SELECT time_bucket('1 hour', time) AS time, device, avg(temp)
FROM disttable
GROUP BY 1,2
ORDER BY 1,2;
