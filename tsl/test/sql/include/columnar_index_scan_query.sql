-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- canary for test diff
SHOW timescaledb.enable_columnarindexscan;

-- simple query with count without grouping
:PREFIX SELECT device, count(*) FROM metrics GROUP BY device;

-- simple query with count with grouping
:PREFIX SELECT device, count(*) FROM metrics GROUP BY device ORDER BY device;

-- simple query with multiple count with grouping
:PREFIX SELECT device, count(*), count(*) FROM metrics GROUP BY device ORDER BY device;

-- simple query with 1 max aggregate that can use optimization
:PREFIX SELECT device, max(time) FROM metrics GROUP BY device;

-- simple query with 1 min aggregate that can use optimization
:PREFIX SELECT sensor, min(time) FROM metrics GROUP BY sensor ORDER BY sensor;

-- simple query with 1 first aggregate that can use optimization
:PREFIX SELECT device, first(time, time) FROM metrics GROUP BY device ORDER BY device;

-- simple query with 1 last aggregate that can use optimization
:PREFIX SELECT device, last(time, time) FROM metrics GROUP BY device ORDER BY device;

-- explicit index columns dont prevent optimization
:PREFIX SELECT sensor, min(value) FROM metrics GROUP BY sensor ORDER BY sensor;

:PREFIX SELECT sensor, min(value), max(value) FROM metrics GROUP BY sensor ORDER BY sensor;

-- multiple aggregates on same column
:PREFIX SELECT sensor, min(time), max(time), first(time,time), last(time,time) FROM metrics GROUP BY sensor ORDER BY sensor;

-- same aggregate on multiple columns
:PREFIX SELECT sensor, min(time), min(value)  FROM metrics GROUP BY sensor ORDER BY sensor;

-- same aggregate on multiple columns but different order
:PREFIX SELECT sensor, min(value), min(time), sensor  FROM metrics GROUP BY sensor ORDER BY sensor;

-- multiple aggregates on multiple columns
:PREFIX SELECT sensor, first(time,time), last(time,time), first(value, value), last(value, value)  FROM metrics GROUP BY sensor ORDER BY sensor;

-- test multiple group by columns
:PREFIX SELECT device, sensor, max(time) FROM metrics GROUP BY device,sensor ORDER BY device,sensor;

-- filter on segmentby allows optimization
:PREFIX SELECT device, min(time) FROM metrics WHERE device IN ('d1','d2') GROUP BY device;
:PREFIX SELECT device, min(time) FROM metrics WHERE device =ANY(ARRAY['d1','d2']) AND sensor='B' GROUP BY device;

-- HAVING on segmentby (pushed down to lower scan)
:PREFIX SELECT device, min(time) FROM metrics GROUP BY device HAVING device IN ('d1','d2');
:PREFIX SELECT device, min(time) FROM metrics GROUP BY device,sensor HAVING device =ANY(ARRAY['d1','d2']) AND sensor='B';

-- HAVING with aggregate function
:PREFIX SELECT device, min(value) FROM metrics GROUP BY device HAVING min(value) > 20;
:PREFIX SELECT device, max(value) FROM metrics GROUP BY device HAVING min(value) < 25;

-- HAVING
:PREFIX SELECT device, min(time) FROM metrics GROUP BY device HAVING min(time) > '2025-01-01 00:30:00 PST';

-- tableoid doesnt prevent optimization
:PREFIX SELECT tableoid, device, max(time) FROM metrics GROUP BY device, tableoid;
:PREFIX SELECT device, max(time), min(tableoid) FROM metrics GROUP BY device ORDER BY device;

-- multiple aggregates on same column use optimization
:PREFIX SELECT device, min(time), max(time) FROM metrics GROUP BY device;

-- multiple aggregates on same column with first/last
:PREFIX SELECT device, min(time), first(time, time), last(time, time), max(time) FROM metrics GROUP BY device;

-- segmentby column at end of targetlist
:PREFIX SELECT first(time, time), last(time, time), device FROM metrics GROUP BY device;

-- multiple aggregates on different columns use optimization
:PREFIX SELECT device, min(time), max(value) FROM metrics GROUP BY device;

-- multiple aggregates on same non-time column use optimization
:PREFIX SELECT device, min(value), max(value) FROM metrics GROUP BY device;

-- multiple aggregates on multiple columns
:PREFIX SELECT device, min(time), max(time), min(value), max(value) FROM metrics GROUP BY device;

-- expression on aggregates
:PREFIX SELECT device, max(time), min(time), max(time) - min(time)  FROM metrics GROUP BY device;

-- test with sort
:PREFIX SELECT device, sensor, first(time,time), last(time,time) from metrics group by device, sensor order by 1,2;

-- test with subquery (SubqueryScan)
:PREFIX SELECT * FROM (SELECT device, min(time), max(time) FROM metrics GROUP BY device) sub ORDER BY device;

:PREFIX SELECT mx, mn, device FROM (SELECT device, min(time) mn, max(time) mx FROM metrics GROUP BY device) sub ORDER BY device;

-- test with CTE (also uses SubqueryScan)
:PREFIX WITH agg_data AS (
    SELECT device, min(time) as min_time, max(time) as max_time FROM metrics GROUP BY device
)
SELECT * FROM agg_data ORDER BY device;

-- test parallel queries (not supported with ColumnarIndexScan)
SELECT set_config(CASE WHEN current_setting('server_version_num')::int < 160000 THEN 'force_parallel_mode' ELSE 'debug_parallel_query' END,'on', false);
:PREFIX SELECT device, min(time), max(time) FROM metrics GROUP BY device ORDER BY device;
SELECT set_config(CASE WHEN current_setting('server_version_num')::int < 160000 THEN 'force_parallel_mode' ELSE 'debug_parallel_query' END,'off', false);

-- test with UNION ALL (Append node)
:PREFIX SELECT device, min(time) FROM metrics WHERE device = 'd1' GROUP BY device
UNION ALL
SELECT device, min(time) FROM metrics WHERE device = 'd2' GROUP BY device
ORDER BY device;

:PREFIX SELECT device, min(time), max(time) FROM metrics GROUP BY device
UNION ALL
SELECT device, max(time), min(time) FROM metrics GROUP BY device ORDER BY 1,2,3;

-- test WINDOW functions
:PREFIX SELECT device, max(time), lead(device) OVER (PARTITION BY max(time)) from metrics GROUP BY device ORDER BY device;
:PREFIX SELECT device, max(time), lead(device) OVER (PARTITION BY device) from metrics GROUP BY device ORDER BY device;

-- currently unoptimized queries

-- count on dimension column (could be optimized but currently is not)
:PREFIX SELECT count(time) FROM metrics;

-- count with segmentby column
:PREFIX SELECT count(device) FROM metrics;

-- count with other columns
:PREFIX SELECT count(value) FROM metrics;

-- group by on non-segmentby prevents optimization
:PREFIX SELECT max(time) FROM metrics GROUP BY value;
:PREFIX SELECT device, max(time) FROM metrics GROUP BY device,value;

-- unsupported aggregates on any column
:PREFIX SELECT avg(value) FROM metrics;
:PREFIX SELECT string_agg(device, ',') FROM metrics;

-- aggregate on segmentby column does not use optimization
:PREFIX SELECT device, min(sensor) FROM metrics GROUP BY device;

-- aggregate on column without metadata
:PREFIX SELECT min(value2) FROM metrics GROUP BY device ORDER BY device;

-- aggregates with FILTER clause
:PREFIX SELECT count(*) FILTER(WHERE device='dev 1') FROM metrics;
:PREFIX SELECT min(value) FILTER(WHERE device='dev 1') FROM metrics;

-- aggregates with DISTINCT
:PREFIX SELECT count(DISTINCT device) FROM metrics;

-- aggregates with ORDER
:PREFIX SELECT min(value ORDER BY time) FROM metrics;

-- WHERE clause on non-segmentby column
:PREFIX SELECT count(*) FROM metrics WHERE value > 10;

-- first/last with different arg1 and arg2
:PREFIX SELECT first(value, time) FROM metrics GROUP BY device ORDER BY device;
:PREFIX SELECT last(value, time) FROM metrics GROUP BY device ORDER BY device;

