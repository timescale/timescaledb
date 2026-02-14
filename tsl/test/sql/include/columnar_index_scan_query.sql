-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- canary for test diff
SHOW timescaledb.enable_columnarindexscan;

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

-- order by does currently prevent optimization
:PREFIX SELECT device, max(time) FROM metrics GROUP BY device ORDER BY device;

-- filter on segmentby allows optimization
:PREFIX SELECT device, min(time) FROM metrics WHERE device IN ('d1','d2') GROUP BY device;
:PREFIX SELECT device, min(time) FROM metrics WHERE device =ANY(ARRAY['d1','d2']) AND sensor='B' GROUP BY device;

-- filter on non-segmentby prevents optimization
:PREFIX SELECT device, max(time) FROM metrics WHERE time <> '2025-01-01'  GROUP BY device;

-- tableoid doesnt prevent optimization
--:PREFIX SELECT tableoid, device, max(time) FROM metrics GROUP BY device, tableoid;

-- group by on non-segmentby prevents optimization
:PREFIX SELECT max(time) FROM metrics GROUP BY value;
:PREFIX SELECT device, max(time) FROM metrics GROUP BY device,value;

-- no group by prevents optimization
:PREFIX SELECT max(time) FROM metrics;

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

-- expression on aggregates (currently not optimized)
:PREFIX SELECT device, max(time), min(time), max(time) - min(time)  FROM metrics GROUP BY device;

-- aggregate on segmentby column does not use optimization
:PREFIX SELECT device, min(sensor) FROM metrics GROUP BY device;

-- aggregate on column without metadata does not use optimization
:PREFIX SELECT device, min(value2) FROM metrics GROUP BY device;

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

WITH agg_data AS (
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

