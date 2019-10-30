-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

--- TEST for constraint aware append  ------------
--should select only newly added chunk --
set timescaledb.enable_chunk_append to false;
:PREFIX select * from ( SELECT * FROM metrics_ordered_idx where time > '2002-01-01' and time < now() ORDER BY time DESC LIMIT 10 ) as q  order by 1,2,3,4;

-- DecompressChunk path because segmentby columns have equality constraints
:PREFIX select * from (SELECT * FROM metrics_ordered_idx WHERE device_id = 4 AND device_id_peer = 5 and time > '2002-01-01' and time < now() ORDER BY time DESC LIMIT 10) as q  order by 1, 2, 3, 4;

:PREFIX SELECT m.device_id, d.v0, count(*)
FROM metrics_ordered_idx d , metrics_ordered_idx m
WHERE m.device_id = d.device_id AND m.device_id_peer = 5
and m.time = d.time and m.time > '2002-01-01' and m.time < now()
AND m.device_id_peer = d.device_id_peer
group by m.device_id, d.v0 order by 1,2 ,3;

--query with no results --
:PREFIX SELECT m.device_id, d.v0, count(*) 
FROM metrics_ordered_idx d , metrics_ordered_idx m 
WHERE m.time = d.time and  m.time > now()
group by m.device_id, d.v0 order by 1,2 ,3;

--query with all chunks but 1 excluded at plan time --
:PREFIX SELECT d.*, m.* 
FROM device_tbl d, metrics_ordered_idx m 
WHERE m.device_id = d.device_id 
and m.time > '2019-01-01' and m.time < now()
order by m.v0;

-- no matches in metrics_ordered_idx but one row in device_tbl
:PREFIX SELECT d.*, m.* 
FROM device_tbl d LEFT OUTER JOIN  metrics_ordered_idx m 
ON m.device_id = d.device_id and m.time > '2019-01-01' and m.time < now()
WHERE d.device_id = 8 
order by m.v0;

-- no matches in device_tbl but 1 row in metrics_ordered_idx
:PREFIX SELECT d.*, m.* 
FROM device_tbl d FULL OUTER JOIN  metrics_ordered_idx m 
ON m.device_id = d.device_id and m.time > '2019-01-01' and m.time < now()
WHERE m.device_id = 7 
order by m.v0;


set timescaledb.enable_chunk_append to true;
