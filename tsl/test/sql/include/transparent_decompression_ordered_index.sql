-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SET work_mem TO '50MB';

---Lets test for index backward scans instead of seq scans ------------
-- for ordered append tests on compressed chunks we need a hypertable with time as compress_orderby column

-- should not have ordered DecompressChunk path because segmentby columns are not part of pathkeys
:PREFIX select * from ( SELECT * FROM metrics_ordered_idx ORDER BY time DESC LIMIT 10 ) as q  order by 1,2,3,4;

-- should have ordered DecompressChunk path because segmentby columns have equality constraints
:PREFIX select * from (SELECT * FROM metrics_ordered_idx WHERE device_id = 3 AND device_id_peer = 3 ORDER BY time DESC LIMIT 10) as q order by 1, 2, 3, 4;

:PREFIX SELECT DISTINCT ON (d.device_id) * FROM metrics_ordered_idx d INNER JOIN LATERAL (SELECT * FROM metrics_ordered_idx m WHERE m.device_id=d.device_id AND m.device_id_peer = 3 ORDER BY time DESC LIMIT 1 ) m ON m.device_id_peer = d.device_id_peer;
:PREFIX SELECT d.device_id, m.time,  m.time
FROM metrics_ordered_idx d INNER JOIN LATERAL (SELECT * FROM metrics_ordered_idx m WHERE m.device_id=d.device_id AND m.device_id_peer = 3 ORDER BY time DESC LIMIT 1 ) m ON m.device_id_peer = d.device_id_peer;

--github issue 1558
set enable_seqscan = false;
set enable_bitmapscan = false;
set max_parallel_workers_per_gather = 0;
set enable_hashjoin = false;
set enable_mergejoin = false;

:PREFIX select device_id, count(*) from
(select * from metrics_ordered_idx mt, nodetime nd 
where mt.time > nd.start_time and mt.device_id = nd.node and mt.time < nd.stop_time) as subq group by device_id;

:PREFIX select nd.node, mt.* from metrics_ordered_idx mt, nodetime nd 
where mt.time > nd.start_time and mt.device_id = nd.node and mt.time < nd.stop_time order by time;
set enable_seqscan = true;
set enable_bitmapscan = true;
set enable_seqscan = true;
set enable_bitmapscan = true;
set max_parallel_workers_per_gather = 0;

set enable_mergejoin = true;
set enable_hashjoin = false;

:PREFIX select nd.node, mt.* from metrics_ordered_idx mt, nodetime nd 
where mt.time > nd.start_time and mt.device_id = nd.node and mt.time < nd.stop_time order by time;

set enable_mergejoin = false;
set enable_hashjoin = true;
:PREFIX select nd.node, mt.* from metrics_ordered_idx mt, nodetime nd 
where mt.time > nd.start_time and mt.device_id = nd.node and mt.time < nd.stop_time order by time;

--enable all joins after the tests
set enable_mergejoin = true;
set enable_hashjoin = true;
 --end github issue 1558
