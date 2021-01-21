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
SET enable_mergejoin = TRUE;

SET enable_hashjoin = TRUE;

--end github issue 1558

-- github issue 2673 
-- nested loop join with parameterized path
-- join condition has a segment by column and another column.
SET enable_hashjoin = false;
SET enable_mergejoin=false;
SET enable_material = false;
SET enable_seqscan = false;

-- restrict so that we select only 1 chunk.
:PREFIX
WITH lookup as ( SELECT * from (values( 3, 5) , (3, 4) ) as lu( did, version) )
SELECT met.*, lookup.*
FROM metrics_ordered_idx met join lookup
ON met.device_id = lookup.did and met.v0 = lookup.version
WHERE met.time > '2000-01-19 19:00:00-05' 
      and met.time < '2000-01-20 20:00:00-05'; 

--add filter to segment by (device_id) and compressed attr column (v0)
:PREFIX
WITH lookup as ( SELECT * from (values( 3, 5) , (3, 4) ) as lu( did, version) )
SELECT met.*, lookup.*
FROM metrics_ordered_idx met join lookup
ON met.device_id = lookup.did and met.v0 = lookup.version
WHERE met.time > '2000-01-19 19:00:00-05' 
      and met.time < '2000-01-20 20:00:00-05' 
      and met.device_id = 3 and met.v0 = 5;

:PREFIX
WITH lookup as ( SELECT * from (values( 3, 5) , (3, 4) ) as lu( did, version) )
SELECT met.*, lookup.*
FROM metrics_ordered_idx met join lookup
ON met.device_id = lookup.did and met.v0 = lookup.version 
WHERE met.time = '2000-01-19 19:00:00-05' 
      and met.device_id = 3
      and met.device_id_peer = 3 and met.v0 = 5;

-- lateral subquery
:PREFIX
WITH f1 as ( SELECT * from (values( 7, 5, 4) , (4, 5, 5) ) as lu( device_id, device_id_peer, v0) )
SELECT * FROM  metrics_ordered_idx met 
JOIN LATERAL
  ( SELECT node, f1.* from nodetime , f1
    WHERE  node = f1.device_id) q
ON met.device_id = q.node and met.device_id_peer = q.device_id_peer 
   and met.v0 = q.v0 and met.v0 > 2 and time = '2018-01-19 20:00:00-05';

-- filter on compressed attr (v0) with seqscan enabled and indexscan 
-- disabled. filters on compressed attr should be above the seq scan.
SET enable_seqscan = true;
SET enable_indexscan = false;
:PREFIX
WITH lookup as ( SELECT * from (values( 3, 5) , (3, 4) ) as lu( did, version) )
SELECT met.*, lookup.*
FROM metrics_ordered_idx met join lookup
ON met.device_id = lookup.did and met.v0 = lookup.version 
   and met.device_id = 3
WHERE met.time > '2000-01-19 19:00:00-05' 
      and met.time < '2000-01-20 20:00:00-05' 
      and met.device_id = 3
      and met.device_id_peer = 3 and met.v0 = 5;

RESET enable_hashjoin  ;
RESET enable_mergejoin;
RESET enable_material ;
RESET enable_indexscan ;
--end github issue 2673 
