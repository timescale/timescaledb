-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SET work_mem TO '50MB';

---Lets test for index backward scans instead of seq scans ------------
-- for ordered append tests on compressed chunks we need a hypertable with time as compress_orderby column
-- should not have ordered DecompressChunk path because segmentby columns are not part of pathkeys

:PREFIX
SELECT *
FROM (
    SELECT *
    FROM metrics_ordered_idx
    ORDER BY time DESC
    LIMIT 10) AS q
ORDER BY 1,
    2,
    3,
    4;

-- should have ordered DecompressChunk path because segmentby columns have equality constraints
:PREFIX
SELECT *
FROM (
    SELECT *
    FROM metrics_ordered_idx
    WHERE device_id = 3
        AND device_id_peer = 3
    ORDER BY time DESC
    LIMIT 10) AS q
ORDER BY 1,
    2,
    3,
    4;

:PREFIX SELECT DISTINCT ON (d.device_id)
    *
FROM metrics_ordered_idx d
    INNER JOIN LATERAL (
        SELECT *
        FROM metrics_ordered_idx m
        WHERE m.device_id = d.device_id
            AND m.device_id_peer = 3
        ORDER BY time DESC
        LIMIT 1) m ON m.device_id_peer = d.device_id_peer
WHERE extract(minute FROM d.time) = 0;

:PREFIX
SELECT d.device_id,
    m.time,
    m.time
FROM metrics_ordered_idx d
    INNER JOIN LATERAL (
        SELECT *
        FROM metrics_ordered_idx m
        WHERE m.device_id = d.device_id
            AND m.device_id_peer = 3
        ORDER BY time DESC
        LIMIT 1) m ON m.device_id_peer = d.device_id_peer
WHERE extract(minute FROM d.time) = 0;

--github issue 1558
SET enable_seqscan = FALSE;

SET enable_bitmapscan = FALSE;

SET max_parallel_workers_per_gather = 0;

SET enable_hashjoin = FALSE;

SET enable_mergejoin = FALSE;

:PREFIX
SELECT device_id,
    count(*)
FROM (
    SELECT *
    FROM metrics_ordered_idx mt,
        nodetime nd
    WHERE mt.time > nd.start_time
        AND mt.device_id = nd.node
        AND mt.time < nd.stop_time) AS subq
GROUP BY device_id;

:PREFIX
SELECT nd.node,
    mt.*
FROM metrics_ordered_idx mt,
    nodetime nd
WHERE mt.time > nd.start_time
    AND mt.device_id = nd.node
    AND mt.time < nd.stop_time
ORDER BY time;

SET enable_seqscan = TRUE;

SET enable_bitmapscan = TRUE;

SET enable_seqscan = TRUE;

SET enable_bitmapscan = TRUE;

SET max_parallel_workers_per_gather = 0;

SET enable_mergejoin = TRUE;

SET enable_hashjoin = FALSE;

:PREFIX
SELECT nd.node,
    mt.*
FROM metrics_ordered_idx mt,
    nodetime nd
WHERE mt.time > nd.start_time
    AND mt.device_id = nd.node
    AND mt.time < nd.stop_time
ORDER BY time;

SET enable_mergejoin = FALSE;

SET enable_hashjoin = TRUE;

:PREFIX
SELECT nd.node,
    mt.*
FROM metrics_ordered_idx mt,
    nodetime nd
WHERE mt.time > nd.start_time
    AND mt.device_id = nd.node
    AND mt.time < nd.stop_time
ORDER BY time;

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
