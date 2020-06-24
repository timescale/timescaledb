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
