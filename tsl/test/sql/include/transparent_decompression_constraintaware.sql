-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

--- TEST for constraint aware append  ------------
--should select only newly added chunk --

SET timescaledb.enable_chunk_append TO FALSE;

:PREFIX
SELECT *
FROM (
    SELECT *
    FROM metrics_ordered_idx
    WHERE time > '2002-01-01'
        AND time < now()
    ORDER BY time DESC
    LIMIT 10) AS q
ORDER BY 1,
    2,
    3,
    4;

-- DecompressChunk path because segmentby columns have equality constraints
:PREFIX
SELECT *
FROM (
    SELECT *
    FROM metrics_ordered_idx
    WHERE device_id = 4
        AND device_id_peer = 5
        AND time > '2002-01-01'
        AND time < now()
    ORDER BY time DESC
    LIMIT 10) AS q
ORDER BY 1,
    2,
    3,
    4;

:PREFIX
SELECT m.device_id,
    d.v0,
    count(*)
FROM metrics_ordered_idx d,
    metrics_ordered_idx m
WHERE m.device_id = d.device_id
    AND m.device_id_peer = 5
    AND m.time = d.time
    AND m.time > '2002-01-01'
    AND m.time < '2000-01-01 0:00:00+0'::text::timestamptz
    AND m.device_id_peer = d.device_id_peer
GROUP BY m.device_id,
    d.v0
ORDER BY 1,
    2,
    3;

--query with no results --
:PREFIX
SELECT m.device_id,
    d.v0,
    count(*)
FROM metrics_ordered_idx d,
    metrics_ordered_idx m
WHERE m.time = d.time
    AND m.time > '2000-01-01 0:00:00+0'::text::timestamptz
GROUP BY m.device_id,
    d.v0
ORDER BY 1,
    2,
    3;

--query with all chunks but 1 excluded at plan time --
:PREFIX
SELECT d.*,
    m.*
FROM device_tbl d,
    metrics_ordered_idx m
WHERE m.device_id = d.device_id
    AND m.time > '2019-01-01'
    AND m.time < '2000-01-01 0:00:00+0'::text::timestamptz
ORDER BY m.v0;

-- no matches in metrics_ordered_idx but one row in device_tbl
:PREFIX
SELECT d.*,
    m.*
FROM device_tbl d
    LEFT OUTER JOIN metrics_ordered_idx m ON m.device_id = d.device_id
    AND m.time > '2019-01-01'
    AND m.time < '2000-01-01 0:00:00+0'::text::timestamptz
WHERE d.device_id = 8
ORDER BY m.v0;

-- no matches in device_tbl but 1 row in metrics_ordered_idx
:PREFIX
SELECT d.*,
    m.*
FROM device_tbl d
    FULL OUTER JOIN metrics_ordered_idx m ON m.device_id = d.device_id
    AND m.time > '2019-01-01'
    AND m.time < '2000-01-01 0:00:00+0'::text::timestamptz
WHERE m.device_id = 7
ORDER BY m.v0;

SET timescaledb.enable_chunk_append TO TRUE;

