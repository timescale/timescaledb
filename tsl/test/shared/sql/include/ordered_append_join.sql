-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- test LATERAL with ordered append in the outer query
:PREFIX
SELECT time,
  pg_typeof(l)
FROM :TEST_TABLE,
  LATERAL (
    SELECT *
    FROM (
      VALUES (1),
        (2)) v) l
ORDER BY time DESC
LIMIT 2;

-- test LATERAL with ordered append in the lateral query
:PREFIX
SELECT time,
  pg_typeof(v)
FROM (
  VALUES (1),
    (2)) v,
  LATERAL (
    SELECT *
    FROM :TEST_TABLE
    ORDER BY time DESC
    LIMIT 2) l;

-- test plan with best index is chosen
-- this should use device_id, time index
:PREFIX
SELECT time,
  device_id
FROM :TEST_TABLE
WHERE device_id = 1
ORDER BY time DESC
LIMIT 1;

-- test plan with best index is chosen
-- this should use time index
:PREFIX
SELECT time
FROM :TEST_TABLE
ORDER BY time DESC
LIMIT 1;

-- test LATERAL with correlated query
-- only last chunk should be executed
:PREFIX
SELECT g.time,
  l.time
FROM generate_series('2000-01-01'::timestamptz, '2000-01-03', '1d') AS g (time)
  LEFT OUTER JOIN LATERAL (
  SELECT *
  FROM :TEST_TABLE o
  WHERE o.time >= g.time
    AND o.time < g.time + '1d'::interval
  ORDER BY time DESC
  LIMIT 1) l ON TRUE;

-- test LATERAL with correlated query
-- only 2nd chunk should be executed
:PREFIX
SELECT g.time,
  l.time
FROM generate_series('2000-01-10'::timestamptz, '2000-01-11', '1d') AS g (time)
  LEFT OUTER JOIN LATERAL (
  SELECT *
  FROM :TEST_TABLE o
  WHERE o.time >= g.time
    AND o.time < g.time + '1d'::interval
  ORDER BY time
  LIMIT 1) l ON TRUE;

-- test startup and runtime exclusion together
:PREFIX
SELECT g.time,
  l.time
FROM generate_series('2000-01-01'::timestamptz, '2000-01-03', '1d') AS g (time)
  LEFT OUTER JOIN LATERAL (
  SELECT *
  FROM :TEST_TABLE o
  WHERE o.time >= g.time
    AND o.time < g.time + '1d'::interval
    AND o.time < now()
  ORDER BY time DESC
  LIMIT 1) l ON TRUE;

-- test startup and runtime exclusion together
-- all chunks should be filtered
:PREFIX
SELECT g.time,
  l.time
FROM generate_series('2000-01-01'::timestamptz, '2000-01-03', '1d') AS g (time)
  LEFT OUTER JOIN LATERAL (
  SELECT *
  FROM :TEST_TABLE o
  WHERE o.time >= g.time
    AND o.time < g.time + '1d'::interval
    AND o.time > now()
  ORDER BY time DESC
  LIMIT 1) l ON TRUE;

-- test JOIN
-- no exclusion on joined table because quals are not propagated yet
:PREFIX
SELECT o1.time,
  o2.time
FROM :TEST_TABLE o1
  INNER JOIN :TEST_TABLE o2 ON o1.time = o2.time
WHERE o1.time < '2000-02-01'
  AND o1.device_id = 1
  AND o2.device_id = 2
ORDER BY o1.time;

-- test JOIN
-- last chunk of o2 should not be executed
:PREFIX
SELECT o1.time,
  o2.time
FROM :TEST_TABLE o1
  INNER JOIN (
    SELECT *
    FROM :TEST_TABLE o2
    ORDER BY time) o2 ON o1.time = o2.time
WHERE o1.time < '2000-01-08'
ORDER BY o1.time
LIMIT 10;

-- test join against max query
-- not ChunkAppend so no chunk exclusion
SET enable_hashjoin = FALSE;

:PREFIX
SELECT o1.time,
  o2.*
FROM :TEST_TABLE o1
  INNER JOIN (
    SELECT max(time) AS max_time
    FROM :TEST_TABLE) o2 ON o1.time = o2.max_time
WHERE o1.device_id = 1
ORDER BY time;

RESET enable_hashjoin;

SET enable_seqscan TO false;

-- test JOIN on time column
-- should use 2 ChunkAppend
:PREFIX
SELECT o1.time
FROM :TEST_TABLE o1
  INNER JOIN :TEST_TABLE o2 ON o1.time = o2.time
WHERE o1.device_id = 1
  AND o2.device_id = 2
ORDER BY o1.time
LIMIT 100;

-- test JOIN on time column with USING
-- should use 2 ChunkAppend
:PREFIX
SELECT o1.time
FROM :TEST_TABLE o1
  INNER JOIN :TEST_TABLE o2 USING (time)
WHERE o1.device_id = 1
  AND o2.device_id = 2
ORDER BY o1.time
LIMIT 100;

-- test NATURAL JOIN on time column
-- should use 2 ChunkAppend
:PREFIX
SELECT o1.time
FROM :TEST_TABLE o1
  NATURAL INNER JOIN :TEST_TABLE o2
WHERE o1.device_id = 1
  AND o2.device_id = 2
ORDER BY o1.time
LIMIT 100;

-- test LEFT JOIN on time column
-- should use 2 ChunkAppend
:PREFIX
SELECT o1.time
FROM :TEST_TABLE o1
  LEFT JOIN :TEST_TABLE o2 ON o1.time = o2.time
WHERE o1.device_id = 1
  AND o2.device_id = 2
ORDER BY o1.time
LIMIT 100;

-- test RIGHT JOIN on time column
-- should use 2 ChunkAppend
:PREFIX
SELECT o1.time
FROM :TEST_TABLE o1
  RIGHT JOIN :TEST_TABLE o2 ON o1.time = o2.time
WHERE o1.device_id = 1
  AND o2.device_id = 2
ORDER BY o2.time
LIMIT 100;

-- test JOIN on time column with ON clause expression order switched
-- should use 2 ChunkAppend
:PREFIX
SELECT o1.time
FROM :TEST_TABLE o1
  INNER JOIN :TEST_TABLE o2 ON o2.time = o1.time
WHERE o1.device_id = 1
  AND o2.device_id = 2
ORDER BY o1.time
LIMIT 100;

-- test JOIN on time column with equality condition in WHERE clause
-- should use 2 ChunkAppend
:PREFIX
SELECT o1.time
FROM :TEST_TABLE o1
  INNER JOIN :TEST_TABLE o2 ON TRUE
WHERE o1.time = o2.time
  AND o1.device_id = 1
  AND o2.device_id = 2
ORDER BY o1.time
LIMIT 100;

-- test JOIN on time column with ORDER BY 2nd hypertable
-- should use 2 ChunkAppend
:PREFIX
SELECT o1.time
FROM :TEST_TABLE o1
  INNER JOIN :TEST_TABLE o2 ON o1.time = o2.time
WHERE o1.device_id = 1
  AND o2.device_id = 2
ORDER BY o2.time
LIMIT 100;

-- test JOIN on time column and device_id
-- should use 2 ChunkAppend
:PREFIX
SELECT o1.time
FROM :TEST_TABLE o1
  INNER JOIN :TEST_TABLE o2 ON o1.device_id = o2.device_id
    AND o1.time = o2.time
  ORDER BY o1.time
  LIMIT 100;

-- test JOIN on device_id
-- should not use ordered append for 2nd hypertable
:PREFIX
SELECT o1.time
FROM :TEST_TABLE o1
  INNER JOIN :TEST_TABLE o2 ON o1.device_id = o2.device_id
WHERE o1.device_id = 1
ORDER BY o1.time
LIMIT 100;

-- test JOIN on time column with implicit join
-- should use 2 ChunkAppend
:PREFIX
SELECT o1.time
FROM :TEST_TABLE o1,
  :TEST_TABLE o2
WHERE o1.time = o2.time
  AND o1.device_id = 1
  AND o2.device_id = 2
ORDER BY o1.time
LIMIT 100;

-- test JOIN on time column with 3 hypertables
-- should use 3 ChunkAppend
:PREFIX
SELECT o1.time
FROM :TEST_TABLE o1
  INNER JOIN :TEST_TABLE o2 ON o1.time = o2.time
  INNER JOIN :TEST_TABLE o3 ON o1.time = o3.time
WHERE o1.device_id = 1
  AND o2.device_id = 2
  AND o3.device_id = 3
ORDER BY o1.time
LIMIT 100;

RESET enable_seqscan;
