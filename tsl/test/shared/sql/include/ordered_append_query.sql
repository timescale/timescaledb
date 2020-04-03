
-- test ASC for ordered chunks
:PREFIX SELECT
  time
FROM :TEST_TABLE
ORDER BY time ASC LIMIT 1;

-- test DESC for ordered chunks
:PREFIX SELECT
  time
FROM :TEST_TABLE
ORDER BY time DESC LIMIT 1;

-- test query with ORDER BY column not in targetlist
:PREFIX SELECT
  pg_typeof(device_id), pg_typeof(v2)
FROM :TEST_TABLE
ORDER BY time ASC LIMIT 1;

-- ORDER BY may include other columns after time column
:PREFIX SELECT
  time, device_id, v0
FROM :TEST_TABLE
ORDER BY time DESC, device_id LIMIT 1;

-- test RECORD in targetlist
:PREFIX SELECT
  (time, device_id, v0)
FROM :TEST_TABLE
ORDER BY time DESC, device_id LIMIT 1;

-- test sort column not in targetlist
:PREFIX SELECT
  time_bucket('1h',time)
FROM :TEST_TABLE
ORDER BY time DESC LIMIT 1;

-- queries with ORDER BY non-time column shouldn't use ordered append
:PREFIX SELECT
  device_id
FROM :TEST_TABLE
ORDER BY device_id LIMIT 1;

-- time column must be primary sort order
:PREFIX SELECT
  time, device_id
FROM :TEST_TABLE
ORDER BY device_id, time LIMIT 1;

-- test equality constraint on ORDER BY prefix
-- currently not optimized
:PREFIX SELECT
  time, device_id
FROM :TEST_TABLE
WHERE device_id = 1
ORDER BY device_id, time LIMIT 10;

-- queries without LIMIT should use ordered append
:PREFIX SELECT
  time
FROM :TEST_TABLE
ORDER BY time ASC;

-- queries without ORDER BY shouldnt use ordered append
:PREFIX SELECT
  pg_typeof(time)
FROM :TEST_TABLE
LIMIT 1;

-- test interaction with constraint exclusion
:PREFIX SELECT
  time
FROM :TEST_TABLE
WHERE time > '2000-01-07'
ORDER BY time ASC LIMIT 1;

:PREFIX SELECT
  time
FROM :TEST_TABLE
WHERE time > '2000-01-07'
ORDER BY time DESC LIMIT 1;

-- test interaction with runtime exclusion
:PREFIX SELECT
  time
FROM :TEST_TABLE
WHERE time > '2000-01-08'::text::timestamptz
ORDER BY time ASC LIMIT 1;

:PREFIX SELECT
  time
FROM :TEST_TABLE
WHERE time < '2000-01-08'::text::timestamptz
ORDER BY time ASC LIMIT 1;

-- test constraint exclusion
:PREFIX SELECT
  time
FROM :TEST_TABLE
WHERE time > '2000-01-08'::text::timestamptz AND time < '2000-01-10'
ORDER BY time ASC LIMIT 1;

:PREFIX SELECT
  time
FROM :TEST_TABLE
WHERE time < '2000-01-08'::text::timestamptz AND time > '2000-01-07'
ORDER BY time ASC LIMIT 1;

-- min/max queries
:PREFIX SELECT max(time) FROM :TEST_TABLE;

:PREFIX SELECT min(time) FROM :TEST_TABLE;

-- test first/last (doesn't use ordered append yet)
:PREFIX SELECT first(time, time) FROM :TEST_TABLE;

:PREFIX SELECT last(time, time) FROM :TEST_TABLE;

-- test query with time_bucket
:PREFIX SELECT
  time_bucket('1d',time)
FROM :TEST_TABLE
ORDER BY time ASC LIMIT 1;

-- test query with ORDER BY time_bucket
:PREFIX SELECT
  time_bucket('1d',time)
FROM :TEST_TABLE
ORDER BY 1 LIMIT 1;

-- test query with ORDER BY time_bucket, device_id
-- must not use ordered append
:PREFIX SELECT
  time_bucket('1d',time), device_id, v0
FROM :TEST_TABLE
ORDER BY time_bucket('1d',time), device_id LIMIT 1;

-- test query with ORDER BY date_trunc
:PREFIX SELECT
  time_bucket('1d',time)
FROM :TEST_TABLE
ORDER BY date_trunc('day', time) LIMIT 1;

-- test query with ORDER BY date_trunc
:PREFIX SELECT
  date_trunc('day',time)
FROM :TEST_TABLE
ORDER BY 1 LIMIT 1;

-- test query with ORDER BY date_trunc, device_id
-- must not use ordered append
:PREFIX SELECT
  date_trunc('day',time), device_id, v0
FROM :TEST_TABLE
ORDER BY 1,2 LIMIT 1;

-- test query with now() should result in ordered ChunkAppend
:PREFIX SELECT time FROM :TEST_TABLE WHERE time < now() + '1 month'
ORDER BY time DESC limit 1;

-- test CTE
:PREFIX WITH i AS (SELECT time FROM :TEST_TABLE WHERE time < now() ORDER BY time DESC limit 100)
SELECT * FROM i;

-- test LATERAL with ordered append in the outer query
:PREFIX SELECT time, pg_typeof(l) FROM :TEST_TABLE, LATERAL(SELECT * FROM (VALUES (1),(2)) v) l ORDER BY time DESC limit 2;

-- test LATERAL with ordered append in the lateral query
:PREFIX SELECT time, pg_typeof(v) FROM (VALUES (1),(2)) v, LATERAL(SELECT * FROM :TEST_TABLE ORDER BY time DESC limit 2) l;

-- test plan with best index is chosen
-- this should use device_id, time index
:PREFIX SELECT time, device_id FROM :TEST_TABLE WHERE device_id = 1 ORDER BY time DESC LIMIT 1;

-- test plan with best index is chosen
-- this should use time index
:PREFIX SELECT time FROM :TEST_TABLE ORDER BY time DESC LIMIT 1;

-- test LATERAL with correlated query
-- only last chunk should be executed
:PREFIX SELECT g.time, l.time
FROM generate_series('2000-01-01'::timestamptz,'2000-01-03','1d') AS g(time)
LEFT OUTER JOIN LATERAL(
  SELECT * FROM :TEST_TABLE o
    WHERE o.time >= g.time AND o.time < g.time + '1d'::interval ORDER BY time DESC LIMIT 1
) l ON true;

-- test LATERAL with correlated query
-- only 2nd chunk should be executed
:PREFIX SELECT g.time, l.time
FROM generate_series('2000-01-10'::timestamptz,'2000-01-11','1d') AS g(time)
LEFT OUTER JOIN LATERAL(
  SELECT * FROM :TEST_TABLE o
    WHERE o.time >= g.time AND o.time < g.time + '1d'::interval ORDER BY time LIMIT 1
) l ON true;

-- test startup and runtime exclusion together
:PREFIX SELECT g.time, l.time
FROM generate_series('2000-01-01'::timestamptz,'2000-01-03','1d') AS g(time)
LEFT OUTER JOIN LATERAL(
  SELECT * FROM :TEST_TABLE o
    WHERE o.time >= g.time AND o.time < g.time + '1d'::interval AND o.time < now() ORDER BY time DESC LIMIT 1
) l ON true;

-- test startup and runtime exclusion together
-- all chunks should be filtered
:PREFIX SELECT g.time, l.time
FROM generate_series('2000-01-01'::timestamptz,'2000-01-03','1d') AS g(time)
LEFT OUTER JOIN LATERAL(
  SELECT * FROM :TEST_TABLE o
    WHERE o.time >= g.time AND o.time < g.time + '1d'::interval AND o.time > now() ORDER BY time DESC LIMIT 1
) l ON true;

-- test CTE
-- no chunk exclusion for CTE because cte query is not pulled up
:PREFIX WITH cte AS (SELECT time FROM :TEST_TABLE ORDER BY time)
SELECT * FROM cte WHERE time < '2000-02-01'::timestamptz;

-- test JOIN
-- no exclusion on joined table because quals are not propagated yet
:PREFIX SELECT o1.time, o2.time
FROM :TEST_TABLE o1
INNER JOIN :TEST_TABLE o2 ON o1.time = o2.time
WHERE o1.time < '2000-02-01'
ORDER BY o1.time;

-- test JOIN
-- last chunk of o2 should not be executed
:PREFIX SELECT o1.time, o2.time
FROM :TEST_TABLE o1
INNER JOIN (SELECT * FROM :TEST_TABLE o2 ORDER BY time) o2 ON o1.time = o2.time
WHERE o1.time < '2000-01-08'
ORDER BY o1.time LIMIT 10;

-- test subquery
-- not ChunkAppend so no chunk exclusion
:PREFIX SELECT time
FROM :TEST_TABLE WHERE time = (SELECT max(time) FROM :TEST_TABLE) ORDER BY time;

-- test join against max query
-- not ChunkAppend so no chunk exclusion
SET enable_hashjoin = false;
:PREFIX SELECT o1.time, o2.*
FROM :TEST_TABLE o1 INNER JOIN (SELECT max(time) AS max_time FROM :TEST_TABLE) o2 ON o1.time = o2.max_time ORDER BY time;
RESET enable_hashjoin;

-- test ordered append with limit expression
:PREFIX SELECT time
FROM :TEST_TABLE ORDER BY time LIMIT (SELECT length('four'));

-- test with ordered guc disabled
SET timescaledb.enable_ordered_append TO off;
:PREFIX SELECT time
FROM :TEST_TABLE ORDER BY time LIMIT 3;

RESET timescaledb.enable_ordered_append;
:PREFIX SELECT time
FROM :TEST_TABLE ORDER BY time LIMIT 3;

-- test with chunk append disabled
SET timescaledb.enable_chunk_append TO off;
:PREFIX SELECT time
FROM :TEST_TABLE ORDER BY time LIMIT 3;

RESET timescaledb.enable_chunk_append;
:PREFIX SELECT time
FROM :TEST_TABLE ORDER BY time LIMIT 3;

-- test JOIN on time column
-- should use 2 ChunkAppend
:PREFIX SELECT o1.time FROM :TEST_TABLE o1 INNER JOIN :TEST_TABLE o2 ON o1.time = o2.time ORDER BY o1.time LIMIT 100;

-- test JOIN on time column with USING
-- should use 2 ChunkAppend
:PREFIX SELECT o1.time FROM :TEST_TABLE o1 INNER JOIN :TEST_TABLE o2 USING(time) ORDER BY o1.time LIMIT 100;

-- test NATURAL JOIN on time column
-- should use 2 ChunkAppend
:PREFIX SELECT o1.time FROM :TEST_TABLE o1 NATURAL INNER JOIN :TEST_TABLE o2 ORDER BY o1.time LIMIT 100;

-- test LEFT JOIN on time column
-- should use 2 ChunkAppend
:PREFIX SELECT o1.time FROM :TEST_TABLE o1 LEFT JOIN :TEST_TABLE o2 ON o1.time=o2.time ORDER BY o1.time LIMIT 100;

-- test RIGHT JOIN on time column
-- should use 2 ChunkAppend
:PREFIX SELECT o1.time FROM :TEST_TABLE o1 RIGHT JOIN :TEST_TABLE o2 ON o1.time=o2.time ORDER BY o2.time LIMIT 100;

-- test JOIN on time column with ON clause expression order switched
-- should use 2 ChunkAppend
:PREFIX SELECT o1.time FROM :TEST_TABLE o1 INNER JOIN :TEST_TABLE o2 ON o2.time = o1.time ORDER BY o1.time LIMIT 100;

-- test JOIN on time column with equality condition in WHERE clause
-- should use 2 ChunkAppend
:PREFIX SELECT o1.time FROM :TEST_TABLE o1 INNER JOIN :TEST_TABLE o2 ON true WHERE o1.time = o2.time ORDER BY o1.time LIMIT 100;

-- test JOIN on time column with ORDER BY 2nd hypertable
-- should use 2 ChunkAppend
:PREFIX SELECT o1.time FROM :TEST_TABLE o1 INNER JOIN :TEST_TABLE o2 ON o1.time = o2.time ORDER BY o2.time LIMIT 100;

-- test JOIN on time column and device_id
-- should use 2 ChunkAppend
:PREFIX SELECT o1.time FROM :TEST_TABLE o1 INNER JOIN :TEST_TABLE o2 ON o1.device_id = o2.device_id AND o1.time = o2.time ORDER BY o1.time LIMIT 100;

-- test JOIN on device_id
-- should not use ordered append for 2nd hypertable
:PREFIX SELECT o1.time FROM :TEST_TABLE o1 INNER JOIN :TEST_TABLE o2 ON o1.device_id = o2.device_id WHERE o1.device_id = 1 ORDER BY o1.time LIMIT 100;

-- test JOIN on time column with implicit join
-- should use 2 ChunkAppend
:PREFIX SELECT o1.time FROM :TEST_TABLE o1, :TEST_TABLE o2 WHERE o1.time = o2.time ORDER BY o1.time LIMIT 100;

-- test JOIN on time column with 3 hypertables
-- should use 3 ChunkAppend
:PREFIX SELECT o1.time FROM :TEST_TABLE o1 INNER JOIN :TEST_TABLE o2 ON o1.time = o2.time INNER JOIN :TEST_TABLE o3 ON o1.time = o3.time ORDER BY o1.time LIMIT 100;

