-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- print chunks ordered by time to ensure ordering we want
SELECT
  ht.table_name AS hypertable,
  c.table_name AS chunk,
  ds.range_start
FROM
  _timescaledb_catalog.chunk c
  INNER JOIN _timescaledb_catalog.chunk_constraint cc ON c.id = cc.chunk_id
  INNER JOIN _timescaledb_catalog.dimension_slice ds ON ds.id=cc.dimension_slice_id
  INNER JOIN _timescaledb_catalog.dimension d ON ds.dimension_id = d.id
  INNER JOIN _timescaledb_catalog.hypertable ht ON d.hypertable_id = ht.id
ORDER BY ht.table_name, range_start;

-- test ASC for ordered chunks
:PREFIX SELECT
  time, device_id, value
FROM ordered_append
ORDER BY time ASC LIMIT 1;

-- test DESC for ordered chunks
:PREFIX SELECT
  time, device_id, value
FROM ordered_append
ORDER BY time DESC LIMIT 1;

-- test ASC for reverse ordered chunks
:PREFIX SELECT
  time, device_id, value
FROM ordered_append_reverse
ORDER BY time ASC LIMIT 1;

-- test DESC for reverse ordered chunks
:PREFIX SELECT
  time, device_id, value
FROM ordered_append_reverse
ORDER BY time DESC LIMIT 1;

-- test query with ORDER BY column not in targetlist
:PREFIX SELECT
  device_id, value
FROM ordered_append
ORDER BY time ASC LIMIT 1;

-- ORDER BY may include other columns after time column
:PREFIX SELECT
  time, device_id, value
FROM ordered_append
ORDER BY time DESC, device_id LIMIT 1;

-- queries with ORDER BY non-time column shouldn't use ordered append
:PREFIX SELECT
  time, device_id, value
FROM ordered_append
ORDER BY device_id LIMIT 1;

-- time column must be primary sort order
:PREFIX SELECT
  time, device_id, value
FROM ordered_append
ORDER BY device_id, time LIMIT 1;

-- queries without LIMIT shouldnt use ordered append
:PREFIX SELECT
  time, device_id, value
FROM ordered_append
ORDER BY time ASC;

-- queries without ORDER BY shouldnt use ordered append (still uses append though)
:PREFIX SELECT
  time, device_id, value
FROM ordered_append
LIMIT 1;

-- test interaction with constraint exclusion
:PREFIX SELECT
  time, device_id, value
FROM ordered_append
WHERE time > '2000-01-07'
ORDER BY time ASC LIMIT 1;

:PREFIX SELECT
  time, device_id, value
FROM ordered_append
WHERE time > '2000-01-07'
ORDER BY time DESC LIMIT 1;

-- test interaction with constraint aware append
:PREFIX SELECT
  time, device_id, value
FROM ordered_append
WHERE time > now_s()
ORDER BY time ASC LIMIT 1;

:PREFIX SELECT
  time, device_id, value
FROM ordered_append
WHERE time < now_s()
ORDER BY time ASC LIMIT 1;

-- test interaction withi constraint exclusion and constraint aware append
:PREFIX SELECT
  time, device_id, value
FROM ordered_append
WHERE time > now_s() AND time < '2000-01-10'
ORDER BY time ASC LIMIT 1;

:PREFIX SELECT
  time, device_id, value
FROM ordered_append
WHERE time < now_s() AND time > '2000-01-07'
ORDER BY time ASC LIMIT 1;

-- min/max queries
:PREFIX SELECT max(time) FROM ordered_append;

:PREFIX SELECT min(time) FROM ordered_append;

-- test first/last (doesn't use ordered append yet)
:PREFIX SELECT first(time, time) FROM ordered_append;

:PREFIX SELECT last(time, time) FROM ordered_append;

-- test query with time_bucket
:PREFIX SELECT
  time_bucket('1d',time), device_id, value
FROM ordered_append
ORDER BY time ASC LIMIT 1;

-- test query with order by time_bucket (should not use ordered append)
:PREFIX SELECT
  time_bucket('1d',time), device_id, value
FROM ordered_append
ORDER BY 1 LIMIT 1;

-- test query with order by time_bucket (should not use ordered append)
:PREFIX SELECT
  time_bucket('1d',time), device_id, value
FROM ordered_append
ORDER BY time_bucket('1d',time) LIMIT 1;

-- test query with now() should result in ordered append with constraint aware append
:PREFIX SELECT * FROM ordered_append WHERE time < now() + '1 month'
ORDER BY time DESC limit 1;

-- test CTE
:PREFIX WITH i AS (SELECT * FROM ordered_append WHERE time < now() ORDER BY time DESC limit 100)
SELECT * FROM i;

-- test LATERAL with ordered append in the outer query
:PREFIX SELECT * FROM ordered_append, LATERAL(SELECT * FROM (VALUES (1),(2)) v) l ORDER BY time DESC limit 2;

-- test LATERAL with ordered append in the lateral query
:PREFIX SELECT * FROM (VALUES (1),(2)) v, LATERAL(SELECT * FROM ordered_append ORDER BY time DESC limit 2) l;

-- test plan with best index is chosen
-- this should use device_id, time index
:PREFIX SELECT * FROM ordered_append WHERE device_id = 1 ORDER BY time DESC LIMIT 1;

-- test plan with best index is chosen
-- this should use time index
:PREFIX SELECT * FROM ordered_append ORDER BY time DESC LIMIT 1;

-- test with table with only dimension column
:PREFIX SELECT * FROM dimension_only ORDER BY time DESC LIMIT 1;

-- test LEFT JOIN against hypertable
:PREFIX_NO_ANALYZE SELECT *
FROM dimension_last
LEFT JOIN dimension_only USING (time)
ORDER BY dimension_last.time DESC
LIMIT 2;

-- test INNER JOIN against non-hypertable
:PREFIX_NO_ANALYZE SELECT *
FROM dimension_last
INNER JOIN dimension_only USING (time)
ORDER BY dimension_last.time DESC
LIMIT 2;

-- test join against non-hypertable
:PREFIX SELECT *
FROM dimension_last
INNER JOIN devices USING(device_id)
ORDER BY dimension_last.time DESC
LIMIT 2;

