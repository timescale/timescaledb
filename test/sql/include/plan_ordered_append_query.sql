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
  INNER JOIN LATERAL(SELECT * FROM _timescaledb_catalog.chunk_constraint cc WHERE c.id = cc.chunk_id ORDER BY cc.dimension_slice_id LIMIT 1) cc ON true
  INNER JOIN _timescaledb_catalog.dimension_slice ds ON ds.id=cc.dimension_slice_id
  INNER JOIN _timescaledb_catalog.dimension d ON ds.dimension_id = d.id
  INNER JOIN _timescaledb_catalog.hypertable ht ON d.hypertable_id = ht.id
ORDER BY ht.table_name, range_start, chunk;

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

-- test query with ORDER BY time_bucket, device_id
-- must not use ordered append
:PREFIX SELECT
  time_bucket('1d',time), device_id, name
FROM dimension_last
ORDER BY time_bucket('1d',time), device_id LIMIT 1;

-- test query with ORDER BY date_trunc, device_id
-- must not use ordered append
:PREFIX SELECT
  date_trunc('day',time), device_id, name
FROM dimension_last
ORDER BY 1,2 LIMIT 1;

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

-- test hypertable with index missing on one chunk
:PREFIX SELECT
  time, device_id, value
FROM ht_missing_indexes
ORDER BY time ASC LIMIT 1;

-- test hypertable with index missing on one chunk
-- and no data
:PREFIX SELECT
  time, device_id, value
FROM ht_missing_indexes
WHERE device_id = 2
ORDER BY time DESC LIMIT 1;

-- test hypertable with index missing on one chunk
-- and no data
:PREFIX SELECT
  time, device_id, value
FROM ht_missing_indexes
WHERE time > '2000-01-07'
ORDER BY time LIMIT 10;

-- test hypertable with dropped columns
:PREFIX SELECT
  time, device_id, value
FROM ht_dropped_columns
ORDER BY time ASC LIMIT 1;

-- test hypertable with dropped columns
:PREFIX SELECT
  time, device_id, value
FROM ht_dropped_columns
WHERE device_id = 1
ORDER BY time DESC;

-- test hypertable with 2 space dimensions
:PREFIX SELECT
  time, device_id, value
FROM space2
ORDER BY time DESC;

-- test hypertable with 3 space dimensions
:PREFIX SELECT
  time
FROM space3
ORDER BY time DESC;

-- test COLLATION
-- cant be tested in our ci because alpine doesnt support locales
-- :PREFIX SELECT * FROM sortopt_test ORDER BY time, device COLLATE "en_US.utf8";

-- test NULLS FIRST
:PREFIX SELECT * FROM sortopt_test ORDER BY time, device NULLS FIRST;

-- test NULLS LAST
:PREFIX SELECT * FROM sortopt_test ORDER BY time, device DESC NULLS LAST;
