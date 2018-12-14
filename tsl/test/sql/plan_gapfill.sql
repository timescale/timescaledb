-- Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
--
-- This file is licensed under the Timescale License,
-- see LICENSE-TIMESCALE at the top of the tsl directory.

-- simple example
EXPLAIN (COSTS OFF)
  SELECT
    time_bucket_gapfill('5m',time,now(),now()),
    avg(c2)
  FROM (VALUES (now(),1),(now(),NULL),(now(),NULL)) as t(time,c2)
  GROUP BY 1
  ORDER BY 1;

-- test sorting
EXPLAIN (COSTS OFF)
  SELECT
    time_bucket_gapfill('5m',time,now(),now()),
    avg(c2)
  FROM (VALUES (now(),1),(now(),NULL),(now(),NULL)) as t(time,c2)
  GROUP BY 1
  ORDER BY 2;

-- test sort direction
EXPLAIN (COSTS OFF)
  SELECT
    time_bucket_gapfill('5m',time,now(),now()),
    avg(c2)
  FROM (VALUES (now(),1),(now(),NULL),(now(),NULL)) as t(time,c2)
  GROUP BY 1
  ORDER BY 1 DESC;

-- test order by aggregate function
EXPLAIN (COSTS OFF)
  SELECT
    time_bucket_gapfill('5m',time,now(),now()),
    avg(c2)
  FROM (VALUES (now(),1),(now(),NULL),(now(),NULL)) as t(time,c2)
  GROUP BY 1
  ORDER BY 2,1;

-- test query without order by
EXPLAIN (COSTS OFF)
  SELECT
    time_bucket_gapfill('5m',time,now(),now()),
    avg(c2)
  FROM (VALUES (now(),1),(now(),NULL),(now(),NULL)) as t(time,c2)
  GROUP BY 1;

