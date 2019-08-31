# Gap filling

This module implements first level support for gap fill queries, including
support for LOCF (last observation carried forward) and interpolation, without
requiring to join against `generate_series`. This makes it easier to join
timeseries with different or irregular sampling intervals.

## Design

This introduces a new gapfill customscan node that is inserted above the
aggregation node of a query. The node will inject tuples for time intervals
without data. The node requires data to be sorted by time, but it will inject
sort nodes in the plan to ensure data is sorted correctly if the query order
does not match the required order.

The time_bucket_gapfill functions only serves to trigger injecting the gapfill
customscan node in the planner all the tuple injecting happens in the gapfill
node and time_bucket_gapfill just calls plain time_bucket.

The locf and interpolate function calls serve as markers in the plan to
trigger locf or interpolate behaviour. In the targetlist of the gapfill node
those functions will be toplevel function calls.

The gapfill state transitions are described in exec.h

## Usage

Gapfill query
```
SELECT
  time_bucket_gapfill(1,time,0,6) AS time,
  min(value) AS value
FROM (values (0,1),(5,6)) v(time,value)
GROUP BY 1 ORDER BY 1;
```

Gapfill query with LOCF
```
SELECT
  time_bucket_gapfill(1,time,0,6) AS time,
  locf(min(value)) AS value
FROM (values (0,1),(5,6)) v(time,value)
GROUP BY 1 ORDER BY 1;
```

Gapfill query with interpolation
```
SELECT
  time_bucket_gapfill(1,time,0,6) AS time,
  interpolate(min(value)) AS value
FROM (values (0,1),(5,6)) v(time,value)
GROUP BY 1 ORDER BY 1;
```

