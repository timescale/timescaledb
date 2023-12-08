-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test gapfill with no data
CREATE TABLE i6385(time timestamptz NOT NULL, value float);
SELECT table_name FROM create_hypertable('i6385','time');

SELECT
  time_bucket_gapfill('1 day', time) AS time,
  sum(value) AS value
FROM
  i6385
WHERE
  time >= '2023-12-01 14:05:00+01'
  AND time < '2023-12-20 14:05:00+01'
GROUP BY 1
ORDER BY 1 DESC;

DROP TABLE i6385;
