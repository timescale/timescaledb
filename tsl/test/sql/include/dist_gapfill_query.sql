-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SELECT time_bucket_gapfill('3 hours', time, '2017-01-01 06:00', '2017-01-02 18:00'),
       first(value, time),
       avg(value)
FROM conditions
GROUP BY 1;

SELECT time_bucket_gapfill('3 hours', time, '2017-01-01 06:00', '2017-01-01 18:00'),
       device,
       first(value, time),
       avg(value)
FROM conditions
GROUP BY 1,2;

SELECT time_bucket_gapfill('3 hours', time, '2017-01-01 06:00', '2017-01-01 18:00'),
       device,
       first(value, time),
       avg(value)
FROM conditions
GROUP BY 2,1;

SELECT
  time_bucket_gapfill('3 hours', time, '2017-01-01 06:00', '2017-01-01 18:00'),
  lag(min(time)) OVER ()
FROM conditions
GROUP BY 1;
