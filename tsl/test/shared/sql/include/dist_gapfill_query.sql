-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SELECT time_bucket_gapfill('3 hours', time, start:='2017-01-01 06:00', finish:='2017-01-02 18:00'),
       first(value, time),
       avg(value)
FROM :CONDITIONS
GROUP BY 1;

SELECT time_bucket_gapfill('3 hours', time, start:='2017-01-01 06:00', finish:='2017-01-01 18:00'),
       device,
       first(value, time),
       avg(value)
FROM :CONDITIONS
GROUP BY 1,2;

SELECT time_bucket_gapfill('3 hours', time, start:='2017-01-01 06:00', finish:='2017-01-01 18:00'),
       device,
       first(value, time),
       avg(value)
FROM :CONDITIONS
GROUP BY 2,1;

SELECT
  time_bucket_gapfill('3 hours', time, start:='2017-01-01 06:00', finish:='2017-01-01 18:00'),
  lag(min(time)) OVER ()
FROM :CONDITIONS
GROUP BY 1;
