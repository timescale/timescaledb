-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Fix for issue #6078: crash in "time_bucket_gapfil"
-- with "locf" using NULL value and NULLs treated as missing.

create table tw(id varchar, time timestamp, value float);
insert into tw values ('uuid', '2023-09-15 12:00'::timestamp, 147673::float), ('uuid', '2023-09-15 12:52', 149400);

SET timezone TO 'Europe/Berlin';

-- Losf lookup query being a SubPlan as it has a predicate over correlated variable
-- We call Locf calculation for a "gapfill_start" tuple retrived from the input to override its NULL value,
-- rather than for a tuple generated to fill the gaps.
-- We used to crash in this scenario due to pointing to a wrong tuple slot. Should work OK now.
SELECT id,
  time_bucket_gapfill('10min'::interval, time, '2023-09-15 12:50', '2023-09-15 12:53') AS bucket,
  locf(null::float, (SELECT p.value FROM tw AS p WHERE p.id = tw.id  ORDER BY 1 LIMIT 1), true) AS value
FROM tw GROUP BY 1,2;

-- Should return same result for simpler Losf lookup query
SELECT id,
  time_bucket_gapfill('10min'::interval, time, '2023-09-15 12:50', '2023-09-15 12:53') AS bucket,
  locf(null::float, (SELECT p.value FROM tw AS p  ORDER BY 1 LIMIT 1), true) AS value
FROM tw GROUP BY 1,2;

-- Should return same result when using CTE instead of input table
WITH data (id, time, value) AS (
  VALUES ('uuid', '2023-09-15 12:00'::timestamp, 147673::float), ('uuid', '2023-09-15 12:52', 149400)
)
SELECT id,
  time_bucket_gapfill('10min'::interval, time, '2023-09-15 12:50', '2023-09-15 12:53') AS bucket,
  locf(null::float, (SELECT p.value FROM data AS p WHERE p.id = data.id ORDER BY 1 LIMIT 1), true) AS value
FROM data GROUP BY 1,2;

-- Pick an average value instead of min
WITH data (id, time, value) AS (
  VALUES ('uuid', '2023-09-15 12:00'::timestamp, 147673::float), ('uuid', '2023-09-15 12:52', 149400)
)
SELECT id,
  time_bucket_gapfill('10min'::interval, time, '2023-09-15 12:50', '2023-09-15 12:53') AS bucket,
  locf(null::float, (SELECT avg(p.value) FROM data AS p WHERE p.id = data.id), true) AS value
FROM data GROUP BY 1,2;

-- Call Locf both for filling gaps and setting the non-NULL value for "gapfill_start" tuple
WITH data (id, time, value) AS (
  VALUES ('uuid', '2023-09-15 12:00'::timestamp, 147673::float), ('uuid', '2023-09-15 12:52', 149400)
)
SELECT id,
  time_bucket_gapfill('1min'::interval, time, '2023-09-15 12:50', '2023-09-15 12:53') AS bucket,
  locf(null::float, (SELECT avg(p.value) FROM data AS p WHERE p.id = data.id), true) AS value
FROM data GROUP BY 1,2;

-- Use Locf lookup for multiple groupby values, use 2nd groupby column as a correlation var in the lookup
-- Make sure correlation var is obtained from correct tuple passed to Locf lookup expression
WITH data (id, gr, time, value) AS (
  VALUES ('uuid', 12, '2023-09-15 12:00'::timestamp, 147673::float), ('uuid', 14, '2023-09-15 12:52', 149400)
)
SELECT id, gr,
  time_bucket_gapfill('10min'::interval, time, '2023-09-15 12:50', '2023-09-15 12:53') AS bucket,
  locf(null::float, (SELECT p.value FROM data AS p where p.gr = data.gr ORDER BY 1 desc LIMIT 1), true) AS value
FROM data GROUP BY 1,2,3 ORDER BY 1,2,3;

-- Same but use two correlation vars in Locf lookup
WITH data (id, gr, time, value) AS (
  VALUES ('uuid', 12, '2023-09-15 12:00'::timestamp, 147673::float), ('uuid', 14, '2023-09-15 12:52', 149400)
)
SELECT id, gr,
  time_bucket_gapfill('10min'::interval, time, '2023-09-15 12:50', '2023-09-15 12:53') AS bucket,
  locf(null::float, (SELECT p.value FROM data AS p where p.gr = data.gr and p.id = data.id ORDER BY 1 desc LIMIT 1), true) AS value
FROM data GROUP BY 1,2,3 ORDER BY 1,2,3;

drop table tw cascade;
RESET timezone;
