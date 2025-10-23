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

-- Fix for 2nd issue reported in #4894: "time_bucket_gapfil" with "locf/interpolate"
-- using out-of-order and repeated columns, resulting in below error:
-- ERROR: attribute 5 of type record has wrong type

-- Should return correct data with no error
WITH el_data(time,position_id,measurement_value,measurement_type,src_mac_id,stream_index,delta) AS(
VALUES
('2025-07-05 23:57:19.000000 +00:00'::timestamptz,'2da82d12-5ceb-4470-b60b-9660b755d052'::uuid,26.5::numeric,'temperature_ambient','FFFF000D6F4DA7B2',3,-900),
('2025-07-06 00:12:19.000000 +00:00'::timestamptz,'2da82d12-5ceb-4470-b60b-9660b755d052'::uuid,26.5::numeric,'temperature_ambient','FFFF000D6F4DA7B2',3,-901),
('2025-07-06 00:27:20.000000 +00:00'::timestamptz,'2da82d12-5ceb-4470-b60b-9660b755d052'::uuid,26.5::numeric,'temperature_ambient','FFFF000D6F4DA7B2',3,-3110560),
('2025-07-06 00:06:12.000000 +00:00'::timestamptz,'9f2e566b-8c26-4ead-ad24-12841c79a932'::uuid,25.5::numeric,'temperature_ambient','FFFF000D6F4DA7B2',3,-900),
('2025-07-06 00:21:12.000000 +00:00'::timestamptz,'9f2e566b-8c26-4ead-ad24-12841c79a932'::uuid,25.5::numeric,'temperature_ambient','FFFF000D6F4DA7B2',3,-3110928),
('2025-07-06 00:02:57.000000 +00:00'::timestamptz,'09ca0d92-1eb5-4d07-adbc-2bb500817cc7'::uuid,26.5::numeric,'temperature_ambient','FFFF000D6F4DA7B2',3,-901),
('2025-07-06 00:17:58.000000 +00:00'::timestamptz,'09ca0d92-1eb5-4d07-adbc-2bb500817cc7'::uuid,26.5::numeric,'temperature_ambient','FFFF000D6F4DA7B2',3,-3111122),
('2025-07-06 00:06:02.000000 +00:00'::timestamptz,'e0145b95-6faa-4eb3-815d-5d0a91ba909c'::uuid,24::numeric,'temperature_ambient','FFFF000D6F4DA7B2',3,-900),
('2025-07-06 00:21:02.000000 +00:00'::timestamptz,'e0145b95-6faa-4eb3-815d-5d0a91ba909c'::uuid,24::numeric,'temperature_ambient','FFFF000D6F4DA7B2',3,-3110938),
('2025-07-06 00:04:48.000000 +00:00'::timestamptz,'d439a8fc-7285-4d32-82bc-2105c9ed2d52'::uuid,24::numeric,'temperature_ambient','FFFF000D6F4DA7B2',3,-900),
('2025-07-06 00:19:48.000000 +00:00'::timestamptz,'d439a8fc-7285-4d32-82bc-2105c9ed2d52'::uuid,24.5::numeric,'temperature_ambient','FFFF000D6F4DA7B2',3,-3111012),
('2025-07-06 00:07:00.000000 +00:00'::timestamptz,'4aa9b368-b9c4-4b15-bfcc-c67b1931dd11'::uuid,25::numeric,'temperature_ambient','FFFF000D6F4DA7B2',3,-900),
('2025-07-06 00:22:00.000000 +00:00'::timestamptz,'4aa9b368-b9c4-4b15-bfcc-c67b1931dd11'::uuid,24.5::numeric,'temperature_ambient','FFFF000D6F4DA7B2',3,-3110880),
('2025-07-06 00:08:03.000000 +00:00'::timestamptz,'64dc15d6-3988-4320-8f18-16ef8512990e'::uuid,30.5::numeric,'temperature_ambient','FFFF000D6F4DA7B2',3,-900),
('2025-07-06 00:23:03.000000 +00:00'::timestamptz,'64dc15d6-3988-4320-8f18-16ef8512990e'::uuid,30.5::numeric,'temperature_ambient','FFFF000D6F4DA7B2',3,-3110817),
('2025-07-05 23:57:13.000000 +00:00'::timestamptz,'bb044d29-c136-412d-a193-354f090dafcb'::uuid,23::numeric,'temperature_ambient','FFFF000D6F4DA7B2',3,-300),
('2025-07-06 00:02:13.000000 +00:00'::timestamptz,'bb044d29-c136-412d-a193-354f090dafcb'::uuid,23::numeric,'temperature_ambient','FFFF000D6F4DA7B2',3,-300),
('2025-07-06 00:07:13.000000 +00:00'::timestamptz,'bb044d29-c136-412d-a193-354f090dafcb'::uuid,23::numeric,'temperature_ambient','FFFF000D6F4DA7B2',3,-300)
)
     ,
    el_nodes_data(time,src_mac_id,stream_index,position_id,measurement_type,measurement_value) as(
VALUES
('2025-07-05 23:42:20.000000 +00:00'::timestamptz,'FFFF000D6F4DA7B2',3,'2da82d12-5ceb-4470-b60b-9660b755d052','temperature_ambient',26::numeric),
('2025-07-06 00:51:09.000000 +00:00'::timestamptz,'FFFF000D6F4DA7B2',3,'2da82d12-5ceb-4470-b60b-9660b755d052','temperature_ambient',26::numeric)
)
SELECT
    time_bucket_gapfill('5 minutes', m.time,
                        start := '2025-07-05 23:55:00+00:00'::timestamptz, finish := '2025-07-06 00:30:00+00:00'::timestamptz) AS intervals,
    m.position_id,
    m.src_mac_id,
    m.stream_index,
    m.measurement_type,
    m.stream_index,
    locf(last(m.measurement_value, m.time)
        ,
         (SELECT d2.measurement_value FROM el_nodes_data d2
          WHERE d2.measurement_type = m.measurement_type
            AND d2.src_mac_id = m.src_mac_id
            AND d2.stream_index = m.stream_index
            AND d2.time < '2025-07-05 23:55:00+00:00'
             AND d2.time > '2025-07-05 23:55:00+00:00'::timestamptz - INTERVAL '30 minutes'
         ORDER BY d2.time DESC LIMIT 1)
    ) AS locf_measurement_value
FROM el_data m
GROUP BY
    time_bucket_gapfill('5 minutes', m.time, start := '2025-07-05 23:55:00+00:00'::timestamptz,
                        finish := '2025-07-06 00:30:00+00:00'::timestamptz),  -- avoid alias here
    m.position_id,m.src_mac_id, m.stream_index, m.measurement_type
ORDER BY
    time_bucket_gapfill('5 minutes', m.time, start := '2025-07-05 23:55:00+00:00'::timestamptz,
                        finish := '2025-07-06 00:30:00+00:00'::timestamptz),  -- avoid alias here
    m.position_id,m.src_mac_id, m.stream_index, m.measurement_type;

-- Fix for #4894: gapfill error over aggregates in expressions with groupby columns and columns out of order
CREATE TABLE hourly (
    time timestamptz NOT NULL,
    signal smallint NOT NULL,
    value real,
    level_a integer,
    level_b smallint,
    level_c smallint,
    agg smallint
);

INSERT into hourly(time, signal,value, level_a, level_b, level_c, agg) values
('2022-10-01T00:00:00Z', 2, 685, 1, -1, -1, 2 ),
('2022-10-01T00:00:00Z', 2, 686, 1, -1, -1, 3 ),
('2022-10-01T02:00:00Z', 2, 686, 1, -1, -3, 2 ),
('2022-10-01T02:00:00Z', 2, 687, 1, -1, -1, 3 ),
('2022-10-01T03:00:00Z', 2, 687, 1, -1, -3, 2 ),
('2022-10-01T03:00:00Z', 2, 688, 1, -1, -1, 3 );

-- Expression over 1 aggregate and 1 groupby column
SELECT
 time_bucket_gapfill('1 hour', time) as time,
 CASE WHEN agg in (0,3) THEN max(value) ELSE null END as max,
 CASE WHEN agg in (0,2) THEN min(value) ELSE null END as min
 FROM hourly WHERE agg in (0,2,3) and signal in (2) AND level_a = 1 AND level_b >= -1 AND time >= '2022-10-01T00:00:00Z' AND time < '2022-10-01T05:59:59Z'
 GROUP BY 1,  agg order by 1,2,3;

-- Expression over 2 aggregates and 1 groupby column
SELECT
 time_bucket_gapfill('1 hour', time) as time,
 CASE WHEN agg in (0,3) THEN max(value) ELSE min(level_c) END as maxmin
 FROM hourly WHERE agg in (0,2,3) and signal in (2) AND level_a = 1 AND level_b = -1 AND time >= '2022-10-01T00:00:00Z' AND time < '2022-10-01T05:59:59Z'
 GROUP BY 1,  agg order by 1,2;

-- Expression over 2 aggregates and 2 groupby columns
SELECT
 time_bucket_gapfill('1 hour', time) as time,
 CASE WHEN agg in (0,3) THEN max(value) ELSE min(level_c)+signal END as maxmin
 FROM hourly WHERE agg in (0,2,3) and signal in (2) AND level_a = 1 AND level_b = -1 AND time >= '2022-10-01T00:00:00Z' AND time < '2022-10-01T05:59:59Z'
 GROUP BY 1,  agg, signal order by 1,2;

-- Expressions over aggregates and complex groupby expressions
SELECT
 time_bucket_gapfill('1 hour', time) as time,
 max(value)+(agg+1)
 FROM hourly WHERE agg in (0,2,3) and signal in (2) AND level_a = 1 AND level_b = -1 AND time >= '2022-10-01T00:00:00Z' AND time < '2022-10-01T05:59:59Z'
 GROUP BY 1,  agg+1 order by 1,2;

SELECT
 time_bucket_gapfill('1 hour', time) as time,
 max(value)+(agg+1)+(agg+1+1)
 FROM hourly WHERE agg in (0,2,3) and signal in (2) AND level_a = 1 AND level_b = -1 AND time >= '2022-10-01T00:00:00Z' AND time < '2022-10-01T05:59:59Z'
 GROUP BY 1,  agg+1, agg+1+1 order by 1,2;

SELECT
 time_bucket_gapfill('1 hour', time) as time,
 max(value) + (agg+signal) maxv,
 min(value) - (agg+signal) minv,
 agg+signal
 FROM hourly WHERE agg in (0,2,3) and signal in (2) AND level_a = 1 AND level_b = -1 AND time >= '2022-10-01T00:00:00Z' AND time < '2022-10-01T05:59:59Z'
 GROUP BY 1,  agg+signal order by 1,2,3;

drop table hourly cascade;
