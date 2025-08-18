-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test grouping by uuid segmentby column (scalar by-reference fixed-width column).

CREATE TABLE public.plan_plus(
  "time"        timestamp with time zone NOT NULL,
  device        uuid                     NOT NULL,
  field         text                     NOT NULL,
  value_numeric double precision,
  value_bool    boolean,
  value_string  text,
  value_geo     point,
  created       timestamp with time zone DEFAULT now()
);

CREATE INDEX plan_plus_device_time_idx
  ON public.plan_plus
  USING btree (device, "time" DESC);

CREATE INDEX plan_plus_time_idx
  ON public.plan_plus
  USING btree ("time" DESC);

SELECT public.create_hypertable(
  relation => 'public.plan_plus',
  time_column_name => 'time',
  create_default_indexes => false
);

ALTER TABLE public.plan_plus SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'device',
  timescaledb.compress_orderby='"time" DESC'
);

INSERT INTO plan_plus
WITH devices AS (
  select gen_random_uuid() AS device from generate_series(1, 10)
),
fields AS (
  select 'field '||f AS field from generate_series(1,100) AS f
)
SELECT
  t, device, field, 10, true, 'value', null, '2025-04-15 00:00:00'::timestamptz
FROM
  generate_series('2025-04-15 00:00:00'::timestamptz - interval '1 month',
    '2025-04-15 00:00:00'::timestamptz, interval '12 hour') AS t,
  devices, fields;

-- Compress data
SELECT count(compress_chunk(c)) FROM show_chunks('plan_plus') AS c;

-- Get one of the UUIDs
SELECT device FROM plan_plus LIMIT 1 \gset

SET timescaledb.debug_require_vector_agg = 'require';

-- Used to segfault
SELECT
    device = :'device'::uuid,
    field,
    SUM(value_numeric) AS value
FROM plan_plus
WHERE
    device=:'device'::uuid
    AND field='field 1'
    AND time > '2024-03-31T00:00:00+00:00'::timestamptz
    AND time < '2025-04-01T00:01:00+00:00'::timestamptz
GROUP BY device, field
LIMIT 1;

SELECT
    device = :'device'::uuid,
    SUM(value_numeric) AS value
FROM plan_plus
WHERE
    device=:'device'::uuid
    AND field='field 1'
    AND time > '2024-03-31T00:00:00+00:00'::timestamptz
    AND time < '2025-04-01T00:01:00+00:00'::timestamptz
GROUP BY device
LIMIT 1;

RESET timescaledb.debug_require_vector_agg;

SELECT count(*) FROM (SELECT device FROM plan_plus GROUP BY device) t;

SELECT count(*) FROM (SELECT device, field FROM plan_plus GROUP BY device, field) t;


-- UUID groupping
create table uuid_table(ts int, u uuid);
select count(*) from create_hypertable('uuid_table', 'ts', chunk_time_interval => 6);
alter table uuid_table set (timescaledb.compress);
insert into uuid_table values
	(1, '0197a7a9-b48b-7c71-92cb-eb724822bb0f'), (2, '0197a7a9-b48b-7c71-92cb-eb724822bb0f'), (3, '0197a7a9-b48b-7c71-92cb-eb724822bb0f'), (4, '0197a7a9-b48b-7c71-92cb-eb724822bb0f'),
	(5, '0197a7a9-b48b-7c75-a810-8acf630e634f'), (6, '0197a7a9-b48b-7c75-a810-8acf630e634f'), (7, '0197a7a9-b48b-7c76-b616-69e64a802b5c'), (8, '0197a7a9-b48b-7c77-b54f-c5f3d64d68d0'),
	(9, '0197a7a9-b48b-7c78-ab14-78b3dd81dbbc'), (10, '0197a7a9-b48b-7c78-ab14-78b3dd81dbbc'), (11, '0197a7a9-b48b-7c7a-8d9e-5afc3bf15234'), (12, '0197a7a9-b48b-7c7b-bc49-7150f16d8d63'),
	(13, '0197a7a9-b48b-7c7d-8cfe-9503ed9bb1c9'), (14, '0197a7a9-b48b-7c7d-8cfe-9503ed9bb1c9'), (15, '0197a7a9-b48b-7c7e-9ebb-acf63f5b625e'), (16, '0197a7a9-b48b-7c7f-a0c1-ba4adf950a2a'),
    (17, '01941f29-7c00-706a-bea9-105dad841304'), (18, '01941f2a-665f-7722-b4b5-cf4e70e666d0'),
	(19, NULL::uuid), (20, NULL::uuid), (21, NULL::uuid);

-- add a few derived columns
alter table uuid_table add column ver int;
alter table uuid_table add column uuid_ts timestamptz;

update uuid_table set
  ver = _timescaledb_functions.uuid_version(u) ,
  uuid_ts = _timescaledb_functions.timestamptz_from_uuid_v7_with_microseconds(u)
where u is not null;

select count(compress_chunk(x, true)) from show_chunks('uuid_table') x;

SET timescaledb.debug_require_vector_agg = 'require';

SELECT ver, u, ts, uuid_ts from uuid_table
where uuid_ts < '2025-06-25 16:16:46.347779+01' and ver = 7
order by 1,2;

SELECT ver, u, count(*), sum(ts) from uuid_table
where uuid_ts < '2025-06-25 16:16:46.347779+01' and ver = 7
group by 1,2 order by 1,2;

RESET timescaledb.debug_require_vector_agg;
