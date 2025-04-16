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
