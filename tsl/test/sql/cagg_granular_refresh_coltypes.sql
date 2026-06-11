-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Granular refresh across tenant column TYPES.
--
-- Exercises the tenant-key encode/decode that granular refresh relies on for the
-- supported tracking column types: the pure encode/decode round-trip, the
-- DateStyle mismatch the canonical encode must guard against, and end-to-end
-- granular refreshes with int, uuid, date and domain tenant columns.

SET timezone TO 'UTC';

-- Anchor the start offset to a fixed date safely before all the fixed 2020
-- fixture dates below, computed relative to today so the window keeps
-- covering them regardless of when this test actually runs.
SELECT (CURRENT_DATE - DATE '2019-01-01')::text || ' days' AS granular_refresh_lookback \gset

-- ============================================================================
-- Tenant-key encode/decode round-trip (outside the cagg refresh context, just
-- confirm that the functions we use work correctly).
--
-- Encode (as record_tenant_invalidation does): store the tenant value's text
-- form, like the tenant_id text column in continuous_aggs_tenant_tracking.
-- Decode (as build_tenant_predicate does): encoded::<type>.  This covers the
-- encode/decode scheme for different tenant types.
-- ============================================================================
CREATE TABLE tenant_key(coltype text, encoded text);

-- ENCODE: store each tenant value's text form.
-- Cover the supported tracking column types (and their variants).
INSERT INTO tenant_key VALUES
  ('int2',    (32767)::int2::text),
  ('int4',    (42)::int4::text),
  ('int8',    (9223372036854775807)::int8::text),
  ('text',    'sensor_a'),
  ('varchar', ('sensor_b')::varchar::text),
  ('bpchar',  ('sensor_c')::bpchar::text),
  ('uuid',    ('00000000-0000-0000-0000-000000000001')::uuid::text),
  ('date',    ('2020-01-02')::date::text);

-- Stored form is the value's text (e.g. int4 42 -> "42"); the byte count is the
-- length of that text form.
SELECT coltype, encoded, octet_length(encoded) AS bytes FROM tenant_key ORDER BY coltype;

-- DECODE each back to its type and confirm it equals the original value.
SELECT 'int2'    AS coltype, encoded::int2    = 32767::int2                                    AS same_as_original FROM tenant_key WHERE coltype = 'int2'
UNION ALL SELECT 'int4',    encoded::int4    = 42::int4                                        FROM tenant_key WHERE coltype = 'int4'
UNION ALL SELECT 'int8',    encoded::int8    = 9223372036854775807::int8                       FROM tenant_key WHERE coltype = 'int8'
UNION ALL SELECT 'text',    encoded::text    = 'sensor_a'                                      FROM tenant_key WHERE coltype = 'text'
UNION ALL SELECT 'varchar', encoded::varchar = 'sensor_b'::varchar                             FROM tenant_key WHERE coltype = 'varchar'
UNION ALL SELECT 'bpchar',  encoded::bpchar  = 'sensor_c'::bpchar                              FROM tenant_key WHERE coltype = 'bpchar'
UNION ALL SELECT 'uuid',    encoded::uuid    = '00000000-0000-0000-0000-000000000001'::uuid    FROM tenant_key WHERE coltype = 'uuid'
UNION ALL SELECT 'date',    encoded::date    = '2020-01-02'::date                              FROM tenant_key WHERE coltype = 'date'
ORDER BY coltype;

DROP TABLE tenant_key;

-- ============================================================================
-- Demonstrate the DateStyle-dependent encode/decode MISMATCH (outside the cagg
-- refresh).  DATE is the only supported type whose text form depends on a
-- session GUC.  record_tenant_invalidation encodes at INSERT time under the
-- inserting session's DateStyle and build_tenant_predicate decodes at REFRESH
-- time under the refreshing session's DateStyle; a difference makes the stored
-- text re-parse to a different value.  This is why the encode pins DateStyle to
-- a canonical ISO form (proven end-to-end in the gm_daily test below).
-- ============================================================================
CREATE TABLE guc_key(coltype text, encoded text);

-- ENCODE a date under one DateStyle (as the inserting session would).
SET datestyle = 'SQL, MDY';
INSERT INTO guc_key VALUES ('date', ('2020-01-02')::date::text);

-- Stored MM/DD/YYYY under this DateStyle.
SELECT coltype, encoded AS stored_text FROM guc_key ORDER BY coltype;

-- DECODE under a DIFFERENT DateStyle: same_as_original = false exposes the raw
-- hazard (01/02/2020 re-parses as Feb 1, not Jan 2) that the canonical encode
-- prevents.
SET datestyle = 'SQL, DMY';
SELECT 'date' AS coltype, encoded::date = '2020-01-02'::date AS same_as_original
FROM guc_key WHERE coltype = 'date';

RESET datestyle;
DROP TABLE guc_key;

-- ============================================================================
-- 3 tenants share one bucket, but only tenant 1 is modified before the second
-- refresh.  The granular refresh rewrites just tenant 1 (deleted 1 + inserted 1),
-- while a forced full refresh over the same window rewrites all 3.
-- ============================================================================
CREATE TABLE s_metrics(time timestamptz NOT NULL, sensor_id int, value float);
SELECT create_hypertable('s_metrics', 'time');
ALTER TABLE s_metrics SET (
    timescaledb.granular_refresh_column = 'sensor_id',
    timescaledb.granular_refresh_start_offset = :'granular_refresh_lookback',
    timescaledb.granular_refresh_end_offset = '1 day'
);

CREATE MATERIALIZED VIEW s_daily
  WITH (timescaledb.continuous) AS
  SELECT time_bucket('1 day', time) AS bucket, sensor_id, avg(value)
  FROM s_metrics
  GROUP BY bucket, sensor_id
  WITH NO DATA;
ALTER MATERIALIZED VIEW s_daily SET (timescaledb.enable_granular_refresh = true);

CALL refresh_continuous_aggregate('s_daily', NULL, '2025-05-01 00:00+00');

-- Three tenants in the same bucket -> all 3 dirty on the first refresh.
INSERT INTO s_metrics VALUES
  ('2020-01-01 00:00+00', 1, 10),
  ('2020-01-01 00:00+00', 2, 20),
  ('2020-01-01 00:00+00', 3, 30);

SET client_min_messages TO LOG;
-- All 3 tracked -> inserts 3 rows.
CALL refresh_continuous_aggregate('s_daily', NULL, '2025-05-01 00:00+00');
RESET client_min_messages;

-- Modify only tenant 1 -> only tenant 1 is dirty in the new generation.
INSERT INTO s_metrics VALUES ('2020-01-01 00:00+00', 1, 50);

SET client_min_messages TO LOG;
-- Granular: only tenant 1 is rewritten (deleted 1 + inserted 1) even though 3
-- tenants exist in the window.
CALL refresh_continuous_aggregate('s_daily', NULL, '2025-05-01 00:00+00');
RESET client_min_messages;

SET client_min_messages TO LOG;
-- Forced full refresh over the same window rewrites all 3, for contrast.
CALL refresh_continuous_aggregate('s_daily', '2020-01-01 00:00+00', '2020-01-02 00:00+00', force => true);
RESET client_min_messages;

SELECT sensor_id, avg
FROM s_daily
ORDER BY sensor_id;

DROP MATERIALIZED VIEW s_daily;
DROP TABLE s_metrics;

-- ============================================================================
-- Same test with a uuid tenant column (int above is by-value, uuid is
-- fixed-length by-reference).  Confirms the granular
-- delete/insert scoping and the ::uuid decode predicate work for a uuid type.
-- ============================================================================
CREATE TABLE su_metrics(time timestamptz NOT NULL, sensor_id uuid, value float);
SELECT create_hypertable('su_metrics', 'time');
ALTER TABLE su_metrics SET (
    timescaledb.granular_refresh_column = 'sensor_id',
    timescaledb.granular_refresh_start_offset = :'granular_refresh_lookback',
    timescaledb.granular_refresh_end_offset = '1 day'
);

CREATE MATERIALIZED VIEW su_daily
  WITH (timescaledb.continuous) AS
  SELECT time_bucket('1 day', time) AS bucket, sensor_id, avg(value)
  FROM su_metrics
  GROUP BY bucket, sensor_id
  WITH NO DATA;
ALTER MATERIALIZED VIEW su_daily SET (timescaledb.enable_granular_refresh = true);

CALL refresh_continuous_aggregate('su_daily', NULL, '2025-05-01 00:00+00');

-- Three uuid tenants in the same bucket -> all 3 dirty on the first refresh.
INSERT INTO su_metrics VALUES
  ('2020-01-01 00:00+00', '00000000-0000-0000-0000-000000000001', 10),
  ('2020-01-01 00:00+00', '00000000-0000-0000-0000-000000000002', 20),
  ('2020-01-01 00:00+00', '00000000-0000-0000-0000-000000000003', 30);

SET client_min_messages TO LOG;
-- All 3 tracked -> inserts 3 rows.
CALL refresh_continuous_aggregate('su_daily', NULL, '2025-05-01 00:00+00');
RESET client_min_messages;

-- Modify only tenant ...0001 -> only it is dirty in the new generation.
INSERT INTO su_metrics VALUES ('2020-01-01 00:00+00', '00000000-0000-0000-0000-000000000001', 50);

SET client_min_messages TO LOG;
-- Granular: only tenant ...0001 is rewritten (deleted 1 + inserted 1) even
-- though 3 tenants exist in the window.
CALL refresh_continuous_aggregate('su_daily', NULL, '2025-05-01 00:00+00');
RESET client_min_messages;

SET client_min_messages TO LOG;
-- Forced full refresh over the same window rewrites all 3, for contrast.
CALL refresh_continuous_aggregate('su_daily', '2020-01-01 00:00+00', '2020-01-02 00:00+00', force => true);
RESET client_min_messages;

SELECT sensor_id, avg
FROM su_daily
ORDER BY sensor_id;

DROP MATERIALIZED VIEW su_daily;
DROP TABLE su_metrics;

-- ============================================================================
-- END-TO-END test for the DateStyle mismatch.  The tenant column (sensor_id) is
-- a date -- the one supported type whose text form depends on DateStyle.  We
-- INSERT the late-arriving row under one DateStyle and REFRESH under a different
-- one; the canonical (ISO) encode must make the stored tenant re-parse to the
-- same value, so the late row is not missed.
--
-- Without canonical encoding the mismatch would leave gm_daily.avg at 10 (the
-- late value 20 ignored); with it, avg becomes 15 (= avg(10, 20)).
-- ============================================================================
CREATE TABLE gm(time timestamptz NOT NULL, sensor_id date, value float);
SELECT create_hypertable('gm', 'time');
ALTER TABLE gm SET (
    timescaledb.granular_refresh_column = 'sensor_id',
    timescaledb.granular_refresh_start_offset = :'granular_refresh_lookback',
    timescaledb.granular_refresh_end_offset = '1 day'
);

CREATE MATERIALIZED VIEW gm_daily
  WITH (timescaledb.continuous) AS
  SELECT time_bucket('1 day', time) AS bucket, sensor_id, avg(value)
  FROM gm
  GROUP BY bucket, sensor_id
  WITH NO DATA;
ALTER MATERIALIZED VIEW gm_daily SET (timescaledb.enable_granular_refresh = true);

CALL refresh_continuous_aggregate('gm_daily', NULL, '2025-05-01 00:00+00');

-- '2020-01-02' is ISO input, so the stored tenant is Jan 2 regardless of DateStyle.
INSERT INTO gm VALUES ('2025-01-01 00:00+00', '2020-01-02', 10);
CALL refresh_continuous_aggregate('gm_daily', NULL, '2025-05-01 00:00+00');

SET datestyle = 'SQL, MDY';
INSERT INTO gm VALUES ('2025-01-01 00:00+00', '2020-01-02', 20);

-- Refresh under DMY: must not miss the late-arriving row (avg should be 15, not 10).
SET datestyle = 'SQL, DMY';
SET client_min_messages TO LOG;

CALL refresh_continuous_aggregate('gm_daily', NULL, '2025-05-01 00:00+00');
RESET client_min_messages;

-- (note that we still keep the DMY datestyle for the select query result below)
SELECT sensor_id, bucket, avg
FROM gm_daily
ORDER BY sensor_id, bucket;
RESET datestyle;

DROP MATERIALIZED VIEW gm_daily;
DROP TABLE gm;

-- ============================================================================
-- Domain tenant column: getBaseType resolves the domain to its base type (int),
-- so it is accepted and tracked exactly like a plain int column.
-- ============================================================================
CREATE DOMAIN sensor_id_dom AS int;
CREATE TABLE dm(time timestamptz NOT NULL, sensor_id sensor_id_dom, value float);
SELECT create_hypertable('dm', 'time');
ALTER TABLE dm SET (
    timescaledb.granular_refresh_column = 'sensor_id',
    timescaledb.granular_refresh_start_offset = :'granular_refresh_lookback',
    timescaledb.granular_refresh_end_offset = '1 day'
);

CREATE MATERIALIZED VIEW dm_daily
  WITH (timescaledb.continuous) AS
  SELECT time_bucket('1 day', time) AS bucket, sensor_id, avg(value)
  FROM dm
  GROUP BY bucket, sensor_id
  WITH NO DATA;
ALTER MATERIALIZED VIEW dm_daily SET (timescaledb.enable_granular_refresh = true);

CALL refresh_continuous_aggregate('dm_daily', NULL, '2025-05-01 00:00+00');

INSERT INTO dm VALUES
  ('2020-01-01 00:00+00', 1, 10),
  ('2020-01-01 00:00+00', 2, 20);
CALL refresh_continuous_aggregate('dm_daily', NULL, '2025-05-01 00:00+00');

-- Modify only tenant 1; the granular refresh must rewrite just tenant 1.
INSERT INTO dm VALUES ('2020-01-01 00:00+00', 1, 50);
SET client_min_messages TO LOG;
CALL refresh_continuous_aggregate('dm_daily', NULL, '2025-05-01 00:00+00');
RESET client_min_messages;

SELECT sensor_id, avg FROM dm_daily ORDER BY sensor_id;

DROP MATERIALIZED VIEW dm_daily;
DROP TABLE dm;
DROP DOMAIN sensor_id_dom;
