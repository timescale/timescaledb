-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Tests ALTER MATERIALIZED VIEW <cagg> ADD COLUMN ... GENERATED ALWAYS AS
-- (agg) STORED on continuous aggregates.

\pset null '<NULL>'

\c :TEST_DBNAME :ROLE_SUPERUSER
SET timezone TO 'UTC';

CREATE TABLE metrics (ts timestamptz NOT NULL, val double precision, temp double precision);
SELECT create_hypertable('metrics', 'ts', chunk_time_interval => INTERVAL '1 day');
GRANT SELECT ON metrics TO :ROLE_DEFAULT_PERM_USER_2;
INSERT INTO metrics
SELECT '2026-01-01 00:00:00+00'::timestamptz + (i * INTERVAL '1 hour'), i, i * 0.1
FROM generate_series(0, 71) i;

-- =====================================================================
-- Validation
-- =====================================================================

CREATE MATERIALIZED VIEW cagg_validation
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT time_bucket(INTERVAL '1 hour', ts) AS bucket, avg(val) AS avg_val
FROM metrics GROUP BY bucket
WITH NO DATA;
CALL refresh_continuous_aggregate('cagg_validation', '2026-01-01', '2026-01-03');

\set ON_ERROR_STOP 0

-- non-aggregate expression
ALTER MATERIALIZED VIEW cagg_validation
    ADD COLUMN not_agg double precision GENERATED ALWAYS AS (temp * 2) STORED;

-- unknown source column
ALTER MATERIALIZED VIEW cagg_validation
    ADD COLUMN bad_col double precision GENERATED ALWAYS AS (min(no_such_col)) STORED;

-- duplicate column name
ALTER MATERIALIZED VIEW cagg_validation
    ADD COLUMN avg_val double precision GENERATED ALWAYS AS (min(val)) STORED;

-- extra column constraints on the GENERATED column
ALTER MATERIALIZED VIEW cagg_validation
    ADD COLUMN min_temp_nn double precision NOT NULL GENERATED ALWAYS AS (min(temp)) STORED;

-- VIRTUAL is rejected; only STORED is supported
ALTER MATERIALIZED VIEW cagg_validation
    ADD COLUMN virt_temp double precision GENERATED ALWAYS AS (min(temp)) VIRTUAL;

\set ON_ERROR_STOP on

-- =====================================================================
-- Mixed ALTER subcommands
-- =====================================================================

\set ON_ERROR_STOP 0

ALTER MATERIALIZED VIEW cagg_validation
    ADD COLUMN regular_col integer;

ALTER MATERIALIZED VIEW cagg_validation
    ADD COLUMN regular_col integer,
    ADD COLUMN min_temp double precision GENERATED ALWAYS AS (min(temp)) STORED;
SELECT column_name FROM information_schema.columns
WHERE table_name='cagg_validation' AND column_name IN ('regular_col', 'min_temp');

\set ON_ERROR_STOP on

ALTER MATERIALIZED VIEW cagg_validation
    ADD COLUMN min_temp double precision GENERATED ALWAYS AS (min(temp)) STORED,
    OWNER TO :ROLE_DEFAULT_PERM_USER_2;
SELECT relowner::regrole AS owner_after
FROM pg_class WHERE oid='cagg_validation'::regclass;
SELECT count(*) AS has_min_temp FROM information_schema.columns
WHERE table_name='cagg_validation' AND column_name='min_temp';

ALTER MATERIALIZED VIEW cagg_validation
    ADD COLUMN max_val double precision GENERATED ALWAYS AS (max(val)) STORED,
    SET (timescaledb.materialized_only=true);
SELECT count(*) AS has_max_val FROM information_schema.columns
WHERE table_name='cagg_validation' AND column_name='max_val';

DROP MATERIALIZED VIEW cagg_validation;

-- Make sure we still don't allow ALTER MATERIALIZED VIEW ... ADD COLUMN on a regular mat view.
\set ON_ERROR_STOP off
CREATE MATERIALIZED VIEW reg_mv AS SELECT 1 AS x;
ALTER MATERIALIZED VIEW reg_mv
    ADD COLUMN y integer GENERATED ALWAYS AS (x) STORED;
\set ON_ERROR_STOP on

DROP MATERIALIZED VIEW reg_mv;

-- =====================================================================
-- Real-time CAggs (materialized_only=false)
-- =====================================================================

CREATE MATERIALIZED VIEW cagg_rt
WITH (timescaledb.continuous, timescaledb.materialized_only=false) AS
SELECT time_bucket(INTERVAL '1 hour', ts) AS bucket, avg(val) AS avg_val
FROM metrics GROUP BY bucket
WITH NO DATA;
-- materialize only the first 24h; the rest is served live from the source
CALL refresh_continuous_aggregate('cagg_rt', '2026-01-01', '2026-01-02');

ALTER MATERIALIZED VIEW cagg_rt
    ADD COLUMN min_temp double precision GENERATED ALWAYS AS (min(temp)) STORED;

-- materialized side: NULL on history; live side: computed at query time
SELECT count(*) AS materialized_not_null FROM cagg_rt
WHERE bucket < '2026-01-02 00:00:00+00' AND min_temp IS NOT NULL;

SELECT count(*) > 0 AND count(*) FILTER (WHERE min_temp IS NULL) = 0
FROM cagg_rt WHERE bucket >= '2026-01-02 00:00:00+00';

SELECT pg_get_viewdef('cagg_rt'::regclass);

-- catalog flag is restored to false (still a real-time CAgg)
SELECT materialized_only FROM _timescaledb_catalog.continuous_agg
WHERE user_view_name='cagg_rt';

DROP MATERIALIZED VIEW cagg_rt;

-- =====================================================================
-- Add column that's an existing GROUP BY column
-- =====================================================================

CREATE TABLE groupby_col (ts timestamptz NOT NULL, device_id int, val double precision);
SELECT create_hypertable('groupby_col', 'ts');
INSERT INTO groupby_col
SELECT '2026-01-01'::timestamptz + (i/2)*INTERVAL '1 hour', (i%2)+1, i
FROM generate_series(0,7) i;

CREATE MATERIALIZED VIEW cagg_groupby
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT time_bucket(INTERVAL '1 hour', ts) AS bucket, avg(val) AS avg_val
FROM groupby_col GROUP BY bucket, device_id
WITH NO DATA;
CALL refresh_continuous_aggregate('cagg_groupby', NULL, NULL);

ALTER MATERIALIZED VIEW cagg_groupby
    ADD COLUMN dev int GENERATED ALWAYS AS (device_id) STORED;

CALL refresh_continuous_aggregate('cagg_groupby', NULL, NULL, force => true);
SELECT bucket, dev, avg_val FROM cagg_groupby ORDER BY bucket, dev;

DROP MATERIALIZED VIEW cagg_groupby;
DROP TABLE groupby_col;

-- =====================================================================
-- End-to-end: JOINed source, multi-column ALTER
--   -> refresh -> ALTER -> verify NULL on history
--   -> insert more data -> refresh new range -> verify populated
--   -> force-refresh history -> verify history populated
-- =====================================================================

CREATE TABLE devices (device_id int PRIMARY KEY, name text);
INSERT INTO devices VALUES (1, 'alpha'), (2, 'beta');

ALTER TABLE metrics ADD COLUMN device_id int DEFAULT 1;
UPDATE metrics SET device_id = CASE WHEN extract(hour from ts)::int % 2 = 0 THEN 1 ELSE 2 END;

CREATE MATERIALIZED VIEW cagg_e2e
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT time_bucket(INTERVAL '1 hour', metrics.ts) AS bucket,
       devices.name AS device_name,
       avg(metrics.val) AS avg_val
FROM metrics JOIN devices USING (device_id)
GROUP BY bucket, devices.name
WITH NO DATA;

-- 1-day mat HT chunks so the initial refresh produces multiple chunks
SELECT set_chunk_time_interval('cagg_e2e', chunk_time_interval => INTERVAL '1 day');

-- refresh the first two days (2 mat HT chunks)
CALL refresh_continuous_aggregate('cagg_e2e',
                                  '2026-01-01 00:00:00+00'::timestamptz,
                                  '2026-01-03 00:00:00+00'::timestamptz);

-- multiple new columns
ALTER MATERIALIZED VIEW cagg_e2e
    ADD COLUMN min_temp double precision GENERATED ALWAYS AS (min(metrics.temp)) STORED,
    ADD COLUMN max_temp double precision GENERATED ALWAYS AS (max(metrics.temp)) STORED,
    ADD COLUMN devices_seen text GENERATED ALWAYS AS (string_agg(DISTINCT devices.name, ',')) STORED;

-- direct/partial views reflect the new aggregations
SELECT pg_get_viewdef(format('%I.%I', direct_view_schema, direct_view_name)::regclass, true)
FROM _timescaledb_catalog.continuous_agg WHERE user_view_name='cagg_e2e';
SELECT pg_get_viewdef(format('%I.%I', partial_view_schema, partial_view_name)::regclass, true)
FROM _timescaledb_catalog.continuous_agg WHERE user_view_name='cagg_e2e';

-- insert MORE data, beyond the originally-loaded range
INSERT INTO metrics
SELECT '2026-01-04 00:00:00+00'::timestamptz + (i * INTERVAL '1 hour'),
       100 + i,
       (100 + i) * 0.5,
       CASE WHEN i % 2 = 0 THEN 1 ELSE 2 END
FROM generate_series(0, 23) i;

-- refresh the new range
CALL refresh_continuous_aggregate('cagg_e2e',
                                  '2026-01-04 00:00:00+00'::timestamptz,
                                  '2026-01-05 00:00:00+00'::timestamptz);

-- new buckets populated on all new columns; old (pre-ALTER) still NULL
SELECT
    sum(CASE WHEN bucket < '2026-01-03 00:00:00+00' AND min_temp IS NULL
             AND max_temp IS NULL AND devices_seen IS NULL THEN 1 ELSE 0 END) AS old_all_null,
    sum(CASE WHEN bucket >= '2026-01-04 00:00:00+00' AND min_temp IS NOT NULL
             AND max_temp IS NOT NULL AND devices_seen IS NOT NULL THEN 1 ELSE 0 END) AS new_all_populated,
    count(*) AS total_buckets
FROM cagg_e2e;

-- force-refresh the pre-ALTER range (spans 2 chunks)
CALL refresh_continuous_aggregate('cagg_e2e',
                                  '2026-01-01 00:00:00+00'::timestamptz,
                                  '2026-01-03 00:00:00+00'::timestamptz,
                                  force => true);

-- verify that new columns are now populated on the pre-ALTER range too
SELECT count(*) AS rows_still_null FROM cagg_e2e
WHERE bucket < '2026-01-03 00:00:00+00'
  AND (min_temp IS NULL OR max_temp IS NULL OR devices_seen IS NULL);

DROP MATERIALIZED VIEW cagg_e2e;
DROP TABLE devices;
ALTER TABLE metrics DROP COLUMN device_id;

-- =====================================================================
-- Atomic rollback in case of bad expression
-- =====================================================================

CREATE MATERIALIZED VIEW v_atomic
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT time_bucket(INTERVAL '1 hour', ts) AS bucket, avg(val) AS avg_val
FROM metrics GROUP BY bucket
WITH NO DATA;
CALL refresh_continuous_aggregate('v_atomic', '2026-01-01', '2026-01-02');

\set ON_ERROR_STOP 0
-- Bad expression: column doesn't exist on the source HT.
ALTER MATERIALIZED VIEW v_atomic
    ADD COLUMN bad_col double precision
    GENERATED ALWAYS AS (min(no_such_column)) STORED;
\set ON_ERROR_STOP on

-- Mat HT must NOT have bad_col after the failed ALTER.
SELECT count(*) AS mat_ht_has_bad_col
FROM _timescaledb_catalog.hypertable h, pg_attribute a
WHERE h.id = (SELECT mat_hypertable_id FROM _timescaledb_catalog.continuous_agg
              WHERE user_view_name='v_atomic')
  AND a.attrelid = format('%I.%I', h.schema_name, h.table_name)::regclass
  AND a.attname = 'bad_col' AND NOT a.attisdropped;

-- Verify view queries has not changed
SELECT pg_get_viewdef(format('%I.%I', direct_view_schema, direct_view_name)::regclass, true)
FROM _timescaledb_catalog.continuous_agg WHERE user_view_name='v_atomic';

SELECT pg_get_viewdef(format('%I.%I', partial_view_schema, partial_view_name)::regclass, true)
FROM _timescaledb_catalog.continuous_agg WHERE user_view_name='v_atomic';

-- The CAgg must remain fully usable after the failed ALTER§
INSERT INTO metrics VALUES ('2026-01-02 12:00:00+00', 999, 9.99);
CALL refresh_continuous_aggregate('v_atomic', '2026-01-02', '2026-01-03');
SELECT count(*) > 0 AS refresh_after_failed_alter_works FROM v_atomic;

DROP MATERIALIZED VIEW v_atomic;

-- =====================================================================
-- Compressed mat HT: ADD COLUMN propagates to the compressed chunks
-- =====================================================================

CREATE MATERIALIZED VIEW cagg_compressed
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT time_bucket(INTERVAL '1 hour', ts) AS bucket, avg(val) AS avg_val
FROM metrics GROUP BY bucket
WITH NO DATA;
-- 1-day mat HT chunks so the 3-day refresh below produces 3 chunks
SELECT set_chunk_time_interval('cagg_compressed', chunk_time_interval => INTERVAL '1 day');
CALL refresh_continuous_aggregate('cagg_compressed',
                                  '2026-01-01 00:00:00+00'::timestamptz,
                                  '2026-01-04 00:00:00+00'::timestamptz);

ALTER MATERIALIZED VIEW cagg_compressed SET (timescaledb.compress = true);
SELECT count(*) AS chunks_compressed FROM (
    SELECT compress_chunk(c) FROM show_chunks('cagg_compressed') c
) s;

ALTER MATERIALIZED VIEW cagg_compressed
    ADD COLUMN min_temp double precision GENERATED ALWAYS AS (min(temp)) STORED;

-- look up the mat HT (schema, name) and the compressed HT id once.
SELECT h.id as mat_ht_id,
       h.compressed_hypertable_id AS compressed_ht_id
FROM _timescaledb_catalog.continuous_agg ca
JOIN _timescaledb_catalog.hypertable h ON h.id = ca.mat_hypertable_id
WHERE ca.user_view_name = 'cagg_compressed' \gset

-- every mat HT chunk has min_temp
SELECT count(*) AS mat_ht_chunks_with_min_temp
FROM information_schema.columns ic, _timescaledb_catalog.chunk c
WHERE c.hypertable_id = :mat_ht_id
  AND ic.table_schema = c.schema_name
  AND ic.table_name = c.table_name
  AND ic.column_name = 'min_temp';

-- every compressed chunk has min_temp
SELECT count(*) AS compressed_chunks_with_min_temp
FROM information_schema.columns ic, _timescaledb_catalog.chunk c
WHERE c.hypertable_id = :compressed_ht_id
  AND ic.table_schema = c.schema_name
  AND ic.table_name = c.table_name
  AND ic.column_name = 'min_temp';

-- before any further refresh, every row's min_temp is NULL
SELECT count(*) AS rows_with_non_null_min_temp FROM cagg_compressed WHERE min_temp IS NOT NULL;

CALL refresh_continuous_aggregate('cagg_compressed',
                                  '2026-01-02 00:00:00+00'::timestamptz,
                                  '2026-01-03 00:00:00+00'::timestamptz,
                                  force => true);
SELECT count(*) FROM cagg_compressed
WHERE bucket >= '2026-01-02 00:00:00+00' AND bucket < '2026-01-03 00:00:00+00'
AND min_temp IS NOT NULL;

DROP MATERIALIZED VIEW cagg_compressed;

-- =====================================================================
-- Metadata preservation: owner, GRANT, COMMENT survive (OID is stable)
-- =====================================================================

CREATE MATERIALIZED VIEW cagg_meta
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT time_bucket(INTERVAL '1 hour', ts) AS bucket, avg(val) AS avg_val
FROM metrics GROUP BY bucket
WITH NO DATA;
CALL refresh_continuous_aggregate('cagg_meta', '2026-01-01', '2026-01-02');

GRANT SELECT ON cagg_meta TO :ROLE_DEFAULT_PERM_USER_2;
COMMENT ON VIEW cagg_meta IS 'hourly average of val';
ALTER MATERIALIZED VIEW cagg_meta OWNER TO :ROLE_DEFAULT_PERM_USER;

-- before ALTER
SELECT relowner::regrole::text AS owner FROM pg_class WHERE oid='cagg_meta'::regclass;
SELECT grantee, privilege_type
FROM information_schema.role_table_grants
WHERE table_name='cagg_meta' AND grantee=:'ROLE_DEFAULT_PERM_USER_2'
ORDER BY privilege_type;
SELECT obj_description('cagg_meta'::regclass, 'pg_class') AS view_comment;

ALTER MATERIALIZED VIEW cagg_meta
    ADD COLUMN min_temp double precision GENERATED ALWAYS AS (min(temp)) STORED;

-- after ALTER: owner / GRANT / comment intact
SELECT relowner::regrole::text AS owner FROM pg_class WHERE oid='cagg_meta'::regclass;
SELECT grantee, privilege_type
FROM information_schema.role_table_grants
WHERE table_name='cagg_meta' AND grantee=:'ROLE_DEFAULT_PERM_USER_2'
ORDER BY privilege_type;
SELECT obj_description('cagg_meta'::regclass, 'pg_class') AS view_comment;

-- granted role can read the new column
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2
SELECT count(*) AS rows_visible FROM cagg_meta;
\c :TEST_DBNAME :ROLE_SUPERUSER

DROP MATERIALIZED VIEW cagg_meta;

-- =====================================================================
-- Hierarchical CAgg: ADD COLUMN on parent, then on a child that
-- references the parent's newly-added column.
-- =====================================================================

CREATE TABLE hier_src (ts timestamptz NOT NULL, val double precision, temp double precision);
SELECT create_hypertable('hier_src', 'ts', chunk_time_interval => INTERVAL '1 day');
INSERT INTO hier_src
SELECT '2026-01-01'::timestamptz + i*INTERVAL '1 hour', i, i * 0.1
FROM generate_series(0, 47) i;

CREATE MATERIALIZED VIEW cagg_parent
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT time_bucket(INTERVAL '1 hour', ts) AS bucket, avg(val) AS avg_val
FROM hier_src GROUP BY bucket
WITH NO DATA;
CALL refresh_continuous_aggregate('cagg_parent', NULL, NULL);

CREATE MATERIALIZED VIEW cagg_child
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT time_bucket(INTERVAL '1 day', bucket) AS day_bucket, avg(avg_val) AS daily_avg
FROM cagg_parent GROUP BY day_bucket
WITH NO DATA;
CALL refresh_continuous_aggregate('cagg_child', NULL, NULL);

-- ADD COLUMN to parent
ALTER MATERIALIZED VIEW cagg_parent
    ADD COLUMN min_temp double precision GENERATED ALWAYS AS (min(temp)) STORED;
SELECT count(*) AS parent_rows_with_min_temp FROM cagg_parent WHERE min_temp IS NOT NULL;

-- ADD COLUMN to child, referencing an existing parent column
ALTER MATERIALIZED VIEW cagg_child
    ADD COLUMN min_avg double precision GENERATED ALWAYS AS (min(avg_val)) STORED;

-- ADD COLUMN to child, referencing parent's newly-added min_temp.
ALTER MATERIALIZED VIEW cagg_child
    ADD COLUMN min_min_temp double precision GENERATED ALWAYS AS (min(min_temp)) STORED;
CALL refresh_continuous_aggregate('cagg_child', NULL, NULL, force => true);
SELECT day_bucket, daily_avg, min_avg, min_min_temp FROM cagg_child;

-- refresh both caggs to populate the new columns on all rows
CALL refresh_continuous_aggregate('cagg_parent', NULL, NULL, force => true);
CALL refresh_continuous_aggregate('cagg_child', NULL, NULL, force => true);
SELECT day_bucket, daily_avg, min_avg, min_min_temp FROM cagg_child;

DROP MATERIALIZED VIEW cagg_child;
DROP MATERIALIZED VIEW cagg_parent;
DROP TABLE hier_src;

DROP TABLE metrics;
