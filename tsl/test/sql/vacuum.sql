-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

-- Test VACUUM FULL with compressed chunks and missing attributes
CREATE TABLE vacuum_missing_test(ts int, c1 int);
SELECT create_hypertable('vacuum_missing_test', 'ts', chunk_time_interval => 1000);
INSERT INTO vacuum_missing_test VALUES (0, 1);
ALTER TABLE vacuum_missing_test SET (timescaledb.compress, timescaledb.compress_segmentby = '');
SELECT compress_chunk(show_chunks('vacuum_missing_test'), true);
ALTER TABLE vacuum_missing_test ADD COLUMN c2 int DEFAULT 7;
VACUUM FULL ANALYZE vacuum_missing_test;
SELECT * FROM vacuum_missing_test;
DROP TABLE vacuum_missing_test;

-- Test VACUUM FULL with partially compressed chunks
SET timescaledb.enable_direct_compress_insert = true;

CREATE TABLE vacuum_partial_test (time TIMESTAMPTZ NOT NULL, device TEXT, value float)
WITH (tsdb.hypertable, tsdb.orderby='time');

INSERT INTO vacuum_partial_test
SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd1', i::float
FROM generate_series(0,100) i;

INSERT INTO vacuum_partial_test
SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd2', i::float
FROM generate_series(101,105) i;

ALTER TABLE vacuum_partial_test ADD COLUMN c2 int DEFAULT 10;

SELECT chunk FROM show_chunks('vacuum_partial_test') AS chunk LIMIT 1 \gset
SELECT _timescaledb_functions.chunk_status_text(:'chunk'::regclass) AS status_before;

SELECT attname, atthasmissing, attmissingval
FROM pg_attribute
WHERE attrelid = :'chunk'::regclass
  AND attnum > 0 AND NOT attisdropped
ORDER BY attnum;

SELECT COUNT(c2) FROM vacuum_partial_test;

VACUUM FULL ANALYZE vacuum_partial_test;

SELECT COUNT(c2) FROM vacuum_partial_test; -- should be the same

SELECT _timescaledb_functions.chunk_status_text(:'chunk'::regclass) AS status_after;

SELECT attname, atthasmissing, attmissingval
FROM pg_attribute
WHERE attrelid = :'chunk'::regclass
  AND attnum > 0 AND NOT attisdropped
ORDER BY attnum;

DROP TABLE vacuum_partial_test;

RESET timescaledb.enable_direct_compress_insert_client_sorted;
RESET timescaledb.enable_direct_compress_insert;

-- Test VACUUM FULL with unordered compressed chunks
SET timescaledb.enable_direct_compress_insert = true;

CREATE TABLE vacuum_unordered_test (time TIMESTAMPTZ NOT NULL, device TEXT, value float)
WITH (tsdb.hypertable, tsdb.orderby='time');

INSERT INTO vacuum_unordered_test
SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd1', i::float
FROM generate_series(0,50) i;

INSERT INTO vacuum_unordered_test
SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd1', i::float
FROM generate_series(40,150) i;

ALTER TABLE vacuum_unordered_test ADD COLUMN c2 int DEFAULT 20;

SELECT chunk FROM show_chunks('vacuum_unordered_test') AS chunk LIMIT 1 \gset
SELECT _timescaledb_functions.chunk_status_text(:'chunk'::regclass) AS status_before;

SELECT attname, atthasmissing, attmissingval
FROM pg_attribute
WHERE attrelid = :'chunk'::regclass
  AND attnum > 0 AND NOT attisdropped
ORDER BY attnum;

SELECT COUNT(c2) FROM vacuum_unordered_test;

VACUUM FULL ANALYZE vacuum_unordered_test;

SELECT COUNT(c2) FROM vacuum_unordered_test; -- should be the same

SELECT _timescaledb_functions.chunk_status_text(:'chunk'::regclass) AS status_after;

SELECT attname, atthasmissing, attmissingval
FROM pg_attribute
WHERE attrelid = :'chunk'::regclass
  AND attnum > 0 AND NOT attisdropped
ORDER BY attnum;

DROP TABLE vacuum_unordered_test;

RESET timescaledb.enable_direct_compress_insert;

-- Test VACUUM FULL with changed compression settings (fallsback to internal decompress/compress)
SET timescaledb.enable_direct_compress_insert = true;

CREATE TABLE vacuum_settings_test (time TIMESTAMPTZ NOT NULL, device TEXT, value float)
WITH (tsdb.hypertable, tsdb.orderby='time');

INSERT INTO vacuum_settings_test
SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd1', i::float
FROM generate_series(0,50) i;

INSERT INTO vacuum_settings_test
SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd1', i::float
FROM generate_series(40,150) i;

INSERT INTO vacuum_settings_test
SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd2', i::float
FROM generate_series(101,105) i;

ALTER TABLE vacuum_settings_test SET (timescaledb.compress, timescaledb.compress_segmentby = 'device');

ALTER TABLE vacuum_settings_test ADD COLUMN c2 int DEFAULT 20;

SELECT chunk FROM show_chunks('vacuum_settings_test') AS chunk LIMIT 1 \gset
SELECT _timescaledb_functions.chunk_status_text(:'chunk'::regclass) AS status_before;

SELECT attname, atthasmissing, attmissingval
FROM pg_attribute
WHERE attrelid = :'chunk'::regclass
  AND attnum > 0 AND NOT attisdropped
ORDER BY attnum;

SELECT COUNT(c2) FROM vacuum_settings_test;

VACUUM FULL ANALYZE vacuum_settings_test;

SELECT COUNT(c2) FROM vacuum_settings_test; -- should be the same

SELECT _timescaledb_functions.chunk_status_text(:'chunk'::regclass) AS status_after;

SELECT attname, atthasmissing, attmissingval
FROM pg_attribute
WHERE attrelid = :'chunk'::regclass
  AND attnum > 0 AND NOT attisdropped
ORDER BY attnum;

DROP TABLE vacuum_settings_test;

RESET timescaledb.enable_direct_compress_insert;

-- ANALYZE/VACUUM on a continuous aggregate user-view should be
-- redirected to the underlying materialization hypertable.
CREATE TABLE cagg_analyze_src(time timestamptz NOT NULL, device int, value float);
SELECT create_hypertable('cagg_analyze_src', 'time', chunk_time_interval => interval '1 day');

INSERT INTO cagg_analyze_src
SELECT '2024-01-01'::timestamptz + (i || ' minute')::interval,
       i % 4,
       i::float
FROM generate_series(0, 5000) i;

CREATE MATERIALIZED VIEW cagg_analyze_view
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 hour', time) AS bucket, device, avg(value) AS avg_value
FROM cagg_analyze_src
GROUP BY 1, 2
WITH NO DATA;

CALL refresh_continuous_aggregate('cagg_analyze_view', NULL, NULL);

-- Locate the materialization hypertable so we can check its stats.
SELECT format('%I.%I', h.schema_name, h.table_name) AS mat_ht
FROM _timescaledb_catalog.continuous_agg c
JOIN _timescaledb_catalog.hypertable h ON h.id = c.mat_hypertable_id
WHERE c.user_view_name = 'cagg_analyze_view' \gset

-- No analyze stats yet on the materialization hypertable.
SELECT relname, last_analyze IS NOT NULL AS has_analyze
FROM pg_stat_all_tables
WHERE relid = :'mat_ht'::regclass;

CREATE FUNCTION cagg_analyze_count_analyzed(view_name name) RETURNS bigint
LANGUAGE sql AS $$
  SELECT count(*)
  FROM pg_stat_all_tables s
  JOIN _timescaledb_catalog.chunk c
    ON c.table_name = s.relname AND c.schema_name = s.schemaname
  WHERE c.hypertable_id = (SELECT mat_hypertable_id
                           FROM _timescaledb_catalog.continuous_agg
                           WHERE user_view_name = view_name)
    AND s.last_analyze IS NOT NULL;
$$;

ANALYZE cagg_analyze_view;
SELECT cagg_analyze_count_analyzed('cagg_analyze_view') > 0 AS chunk_stats_collected;

-- VACUUM ANALYZE and plain VACUUM should also be accepted on the view.
VACUUM ANALYZE cagg_analyze_view;
VACUUM cagg_analyze_view;

-- Mixed list with a plain table, a hypertable and the cagg view.
CREATE TABLE cagg_analyze_plain(a int);
INSERT INTO cagg_analyze_plain SELECT generate_series(1, 100);
ANALYZE cagg_analyze_plain, cagg_analyze_src, cagg_analyze_view;
DROP TABLE cagg_analyze_plain;

-- Compressed cagg: the compression chunk should also be analyzed.
ALTER MATERIALIZED VIEW cagg_analyze_view SET (timescaledb.compress = true);
SELECT compress_chunk(ch) IS NOT NULL AS compressed
FROM show_chunks('cagg_analyze_view') ch
ORDER BY ch
LIMIT 1;

ANALYZE cagg_analyze_view;

SELECT count(*) > 0 AS compression_chunk_analyzed
FROM pg_stat_all_tables s
JOIN _timescaledb_catalog.chunk c
  ON c.table_name = s.relname AND c.schema_name = s.schemaname
JOIN _timescaledb_catalog.chunk parent
  ON parent.compressed_chunk_id = c.id
WHERE parent.hypertable_id = (SELECT mat_hypertable_id
                              FROM _timescaledb_catalog.continuous_agg
                              WHERE user_view_name = 'cagg_analyze_view')
  AND s.last_analyze IS NOT NULL;

DROP FUNCTION cagg_analyze_count_analyzed(name);
DROP MATERIALIZED VIEW cagg_analyze_view;
DROP TABLE cagg_analyze_src;
