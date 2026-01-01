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
