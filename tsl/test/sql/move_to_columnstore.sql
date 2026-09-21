-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set TEST_BASE_NAME move_to_columnstore
SELECT format('include/%s_query.sql', :'TEST_BASE_NAME') as "TEST_QUERY_NAME",
       format('%s/results/%s_before.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_BEFORE",
       format('%s/results/%s_after.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_AFTER"
\gset
SELECT format('\! diff -u --label "Before move" --label "After move" %s %s',
              :'TEST_RESULTS_BEFORE', :'TEST_RESULTS_AFTER') as "DIFF_CMD"
\gset

CREATE VIEW chunk_status AS
SELECT ch.relid::regclass::text AS chunk,
       _timescaledb_functions.chunk_status_text(ch.status) AS status
FROM _timescaledb_catalog.chunk ch;

-- Test 1 : Move a partial chunk
CREATE TABLE metrics(time timestamptz NOT NULL, device int, value float8);
SELECT table_name FROM create_hypertable('metrics', 'time',
    chunk_time_interval => interval '100 years');
ALTER TABLE metrics SET (timescaledb.compress, timescaledb.compress_segmentby='device');

INSERT INTO metrics SELECT '2026-01-01'::timestamptz + (i || 's')::interval, i % 5, i
  FROM generate_series(1, 1000) i;
SELECT count(compress_chunk(c)) FROM show_chunks('metrics') c;

INSERT INTO metrics SELECT '2026-01-01'::timestamptz + (i || 's')::interval, i % 5, i
  FROM generate_series(1001, 1200) i;

SELECT status FROM chunk_status;
SELECT count(*) AS uncompressed_rows FROM ONLY _timescaledb_internal._hyper_1_1_chunk;

\set ECHO errors
\o :TEST_RESULTS_BEFORE
\ir :TEST_QUERY_NAME
\o
\set ECHO all
SELECT _timescaledb_functions.move_to_columnstore(c) FROM show_chunks('metrics') c;
\set ECHO errors
\o :TEST_RESULTS_AFTER
\ir :TEST_QUERY_NAME
\o
:DIFF_CMD
\set ECHO all

SELECT status FROM chunk_status;
SELECT count(*) AS uncompressed_rows FROM ONLY _timescaledb_internal._hyper_1_1_chunk;

-- Test 2 : Move a chunk with nothing left to move
SELECT _timescaledb_functions.move_to_columnstore(c) FROM show_chunks('metrics') c;

-- Test 3 : Leave an already unordered chunk unordered
INSERT INTO metrics SELECT '2026-01-01'::timestamptz + (i || 's')::interval, i % 5, i
  FROM generate_series(1201, 1300) i;
SELECT status FROM chunk_status;
SELECT _timescaledb_functions.move_to_columnstore(c) FROM show_chunks('metrics') c;
SELECT status FROM chunk_status;
SELECT count(*) AS total_rows FROM metrics;

DROP TABLE metrics;

-- Test 4 : Move a chunk that was never compressed
CREATE TABLE fresh(time timestamptz NOT NULL, device int, value float8);
SELECT table_name FROM create_hypertable('fresh', 'time',
    chunk_time_interval => interval '100 years');
ALTER TABLE fresh SET (timescaledb.compress, timescaledb.compress_segmentby='device');
INSERT INTO fresh SELECT '2026-01-01'::timestamptz + (i || 's')::interval, i % 5, i
  FROM generate_series(1, 500) i;

SELECT status FROM chunk_status;
SELECT _timescaledb_functions.move_to_columnstore(c) FROM show_chunks('fresh') c;
SELECT status FROM chunk_status;
SELECT count(*) AS total_rows FROM fresh;

DROP TABLE fresh;

-- Test 5 : Flush more than once via the sort limit
CREATE TABLE metrics(time timestamptz NOT NULL, device int, value float8);
SELECT table_name FROM create_hypertable('metrics', 'time',
    chunk_time_interval => interval '100 years');
ALTER TABLE metrics SET (timescaledb.compress, timescaledb.compress_segmentby='device');
INSERT INTO metrics SELECT '2026-01-01'::timestamptz + (i || 's')::interval, i % 5, i
  FROM generate_series(1, 1000) i;

SET timescaledb.move_to_columnstore_tuple_sort_limit = 100;
\set ECHO errors
\o :TEST_RESULTS_BEFORE
\ir :TEST_QUERY_NAME
\o
\set ECHO all
SELECT _timescaledb_functions.move_to_columnstore(c) FROM show_chunks('metrics') c;
\set ECHO errors
\o :TEST_RESULTS_AFTER
\ir :TEST_QUERY_NAME
\o
:DIFF_CMD
\set ECHO all
RESET timescaledb.move_to_columnstore_tuple_sort_limit;

SELECT status FROM chunk_status;
DROP TABLE metrics;

-- Test 6 : Refuse chunks with unique constraints or triggers
CREATE TABLE uniq(time timestamptz NOT NULL, device int);
SELECT table_name FROM create_hypertable('uniq', 'time',
    chunk_time_interval => interval '100 years');
CREATE UNIQUE INDEX ON uniq(device, time);
ALTER TABLE uniq SET (timescaledb.compress, timescaledb.compress_segmentby='device');
INSERT INTO uniq SELECT '2026-01-01'::timestamptz + (i || 's')::interval, i
  FROM generate_series(1, 10) i;
SELECT _timescaledb_functions.move_to_columnstore(c) FROM show_chunks('uniq') c;

CREATE TABLE trig(time timestamptz NOT NULL, device int);
SELECT table_name FROM create_hypertable('trig', 'time',
    chunk_time_interval => interval '100 years');
ALTER TABLE trig SET (timescaledb.compress, timescaledb.compress_segmentby='device');
INSERT INTO trig SELECT '2026-01-01'::timestamptz + (i || 's')::interval, i
  FROM generate_series(1, 10) i;
CREATE FUNCTION noop_trigger() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN RETURN NEW; END $$;
CREATE TRIGGER noop AFTER INSERT ON trig FOR EACH ROW EXECUTE FUNCTION noop_trigger();
SELECT _timescaledb_functions.move_to_columnstore(c) FROM show_chunks('trig') c;

-- Test 7 : Fall back to compress_chunk when the move is refused
SELECT _timescaledb_functions.move_to_columnstore(c, true) FROM show_chunks('uniq') c;
SELECT status FROM chunk_status WHERE chunk LIKE '%_hyper%' AND chunk LIKE '%_4_%';
SELECT count(*) AS total_rows FROM uniq;

DROP TABLE trig;
DROP FUNCTION noop_trigger();
DROP TABLE uniq;

-- Test 8 : Skip a frozen chunk and reject a NULL argument
CREATE TABLE frz(time timestamptz NOT NULL, device int);
SELECT table_name FROM create_hypertable('frz', 'time',
    chunk_time_interval => interval '100 years');
ALTER TABLE frz SET (timescaledb.compress, timescaledb.compress_segmentby='device');
INSERT INTO frz SELECT '2026-01-01'::timestamptz + (i || 's')::interval, i
  FROM generate_series(1, 10) i;
SELECT _timescaledb_functions.freeze_chunk(c) FROM show_chunks('frz') c;

-- frozen returns before the fallback branch, so no error even with fallback => false
SELECT _timescaledb_functions.move_to_columnstore(c) FROM show_chunks('frz') c;

SELECT _timescaledb_functions.unfreeze_chunk(c) FROM show_chunks('frz') c;
DROP TABLE frz;

-- the function is not STRICT, so NULL reaches the invalid-OID check
\set ON_ERROR_STOP 0
SELECT _timescaledb_functions.move_to_columnstore(NULL);
\set ON_ERROR_STOP 1

-- Test 9 : Reject a hypertable without columnstore enabled
CREATE TABLE plain(time timestamptz NOT NULL, device int);
SELECT table_name FROM create_hypertable('plain', 'time',
    chunk_time_interval => interval '100 years');
INSERT INTO plain SELECT '2026-01-01'::timestamptz + (i || 's')::interval, i
  FROM generate_series(1, 10) i;

-- the check runs before the fallback branch, so fallback => true errors too
\set ON_ERROR_STOP 0
SELECT _timescaledb_functions.move_to_columnstore(c) FROM show_chunks('plain') c;
\set ON_ERROR_STOP 1

DROP TABLE plain;

-- Test 10 : Move a chunk of a hypertable with continuous aggregates
CREATE TABLE cagg_src(time timestamptz NOT NULL, device int, value float8);
SELECT table_name FROM create_hypertable('cagg_src', 'time',
    chunk_time_interval => interval '100 years');
ALTER TABLE cagg_src SET (timescaledb.compress, timescaledb.compress_segmentby='device');
INSERT INTO cagg_src SELECT '2026-01-01'::timestamptz + (i || 's')::interval, i % 3, i
  FROM generate_series(1, 10) i;
CREATE MATERIALIZED VIEW cagg_hourly WITH (timescaledb.continuous) AS
SELECT time_bucket(INTERVAL '1 hour', time) AS bucket, device, max(value)
FROM cagg_src GROUP BY bucket, device WITH NO DATA;
CALL refresh_continuous_aggregate('cagg_hourly', '2026-01-01', '2026-01-02');

SELECT _timescaledb_functions.move_to_columnstore(c) FROM show_chunks('cagg_src') c;
SELECT _timescaledb_functions.chunk_status_text(c) AS status FROM show_chunks('cagg_src') c;
SELECT count(*) AS total_rows FROM cagg_src;
-- the cagg still reads correctly over the moved rows
SELECT count(*) AS cagg_rows FROM cagg_hourly;

DROP MATERIALIZED VIEW cagg_hourly;
DROP TABLE cagg_src;

-- Test 11 : Move a chunk of a hypertable with no segmentby
CREATE TABLE nosegment(time timestamptz NOT NULL, device int, value float8);
SELECT table_name FROM create_hypertable('nosegment', 'time',
    chunk_time_interval => interval '100 years');
ALTER TABLE nosegment SET (timescaledb.compress);
INSERT INTO nosegment SELECT '2026-01-01'::timestamptz + (i || 's')::interval, i % 5, i
  FROM generate_series(1, 1000) i;

SELECT _timescaledb_functions.chunk_status_text(c) AS status FROM show_chunks('nosegment') c;
SELECT _timescaledb_functions.move_to_columnstore(c) FROM show_chunks('nosegment') c;
SELECT _timescaledb_functions.chunk_status_text(c) AS status FROM show_chunks('nosegment') c;
SELECT count(*) AS total_rows, min(value) AS min_v, max(value) AS max_v FROM nosegment;

DROP TABLE nosegment;

DROP VIEW chunk_status;
