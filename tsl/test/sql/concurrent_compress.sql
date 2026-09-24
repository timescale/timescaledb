-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE VIEW jobs_of AS
SELECT h.table_name, j.proc_name, j.schedule_interval, j.config
FROM _timescaledb_catalog.hypertable h
LEFT JOIN _timescaledb_config.bgw_job j ON j.hypertable_id = h.id;

CREATE VIEW status_of AS
SELECT table_name, _timescaledb_functions.hypertable_status_text(status) AS status
FROM _timescaledb_catalog.hypertable;

-- Test 1 : ALTER TABLE creates both policies and sets the status bit
CREATE TABLE metrics(time timestamptz NOT NULL, device int, value float8);
SELECT table_name FROM create_hypertable('metrics', 'time',
    chunk_time_interval => interval '1 day');
ALTER TABLE metrics SET (timescaledb.compress, timescaledb.compress_segmentby='device',
                         timescaledb.concurrent_compress);

SELECT proc_name, schedule_interval, config FROM jobs_of WHERE table_name = 'metrics'
  ORDER BY proc_name;
SELECT status FROM status_of WHERE table_name = 'metrics';

-- Test 2 : No columnstore policy is created, since the move policy replaces it
SELECT count(*) AS columnstore_policies FROM jobs_of
  WHERE table_name = 'metrics' AND proc_name = 'policy_compression';

-- Test 3 : The schedule interval option applies to both jobs
CREATE TABLE m30(time timestamptz NOT NULL, device int, value float8);
SELECT table_name FROM create_hypertable('m30', 'time',
    chunk_time_interval => interval '1 day');
ALTER TABLE m30 SET (timescaledb.compress, timescaledb.compress_segmentby='device',
                     timescaledb.concurrent_compress,
                     timescaledb.concurrent_compress_schedule_interval='30 seconds');
SELECT proc_name, schedule_interval FROM jobs_of WHERE table_name = 'm30' ORDER BY proc_name;

-- Test 4 : Re-running converges the schedule rather than duplicating the jobs
ALTER TABLE m30 SET (timescaledb.concurrent_compress,
                     timescaledb.concurrent_compress_schedule_interval='10 seconds');
SELECT proc_name, schedule_interval FROM jobs_of WHERE table_name = 'm30' ORDER BY proc_name;
DROP TABLE m30;

-- Test 5 : The schedule interval requires its parent option
CREATE TABLE si(time timestamptz NOT NULL, device int);
SELECT table_name FROM create_hypertable('si', 'time',
    chunk_time_interval => interval '1 day');
\set ON_ERROR_STOP 0
ALTER TABLE si SET (timescaledb.compress,
                    timescaledb.concurrent_compress_schedule_interval='30 seconds');
\set ON_ERROR_STOP 1
DROP TABLE si;

-- Test 6 : The option alone turns on columnstore
CREATE TABLE implied(time timestamptz NOT NULL, device int);
SELECT table_name FROM create_hypertable('implied', 'time',
    chunk_time_interval => interval '1 day');
ALTER TABLE implied SET (timescaledb.concurrent_compress);
SELECT status FROM status_of WHERE table_name = 'implied';
DROP TABLE implied;

-- Test 7 : Disabling removes both jobs and clears the bit
ALTER TABLE metrics SET (timescaledb.concurrent_compress = false);
SELECT count(*) AS jobs FROM jobs_of WHERE table_name = 'metrics' AND proc_name IS NOT NULL;
SELECT status FROM status_of WHERE table_name = 'metrics';

-- Test 8 : Disabling again is a clean no-op
ALTER TABLE metrics SET (timescaledb.concurrent_compress = false);
SELECT count(*) AS jobs FROM jobs_of WHERE table_name = 'metrics' AND proc_name IS NOT NULL;

-- Test 9 : Disabling columnstore cascades to the policies
ALTER TABLE metrics SET (timescaledb.concurrent_compress);
SELECT count(*) AS jobs_before FROM jobs_of WHERE table_name = 'metrics' AND proc_name IS NOT NULL;
ALTER TABLE metrics SET (timescaledb.compress = false);
SELECT count(*) AS jobs_after FROM jobs_of WHERE table_name = 'metrics' AND proc_name IS NOT NULL;
SELECT status FROM status_of WHERE table_name = 'metrics';
DROP TABLE metrics;

-- Test 10 : CREATE TABLE WITH accepts the same options
CREATE TABLE created(time timestamptz NOT NULL, device int, value float8)
  WITH (tsdb.hypertable, tsdb.partition_column='time', tsdb.chunk_interval='1 day',
        tsdb.segmentby='device', tsdb.concurrent_compress,
        tsdb.concurrent_compress_schedule_interval='15 seconds');
SELECT proc_name, schedule_interval FROM jobs_of WHERE table_name = 'created' ORDER BY proc_name;
DROP TABLE created;

-- Test 11 : Enabling on a table that already has a columnstore policy
CREATE TABLE replaced(time timestamptz NOT NULL, device int, value float8)
  WITH (tsdb.hypertable, tsdb.partition_column='time', tsdb.chunk_interval='1 day',
        tsdb.segmentby='device');
SELECT proc_name FROM jobs_of WHERE table_name = 'replaced' ORDER BY proc_name;
ALTER TABLE replaced SET (timescaledb.concurrent_compress);
SELECT proc_name FROM jobs_of WHERE table_name = 'replaced' ORDER BY proc_name;
DROP TABLE replaced;

-- Test 12 : Cannot be combined with direct compress, in either order
CREATE TABLE both1(time timestamptz NOT NULL, device int);
SELECT table_name FROM create_hypertable('both1', 'time',
    chunk_time_interval => interval '1 day');
\set ON_ERROR_STOP 0
ALTER TABLE both1 SET (timescaledb.compress, timescaledb.direct_compress,
                       timescaledb.concurrent_compress);
\set ON_ERROR_STOP 1

ALTER TABLE both1 SET (timescaledb.compress, timescaledb.concurrent_compress);
\set ON_ERROR_STOP 0
ALTER TABLE both1 SET (timescaledb.direct_compress);
\set ON_ERROR_STOP 1

ALTER TABLE both1 SET (timescaledb.concurrent_compress = false);
ALTER TABLE both1 SET (timescaledb.direct_compress);
\set ON_ERROR_STOP 0
ALTER TABLE both1 SET (timescaledb.concurrent_compress);
\set ON_ERROR_STOP 1
DROP TABLE both1;

-- Test 13 : The option is not available on continuous aggregates
CREATE TABLE src(time timestamptz NOT NULL, device int, value float8);
SELECT table_name FROM create_hypertable('src', 'time',
    chunk_time_interval => interval '1 day');
ALTER TABLE src SET (timescaledb.compress, timescaledb.compress_segmentby='device');
CREATE MATERIALIZED VIEW cagg WITH (timescaledb.continuous) AS
SELECT time_bucket(INTERVAL '1 hour', time) AS bucket, device, max(value)
FROM src GROUP BY bucket, device WITH NO DATA;

\set ON_ERROR_STOP 0
ALTER MATERIALIZED VIEW cagg SET (timescaledb.concurrent_compress = true);
\set ON_ERROR_STOP 1

-- but the raw hypertable behind a cagg is fine
ALTER TABLE src SET (timescaledb.concurrent_compress);
SELECT proc_name FROM jobs_of WHERE table_name = 'src' ORDER BY proc_name;
DROP MATERIALIZED VIEW cagg;
DROP TABLE src;

DROP VIEW status_of;
DROP VIEW jobs_of;
