-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Tests for the timescaledb.direct_compress option, which enables direct
-- compress on a hypertable and creates the compaction and compression
-- policies that back it as a single command. Available both as an
-- ALTER TABLE ... SET (...) option and as a CREATE TABLE ... WITH (...) option.

----------------------------------------------------------------------
-- ALTER TABLE: enabling direct_compress also enables columnstore and
-- creates both policies
----------------------------------------------------------------------

-- create_hypertable() doesn't enable columnstore by default, so this table
-- starts out with no compression and no policies.
CREATE TABLE metrics (time TIMESTAMPTZ NOT NULL, device TEXT, value float);
SELECT create_hypertable('metrics', 'time');

ALTER TABLE metrics SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'device',
  timescaledb.direct_compress
);

SELECT status FROM _timescaledb_catalog.hypertable WHERE table_name = 'metrics';

SELECT proc_name, schedule_interval, config
FROM _timescaledb_config.bgw_job WHERE hypertable_id = (SELECT id FROM _timescaledb_catalog.hypertable WHERE table_name = 'metrics')
ORDER BY proc_name;

----------------------------------------------------------------------
-- ALTER TABLE: direct_compress_schedule_interval is honored by both policies
----------------------------------------------------------------------

CREATE TABLE metrics2 (time TIMESTAMPTZ NOT NULL, device TEXT, value float);
SELECT create_hypertable('metrics2', 'time');

ALTER TABLE metrics2 SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'device',
  timescaledb.direct_compress,
  timescaledb.direct_compress_schedule_interval = '30 seconds'
);

SELECT proc_name, schedule_interval
FROM _timescaledb_config.bgw_job WHERE hypertable_id = (SELECT id FROM _timescaledb_catalog.hypertable WHERE table_name = 'metrics2')
ORDER BY proc_name;

----------------------------------------------------------------------
-- ALTER TABLE: re-running is idempotent and converges the existing
-- policies to the new schedule
----------------------------------------------------------------------

ALTER TABLE metrics SET (
  timescaledb.direct_compress,
  timescaledb.direct_compress_schedule_interval = '10 seconds'
);

SELECT count(*) FROM _timescaledb_config.bgw_job WHERE hypertable_id = (SELECT id FROM _timescaledb_catalog.hypertable WHERE table_name = 'metrics');

SELECT proc_name, schedule_interval, config
FROM _timescaledb_config.bgw_job WHERE hypertable_id = (SELECT id FROM _timescaledb_catalog.hypertable WHERE table_name = 'metrics')
ORDER BY proc_name;

----------------------------------------------------------------------
-- ALTER TABLE: direct_compress alone (no timescaledb.compress) auto-enables
-- columnstore too
----------------------------------------------------------------------

CREATE TABLE metrics3 (time TIMESTAMPTZ NOT NULL, device TEXT, value float);
SELECT create_hypertable('metrics3', 'time');
ALTER TABLE metrics3 SET (timescaledb.direct_compress);
SELECT status FROM _timescaledb_catalog.hypertable WHERE table_name = 'metrics3';
SELECT count(*) FROM _timescaledb_config.bgw_job WHERE hypertable_id = (SELECT id FROM _timescaledb_catalog.hypertable WHERE table_name = 'metrics3');

----------------------------------------------------------------------
-- ALTER TABLE: disabling direct_compress clears the status bit and
-- removes the policies it created
----------------------------------------------------------------------

ALTER TABLE metrics SET (timescaledb.direct_compress = false);
SELECT status FROM _timescaledb_catalog.hypertable WHERE table_name = 'metrics';
SELECT count(*) FROM _timescaledb_config.bgw_job WHERE hypertable_id = (SELECT id FROM _timescaledb_catalog.hypertable WHERE table_name = 'metrics');

----------------------------------------------------------------------
-- ALTER TABLE: disabling columnstore also clears direct_compress and
-- removes the policies
----------------------------------------------------------------------

ALTER TABLE metrics2 SET (timescaledb.direct_compress);
SELECT status FROM _timescaledb_catalog.hypertable WHERE table_name = 'metrics2';
ALTER TABLE metrics2 SET (timescaledb.compress = false);
SELECT status FROM _timescaledb_catalog.hypertable WHERE table_name = 'metrics2';
SELECT count(*) FROM _timescaledb_config.bgw_job WHERE hypertable_id = (SELECT id FROM _timescaledb_catalog.hypertable WHERE table_name = 'metrics2');

----------------------------------------------------------------------
-- ALTER TABLE: per-table flag turns on direct compress even with the
-- instance GUC off
----------------------------------------------------------------------

SET timescaledb.enable_direct_compress_insert = false;

CREATE TABLE metrics4 (time TIMESTAMPTZ NOT NULL, device TEXT, value float);
SELECT create_hypertable('metrics4', 'time');
ALTER TABLE metrics4 SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'device',
  timescaledb.direct_compress
);

BEGIN;
INSERT INTO metrics4 SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd1', i::float FROM generate_series(0,3000) i;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM metrics4;
ROLLBACK;

-- without direct_compress set, the GUC being off means no direct compress
CREATE TABLE metrics5 (time TIMESTAMPTZ NOT NULL, device TEXT, value float);
SELECT create_hypertable('metrics5', 'time');
ALTER TABLE metrics5 SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'device'
);

BEGIN;
INSERT INTO metrics5 SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd1', i::float FROM generate_series(0,3000) i;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM metrics5;
ROLLBACK;

RESET timescaledb.enable_direct_compress_insert;

----------------------------------------------------------------------
-- CREATE TABLE ... WITH (tsdb.hypertable, tsdb.direct_compress, ...)
----------------------------------------------------------------------

CREATE TABLE metrics6 (time TIMESTAMPTZ NOT NULL, device TEXT, value float)
  WITH (tsdb.hypertable, tsdb.orderby='time', tsdb.segmentby='device', tsdb.direct_compress);

SELECT status FROM _timescaledb_catalog.hypertable WHERE table_name = 'metrics6';

SELECT proc_name, schedule_interval, config
FROM _timescaledb_config.bgw_job WHERE hypertable_id = (SELECT id FROM _timescaledb_catalog.hypertable WHERE table_name = 'metrics6')
ORDER BY proc_name;

-- direct_compress_schedule_interval is honored at create time too
CREATE TABLE metrics7 (time TIMESTAMPTZ NOT NULL, device TEXT, value float)
  WITH (
    tsdb.hypertable, tsdb.orderby='time', tsdb.segmentby='device',
    tsdb.direct_compress, tsdb.direct_compress_schedule_interval='15 seconds'
  );

SELECT proc_name, schedule_interval
FROM _timescaledb_config.bgw_job WHERE hypertable_id = (SELECT id FROM _timescaledb_catalog.hypertable WHERE table_name = 'metrics7')
ORDER BY proc_name;

-- tsdb.hypertable without direct_compress doesn't set the status bit, but
-- still gets the regular default compression policy (1 chunk interval,
-- scheduled daily) since plain columnstore always adds one
CREATE TABLE metrics8 (time TIMESTAMPTZ NOT NULL, device TEXT, value float)
  WITH (tsdb.hypertable, tsdb.orderby='time', tsdb.segmentby='device', tsdb.chunk_interval='1 day');
SELECT status FROM _timescaledb_catalog.hypertable WHERE table_name = 'metrics8';
SELECT proc_name, schedule_interval, config
FROM _timescaledb_config.bgw_job WHERE hypertable_id = (SELECT id FROM _timescaledb_catalog.hypertable WHERE table_name = 'metrics8')
ORDER BY proc_name;

----------------------------------------------------------------------
-- disabling direct_compress and then just re-enabling plain compression
-- (no direct_compress) leaves no policy: unlike CREATE TABLE WITH, plain
-- ALTER TABLE compress on its own never adds a default compression policy
----------------------------------------------------------------------

CREATE TABLE metrics9 (time TIMESTAMPTZ NOT NULL, device TEXT, value float);
SELECT create_hypertable('metrics9', 'time');
ALTER TABLE metrics9 SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'device',
  timescaledb.direct_compress
);
SELECT count(*) FROM _timescaledb_config.bgw_job WHERE hypertable_id = (SELECT id FROM _timescaledb_catalog.hypertable WHERE table_name = 'metrics9');

ALTER TABLE metrics9 SET (timescaledb.direct_compress = false);
SELECT status FROM _timescaledb_catalog.hypertable WHERE table_name = 'metrics9';
SELECT count(*) FROM _timescaledb_config.bgw_job WHERE hypertable_id = (SELECT id FROM _timescaledb_catalog.hypertable WHERE table_name = 'metrics9');
