-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

-- hypertable creation
SHOW timescaledb.enable_hypertable_create;
SET timescaledb.enable_hypertable_create TO off;

CREATE TABLE test(time timestamptz, device int);
\set ON_ERROR_STOP 0
SELECT * FROM create_hypertable('test', 'time');
\set ON_ERROR_STOP 1

SET timescaledb.enable_hypertable_create TO on;
SELECT * FROM create_hypertable('test', 'time');

-- hypertable compression
SHOW timescaledb.enable_hypertable_compression;
SET timescaledb.enable_hypertable_compression TO off;

INSERT INTO test SELECT t, 0
FROM generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-04 1:00', '1 hour') t;
SELECT * FROM show_chunks('test');

-- compress_chunk
\set ON_ERROR_STOP 0
SELECT compress_chunk('_timescaledb_internal._hyper_1_1_chunk');
\set ON_ERROR_STOP 1

SET timescaledb.enable_hypertable_compression TO on;

-- ensure compression cannot be enabled
\set ON_ERROR_STOP 0
ALTER TABLE test
SET (timescaledb.compress,
     timescaledb.compress_orderby = 'time',
     timescaledb.compress_segmentby = 'device');
\set ON_ERROR_STOP 1

SET timescaledb.enable_hypertable_compression TO on;

ALTER TABLE test
SET (timescaledb.compress,
     timescaledb.compress_orderby = 'time',
     timescaledb.compress_segmentby = 'device');
SELECT compress_chunk('_timescaledb_internal._hyper_1_1_chunk');

SET timescaledb.enable_hypertable_compression TO off;

-- cannot alter compressed table
\set ON_ERROR_STOP 0
ALTER TABLE test ADD COLUMN col1 boolean DEFAULT false NOT NULL;
\set ON_ERROR_STOP 1

SET timescaledb.enable_hypertable_compression TO on;
ALTER TABLE test ADD COLUMN col1 boolean DEFAULT false NOT NULL;

SET timescaledb.enable_hypertable_compression TO off;
\set ON_ERROR_STOP 0
ALTER TABLE test DROP COLUMN col1;
\set ON_ERROR_STOP 1

-- cagg creation
SHOW timescaledb.enable_cagg_create;
SET timescaledb.enable_cagg_create TO off;

\set ON_ERROR_STOP 0

CREATE MATERIALIZED VIEW contagg
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 hour', time) AS hour,
  device
FROM
  test
GROUP BY hour, device;

\set ON_ERROR_STOP 1

SET timescaledb.enable_cagg_create TO on;

CREATE MATERIALIZED VIEW contagg
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 hour', time) AS hour,
  device
FROM
  test
GROUP BY hour, device;

SET timescaledb.enable_cagg_create TO off;

\set ON_ERROR_STOP 0
CALL refresh_continuous_aggregate('contagg', NULL, NULL);
\set ON_ERROR_STOP 1

SET timescaledb.enable_cagg_create TO on;

-- policy creation
SHOW timescaledb.enable_policy_create;
SET timescaledb.enable_policy_create TO off;

\set ON_ERROR_STOP 0

select add_retention_policy('test', INTERVAL '4 months', true);
select remove_retention_policy('test');

select add_compression_policy('test', compress_after => NULL);
SELECT remove_compression_policy('test');

SELECT add_continuous_aggregate_policy('contagg', '1 day'::interval, 10 , '1 h'::interval);
SELECT remove_continuous_aggregate_policy('contagg');

CREATE INDEX idx ON test(device);
SELECT add_reorder_policy('test', 'idx');
select remove_reorder_policy('test');

SELECT timescaledb_experimental.add_policies('test', refresh_start_offset => 1, refresh_end_offset => 10, compress_after => 11, drop_after => 20);
SELECT timescaledb_experimental.show_policies('test');
SELECT timescaledb_experimental.alter_policies('test',  refresh_start_offset => 11, compress_after=>13, drop_after => 25);
SELECT timescaledb_experimental.remove_all_policies('test');
SELECT timescaledb_experimental.remove_policies('test', false, 'policy_refresh_continuous_aggregate', 'policy_compression');

\set ON_ERROR_STOP 1
