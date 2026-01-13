-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Test declarative partitioning for hypertables


-- Enable declarative partitioning for all subsequent tests
SET timescaledb.enable_partitioned_hypertables = true;

-- Basic hypertable creation with TIMESTAMPTZ
CREATE TABLE metrics(
    time TIMESTAMP WITH TIME ZONE,
    device TEXT,
    value FLOAT
) WITH (timescaledb.hypertable, timescaledb.partition_column='time');

-- Create with TIMESTAMP
CREATE TABLE metrics_ts(
    time TIMESTAMP NOT NULL,
    device TEXT,
    value FLOAT
) WITH (timescaledb.hypertable, timescaledb.partition_column='time');

-- Create with DATE
CREATE TABLE metrics_date(
    time DATE NOT NULL,
    device TEXT,
    value FLOAT
) WITH (timescaledb.hypertable, timescaledb.partition_column='time');

-- Create with int
CREATE TABLE metrics_int(
    time INT NOT NULL,
    device TEXT,
    value FLOAT
) WITH (timescaledb.hypertable, timescaledb.partition_column='time');

-- Create with custom chunk_time_interval
CREATE TABLE metrics_custom_interval(
    time TIMESTAMPTZ NOT NULL,
    device TEXT,
    value FLOAT
) WITH (timescaledb.hypertable, timescaledb.partition_column='time', timescaledb.chunk_interval='30 days');

-- Verify hypertables are actually created and partitioned
SELECT hypertable_name FROM timescaledb_information.hypertables
WHERE hypertable_name IN ('metrics', 'metrics_ts', 'metrics_date', 'metrics_int', 'metrics_custom_interval')
ORDER BY hypertable_name;

SELECT DISTINCT(relkind) = 'p' FROM pg_class
WHERE relname IN ('metrics', 'metrics_ts', 'metrics_date', 'metrics_int', 'metrics_custom_interval');

\set ON_ERROR_STOP 0
-- Try to create with invalid partition column type
CREATE TABLE invalid(time TEXT, value FLOAT) WITH (timescaledb.hypertable, timescaledb.partition_column='time');
CREATE TABLE invalid(time TEXT, value FLOAT) WITH (timescaledb.hypertable);
\set ON_ERROR_STOP 1

DROP TABLE IF EXISTS metrics_ts;
DROP TABLE IF EXISTS metrics_date;
DROP TABLE IF EXISTS metrics_int;
DROP TABLE IF EXISTS metrics_custom_interval;
DROP TABLE IF EXISTS invalid;

-- Test PARTITION BY syntax
CREATE TABLE metrics_partition_by(
    time TIMESTAMPTZ NOT NULL,
    device TEXT,
    value FLOAT
) PARTITION BY RANGE (time) WITH (timescaledb.hypertable);

\set ON_ERROR_STOP 0
CREATE TABLE part_col_specified(time TIMESTAMPTZ, device TEXT) PARTITION BY RANGE (time) WITH (timescaledb.hypertable, timescaledb.partition_column='time');
CREATE TABLE multiple_part_key(time TIMESTAMPTZ, time2 TIMESTAMP, device TEXT) PARTITION BY RANGE (time, time2) WITH (timescaledb.hypertable, timescaledb.partition_column='time');
CREATE TABLE bad_strategy(time TIMESTAMPTZ, device TEXT) PARTITION BY LIST (time) WITH (timescaledb.hypertable);
\set ON_ERROR_STOP 1
DROP TABLE IF EXISTS metrics_partition_by;

-- Insert Operations and Chunk Creation
INSERT INTO metrics VALUES
  ('2025-01-15 00:00:00+00', 'device1', 11.0),
  ('2025-02-15 00:00:00+00', 'device2', 12.0),
  ('2025-03-15 00:00:00+00', 'device3', 13.0);
SELECT count(*) FROM show_chunks('metrics');
SELECT count(*) FROM metrics;

SELECT * FROM metrics ORDER BY time;

-- Verify chunk was created and attached as partition
SELECT count(*) FROM show_chunks('metrics');

SELECT child.relname AS chunk, parent.relname AS hypertable
FROM pg_inherits
JOIN pg_class child ON inhrelid = child.oid
JOIN pg_class parent ON inhparent = parent.oid
WHERE parent.relname = 'metrics'
LIMIT 1;

-- Insert with CHECK constraint
ALTER TABLE metrics ADD CONSTRAINT valcheck CHECK (value >= 0);

-- Try inserting into existing and new chunk to violate CHECK constraint
\set ON_ERROR_STOP 0
INSERT INTO metrics VALUES ('2025-03-15 00:00:00+00', 'device1', -10.0);
INSERT INTO metrics VALUES ('2025-04-15 00:00:00+00', 'device1', -10.0);
\set ON_ERROR_STOP 1


-- SELECT with WHERE on time (partition pruning)
EXPLAIN (COSTS OFF)
SELECT * FROM metrics WHERE time >= '2025-01-01' AND time < '2025-02-01';


-- FOREIGN KEY from hypertable to regular table
CREATE TABLE ref_table(id INT PRIMARY KEY, name TEXT);
INSERT INTO ref_table VALUES (1, 'ref1'), (2, 'ref2');

CREATE TABLE fk_table(
  time TIMESTAMPTZ NOT NULL,
  ref_id INT REFERENCES ref_table(id),
  value FLOAT
) WITH (timescaledb.hypertable, timescaledb.partition_column='time');
INSERT INTO fk_table VALUES ('2025-11-01', 1, 10.0);

\set ON_ERROR_STOP 0
INSERT INTO fk_table VALUES ('2025-11-01', 999, 20.0);
\set ON_ERROR_STOP 1

DROP TABLE ref_table CASCADE;
DROP TABLE fk_table CASCADE;

-- FOREIGN KEY from hypertable to hypertable
CREATE TABLE ref_ht(
    time TIMESTAMPTZ NOT NULL ,
    id INT,
    CONSTRAINT ref_ht_pkey PRIMARY KEY (time, id)
) WITH (timescaledb.hypertable, timescaledb.partition_column='time');
INSERT INTO ref_ht VALUES
  ('2025-06-15 00:00:00+00', 1),
  ('2025-07-15 00:00:00+00', 2);

CREATE TABLE fk_ht(
    time TIMESTAMPTZ NOT NULL,
    ref_time TIMESTAMPTZ,
    ref_id INT,
    value FLOAT,
    FOREIGN KEY (ref_time, ref_id) REFERENCES ref_ht(time, id)
) WITH (timescaledb.hypertable, timescaledb.partition_column='time');
INSERT INTO fk_ht VALUES
    ('2025-08-15 00:00:00+00', '2025-06-15 00:00:00+00', 1, 31.0);
\set ON_ERROR_STOP 0
INSERT INTO fk_ht VALUES
    ('2025-08-15 00:00:00+00', '2025-01-01 00:00:00+00', 999, 32.0);
\set ON_ERROR_STOP 1

DROP TABLE ref_ht CASCADE;
DROP TABLE fk_ht CASCADE;

-- Test if foreign keys to hypertables not using declarative partitioning are still disallowed
SET timescaledb.enable_partitioned_hypertables = false;
CREATE TABLE ref_ht(
    time TIMESTAMPTZ NOT NULL ,
    id INT,
    CONSTRAINT ref_ht_pkey PRIMARY KEY (time, id)
) WITH (timescaledb.hypertable, timescaledb.partition_column='time');
INSERT INTO ref_ht VALUES
  ('2025-06-15 00:00:00+00', 1),
  ('2025-07-15 00:00:00+00', 2);
SET timescaledb.enable_partitioned_hypertables = true;

\set ON_ERROR_STOP 0
CREATE TABLE fk_ht(
    time TIMESTAMPTZ NOT NULL,
    ref_time TIMESTAMPTZ,
    ref_id INT,
    value FLOAT,
    FOREIGN KEY (ref_time, ref_id) REFERENCES ref_ht(time, id)
) WITH (timescaledb.hypertable, timescaledb.partition_column='time');
\set ON_ERROR_STOP 1

DROP TABLE ref_ht CASCADE;
DROP TABLE IF EXISTS fk_ht CASCADE;

-- Test partition wise joins
CREATE TABLE metrics_pwj(
    time TIMESTAMPTZ NOT NULL,
    device TEXT,
    value FLOAT
) WITH (timescaledb.hypertable, timescaledb.partition_column='time');
INSERT INTO metrics_pwj VALUES
  ('2025-01-15 00:00:00+00', 'device1', 11.0),
  ('2025-02-15 00:00:00+00', 'device2', 12.0),
  ('2025-03-15 00:00:00+00', 'device3', 13.0);

SET enable_partitionwise_join = true;
EXPLAIN (COSTS OFF)
SELECT m1.device, m2.device
FROM metrics AS m1
JOIN metrics_pwj AS m2 ON m1.time = m2.time;
SET enable_partitionwise_join = false;

-- Transaction - ROLLBACK
BEGIN;
INSERT INTO metrics VALUES ('2024-12-20', 'rollback_test', 42.0);
SELECT count(*)=1 FROM metrics WHERE device = 'rollback_test';
ROLLBACK;
SELECT count(*)=0 FROM metrics WHERE device = 'rollback_test';

-- Reset GUC
SET timescaledb.enable_partitioned_hypertables = false;

-- Cleanup
DROP TABLE IF EXISTS metrics CASCADE;
DROP TABLE IF EXISTS metrics_pwj CASCADE;
