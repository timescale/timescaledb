-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SET client_min_messages TO ERROR;
GRANT CREATE ON SCHEMA public TO :ROLE_DEFAULT_PERM_USER;
SET ROLE :ROLE_DEFAULT_PERM_USER;

-- this is a noop in our regression tests but allows us to use this
-- dataset more easily in other tests like the sqlsmith test
SELECT current_database() AS "TEST_DBNAME" \gset

CREATE SCHEMA test;

-- create normal hypertable with dropped columns, each chunk will have different attribute numbers
CREATE TABLE metrics(filler_1 int, filler_2 int, filler_3 int, time timestamptz NOT NULL, device_id int, v0 int, v1 int, v2 float, v3 float);
SELECT create_hypertable('metrics','time',create_default_indexes:=false);

ALTER TABLE metrics DROP COLUMN filler_1;
INSERT INTO metrics(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id+1,  device_id + 2, device_id + 0.5, NULL FROM generate_series('2000-01-01 0:00:00+0'::timestamptz,'2000-01-05 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);
ALTER TABLE metrics DROP COLUMN filler_2;
INSERT INTO metrics(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id-1, device_id + 2, device_id + 0.5, NULL FROM generate_series('2000-01-06 0:00:00+0'::timestamptz,'2000-01-12 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);
ALTER TABLE metrics DROP COLUMN filler_3;
INSERT INTO metrics(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id, device_id + 2, device_id + 0.5, NULL FROM generate_series('2000-01-13 0:00:00+0'::timestamptz,'2000-01-19 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);
CREATE INDEX ON metrics(time DESC);
CREATE INDEX ON metrics(device_id,time DESC);
ANALYZE metrics;

-- create identical hypertable with space partitioning
CREATE TABLE metrics_space(filler_1 int, filler_2 int, filler_3 int, time timestamptz NOT NULL, device_id int, v0 int, v1 int, v2 float, v3 float);
SELECT create_hypertable('metrics_space','time','device_id',3,create_default_indexes:=false);

ALTER TABLE metrics_space DROP COLUMN filler_1;
INSERT INTO metrics_space(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id+1, device_id + 2, device_id + 0.5, NULL FROM generate_series('2000-01-01 0:00:00+0'::timestamptz,'2000-01-05 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);
ALTER TABLE metrics_space DROP COLUMN filler_2;
INSERT INTO metrics_space(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id+1, device_id + 2, device_id + 0.5, NULL FROM generate_series('2000-01-06 0:00:00+0'::timestamptz,'2000-01-12 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);
ALTER TABLE metrics_space DROP COLUMN filler_3;
INSERT INTO metrics_space(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id+1, device_id + 2, device_id + 0.5, NULL FROM generate_series('2000-01-13 0:00:00+0'::timestamptz,'2000-01-19 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);
CREATE INDEX ON metrics_space(time);
CREATE INDEX ON metrics_space(device_id,time);
ANALYZE metrics_space;


-- create hypertable with compression
CREATE TABLE metrics_compressed(filler_1 int, filler_2 int, filler_3 int, time timestamptz NOT NULL, device_id int, v0 int, v1 int, v2 float, v3 float);
SELECT create_hypertable('metrics_compressed','time',create_default_indexes:=false);

ALTER TABLE metrics_compressed DROP COLUMN filler_1;
INSERT INTO metrics_compressed(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id+1,  device_id + 2, device_id + 0.5, NULL FROM generate_series('2000-01-01 0:00:00+0'::timestamptz,'2000-01-05 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);
ALTER TABLE metrics_compressed DROP COLUMN filler_2;
INSERT INTO metrics_compressed(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id-1, device_id + 2, device_id + 0.5, NULL FROM generate_series('2000-01-06 0:00:00+0'::timestamptz,'2000-01-12 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);
ALTER TABLE metrics_compressed DROP COLUMN filler_3;
INSERT INTO metrics_compressed(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id, device_id + 2, device_id + 0.5, NULL FROM generate_series('2000-01-13 0:00:00+0'::timestamptz,'2000-01-19 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);
CREATE INDEX ON metrics_compressed(time);
CREATE INDEX ON metrics_compressed(device_id,time);
ANALYZE metrics_compressed;

-- compress chunks
ALTER TABLE metrics_compressed SET (timescaledb.compress, timescaledb.compress_orderby='time DESC', timescaledb.compress_segmentby='device_id');
SELECT compress_chunk(show_chunks('metrics_compressed'));
UPDATE metrics_compressed SET v3 = 42 WHERE device_id=1 AND time > '2000-01-01' AND time < '2000-01-02';

-- Reindexing compressed hypertable to update statistics
-- this is for planner tests which depend on them
-- necessary because this operation was previously done by compress_chunk
REINDEX TABLE _timescaledb_internal._compressed_hypertable_4;

-- create hypertable with space partitioning and compression
CREATE TABLE metrics_space_compressed(filler_1 int, filler_2 int, filler_3 int, time timestamptz NOT NULL, device_id int, v0 int, v1 int, v2 float, v3 float);
SELECT create_hypertable('metrics_space_compressed','time','device_id',3,create_default_indexes:=false);

ALTER TABLE metrics_space_compressed DROP COLUMN filler_1;
INSERT INTO metrics_space_compressed(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id+1, device_id + 2, device_id + 0.5, NULL FROM generate_series('2000-01-01 0:00:00+0'::timestamptz,'2000-01-05 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);
ALTER TABLE metrics_space_compressed DROP COLUMN filler_2;
INSERT INTO metrics_space_compressed(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id+1, device_id + 2, device_id + 0.5, NULL FROM generate_series('2000-01-06 0:00:00+0'::timestamptz,'2000-01-12 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);
ALTER TABLE metrics_space_compressed DROP COLUMN filler_3;
INSERT INTO metrics_space_compressed(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id+1, device_id + 2, device_id + 0.5, NULL FROM generate_series('2000-01-13 0:00:00+0'::timestamptz,'2000-01-19 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);
CREATE INDEX ON metrics_space_compressed(time);
CREATE INDEX ON metrics_space_compressed(device_id,time);
ANALYZE metrics_space_compressed;

-- compress chunks
ALTER TABLE metrics_space_compressed SET (timescaledb.compress, timescaledb.compress_orderby='time DESC', timescaledb.compress_segmentby='device_id');
SELECT compress_chunk(show_chunks('metrics_space_compressed'));

-- Reindexing compressed hypertable to update statistics
-- this is for planner tests which depend on them
-- necessary because this operation was previously done by compress_chunk
REINDEX TABLE _timescaledb_internal._compressed_hypertable_6;

CREATE TABLE metrics_int(
    time int NOT NULL,
    device_id int,
    sensor_id int,
    value float);
INSERT INTO metrics_int VALUES
    (-100,1,1,0.0),
    (-100,1,2,-100.0),
    (0,1,1,5.0),
    (5,1,2,10.0),
    (100,1,1,0.0),
    (100,1,2,-100.0);

CREATE TABLE devices(device_id INT, name TEXT);
INSERT INTO devices VALUES (1,'Device 1'),(2,'Device 2'),(3,'Device 3');

CREATE TABLE sensors(sensor_id INT, name TEXT);
INSERT INTO sensors VALUES (1,'Sensor 1'),(2,'Sensor 2'),(3,'Sensor 3');

CREATE TABLE metrics_tstz(time timestamptz, device_id INT, v1 float, v2 int);
SELECT create_hypertable('metrics_tstz','time');
INSERT INTO metrics_tstz VALUES
    (timestamptz '2018-01-01 05:00:00 PST', 1, 0.5, 10),
    (timestamptz '2018-01-01 05:00:00 PST', 2, 0.7, 20),
    (timestamptz '2018-01-01 05:00:00 PST', 3, 0.9, 30),
    (timestamptz '2018-01-01 07:00:00 PST', 1, 0.0, 0),
    (timestamptz '2018-01-01 07:00:00 PST', 2, 1.4, 40),
    (timestamptz '2018-01-01 07:00:00 PST', 3, 0.9, 30)
;

-- scramble index column names
--
-- index column names must never be used to match between relation columns and index columns
-- to catch any such bugs, we scramble the index column names in regresscheck-shared
DO $$
DECLARE
i regclass;
colname name;
BEGIN
  FOR i IN
    SELECT c.oid FROM pg_class c
    WHERE
      c.relkind='i' AND
      c.relnamespace IN (
        'public'::regnamespace,
        '_timescaledb_internal'::regnamespace
      ) AND
      c.relname NOT IN (
        'bgw_job_stat_pkey',
        'bgw_job_stat_history_pkey',
        'bgw_job_stat_history_job_id_idx',
        'bgw_policy_chunk_stats_job_id_chunk_id_key'
      )
  LOOP
    FOR colname IN SELECT a.attname FROM pg_attribute a WHERE a.attrelid = i ORDER BY a.attnum
    LOOP
      EXECUTE format('ALTER TABLE %s RENAME COLUMN %I TO %s', i, colname, format('XyZ_%s', colname));
    END LOOP;
  END LOOP;
END$$;

