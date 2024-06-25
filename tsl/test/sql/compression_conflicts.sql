-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- test conflict handling on compressed hypertables with unique constraints

-- test 1: single column primary key
CREATE TABLE comp_conflicts_1(time timestamptz, device text, value float, PRIMARY KEY(time));

SELECT table_name FROM create_hypertable('comp_conflicts_1','time');
ALTER TABLE comp_conflicts_1 SET (timescaledb.compress);

-- implicitly create chunk
INSERT INTO comp_conflicts_1 VALUES ('2020-01-01','d1',0.1);

-- sanity check behaviour without compression
-- should fail due to multiple entries with same time value
\set ON_ERROR_STOP 0
INSERT INTO comp_conflicts_1 VALUES ('2020-01-01','d1',0.1);
INSERT INTO comp_conflicts_1 VALUES
('2020-01-01','d1',0.1),
('2020-01-01','d2',0.2),
('2020-01-01','d3',0.3);
\set ON_ERROR_STOP 1

-- should succeed since there are no conflicts in the values
BEGIN;
INSERT INTO comp_conflicts_1 VALUES
('2020-01-01 0:00:01','d1',0.1),
('2020-01-01 0:00:02','d2',0.2),
('2020-01-01 0:00:03','d3',0.3);
ROLLBACK;

SELECT compress_chunk(c) AS "CHUNK" FROM show_chunks('comp_conflicts_1') c
\gset

-- after compression no data should be in uncompressed chunk
SELECT count(*) FROM ONLY :CHUNK;

-- repeat tests on an actual compressed chunk
-- should fail due to multiple entries with same time value
\set ON_ERROR_STOP 0
INSERT INTO comp_conflicts_1 VALUES ('2020-01-01','d1',0.1);
INSERT INTO comp_conflicts_1 VALUES
('2020-01-01','d1',0.1),
('2020-01-01','d2',0.2),
('2020-01-01','d3',0.3);
\set ON_ERROR_STOP 1

-- no data should be in uncompressed chunk since the inserts failed and their transaction rolled back
SELECT count(*) FROM ONLY :CHUNK;

-- should succeed since there are no conflicts in the values
BEGIN;

  INSERT INTO comp_conflicts_1 VALUES
  ('2020-01-01 0:00:01','d1',0.1),
  ('2020-01-01 0:00:02','d2',0.2),
  ('2020-01-01 0:00:03','d3',0.3);

  -- no data should have moved into uncompressed chunk for conflict check
  -- since we used metadata optimization to guarantee uniqueness
  SELECT count(*) FROM ONLY :CHUNK;

ROLLBACK;

-- no data should be in uncompressed chunk since we did rollback
SELECT count(*) FROM ONLY :CHUNK;

-- should fail since it conflicts with existing row
\set ON_ERROR_STOP 0
INSERT INTO comp_conflicts_1 VALUES ('2020-01-01','d1',0.1);
\set ON_ERROR_STOP 1

INSERT INTO comp_conflicts_1 VALUES ('2020-01-01','d1',0.1) ON CONFLICT DO NOTHING;

-- data should have move into uncompressed chunk for conflict check
SELECT count(*) FROM ONLY :CHUNK;

-- test 2: multi-column unique without segmentby
CREATE TABLE comp_conflicts_2(time timestamptz NOT NULL, device text, value float, UNIQUE(time, device));

SELECT table_name FROM create_hypertable('comp_conflicts_2','time');
ALTER TABLE comp_conflicts_2 SET (timescaledb.compress, timescaledb.compress_segmentby='');

-- implicitly create chunk
INSERT INTO comp_conflicts_2 VALUES ('2020-01-01','d1',0.1);
INSERT INTO comp_conflicts_2 VALUES ('2020-01-01','d2',0.2);

SELECT compress_chunk(c) AS "CHUNK" FROM show_chunks('comp_conflicts_2') c
\gset

-- after compression no data should be in uncompressed chunk
SELECT count(*) FROM ONLY :CHUNK;

-- should fail due to multiple entries with same time, device value
\set ON_ERROR_STOP 0
INSERT INTO comp_conflicts_2 VALUES ('2020-01-01','d1',0.1);
INSERT INTO comp_conflicts_2 VALUES ('2020-01-01','d2',0.2);
INSERT INTO comp_conflicts_2 VALUES
('2020-01-01','d1',0.1),
('2020-01-01','d2',0.2),
('2020-01-01','d3',0.3);
\set ON_ERROR_STOP 1

-- no data should be in uncompressed chunk since the inserts failed and their transaction rolled back
SELECT count(*) FROM ONLY :CHUNK;

-- should succeed since there are no conflicts in the values
BEGIN;

  INSERT INTO comp_conflicts_2 VALUES
  ('2020-01-01 0:00:01','d1',0.1),
  ('2020-01-01 0:00:01','d2',0.2),
  ('2020-01-01 0:00:01','d3',0.3);

  -- no data should have move into uncompressed chunk for conflict check
  -- since we used metadata optimization to guarantee uniqueness
  SELECT count(*) FROM ONLY :CHUNK;

ROLLBACK;

-- no data should be in uncompressed chunk since we did rollback
SELECT count(*) FROM ONLY :CHUNK;

-- should fail since it conflicts with existing row
\set ON_ERROR_STOP 0
INSERT INTO comp_conflicts_2 VALUES ('2020-01-01','d1',0.1);
\set ON_ERROR_STOP 1

INSERT INTO comp_conflicts_2 VALUES ('2020-01-01','d1',0.1) ON CONFLICT DO NOTHING;

-- data should have move into uncompressed chunk for conflict check
SELECT count(*) FROM ONLY :CHUNK;

-- test 3: multi-column primary key with segmentby
CREATE TABLE comp_conflicts_3(time timestamptz NOT NULL, device text, label text DEFAULT 'label', value float, UNIQUE(time, device, label));

SELECT table_name FROM create_hypertable('comp_conflicts_3','time');
ALTER TABLE comp_conflicts_3 SET (timescaledb.compress,timescaledb.compress_segmentby='device, label');

-- implicitly create chunk
INSERT INTO comp_conflicts_3 VALUES ('2020-01-01','d1', 'label', 0.1);
INSERT INTO comp_conflicts_3 VALUES ('2020-01-01','d2', 'label', 0.2);
INSERT INTO comp_conflicts_3 VALUES ('2020-01-01',NULL, 'label', 0.3);

SELECT compress_chunk(c) AS "CHUNK" FROM show_chunks('comp_conflicts_3') c
\gset

-- after compression no data should be in uncompressed chunk
SELECT count(*) FROM ONLY :CHUNK;

-- should fail due to multiple entries with same time, device value
\set ON_ERROR_STOP 0
INSERT INTO comp_conflicts_3 VALUES ('2020-01-01','d1', 'label', 0.1);
INSERT INTO comp_conflicts_3 VALUES ('2020-01-01','d2', 'label', 0.2);
INSERT INTO comp_conflicts_3 VALUES
('2020-01-01','d1', 'label', 0.1),
('2020-01-01','d2', 'label', 0.2),
('2020-01-01','d3', 'label', 0.3);
-- should work the same without the index present
BEGIN;
  DROP INDEX _timescaledb_internal.compress_hyper_6_6_chunk_device_label__ts_meta_sequence_num_idx;
  INSERT INTO comp_conflicts_3 VALUES ('2020-01-01','d1', 'label', 0.1);
ROLLBACK;
BEGIN;
  DROP INDEX _timescaledb_internal.compress_hyper_6_6_chunk_device_label__ts_meta_sequence_num_idx;
  INSERT INTO comp_conflicts_3 VALUES ('2020-01-01','d2', 'label', 0.2);
ROLLBACK;
BEGIN;
  DROP INDEX _timescaledb_internal.compress_hyper_6_6_chunk_device_label__ts_meta_sequence_num_idx;
  INSERT INTO comp_conflicts_3 VALUES
  ('2020-01-01','d1', 'label', 0.1),
  ('2020-01-01','d2', 'label', 0.2),
  ('2020-01-01','d3', 'label', 0.3);
ROLLBACK;

-- using superuser to create indexes on compressed chunks
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER
-- ignore matching partial index
BEGIN;
  DROP INDEX _timescaledb_internal.compress_hyper_6_6_chunk_device_label__ts_meta_sequence_num_idx;
  CREATE INDEX partial_index ON _timescaledb_internal.compress_hyper_6_6_chunk (device, label, _ts_meta_sequence_num)
	WHERE label LIKE 'missing';
  INSERT INTO comp_conflicts_3 VALUES ('2020-01-01','d1', 'label', 0.1);
ROLLBACK;

-- ignore matching covering index
BEGIN;
  DROP INDEX _timescaledb_internal.compress_hyper_6_6_chunk_device_label__ts_meta_sequence_num_idx;
  CREATE INDEX covering_index ON _timescaledb_internal.compress_hyper_6_6_chunk (device) INCLUDE (label, _ts_meta_sequence_num);
  INSERT INTO comp_conflicts_3 VALUES ('2020-01-01','d1', 'label', 0.1);
ROLLBACK;

-- ignore matching but out of order segmentby index
BEGIN;
  DROP INDEX _timescaledb_internal.compress_hyper_6_6_chunk_device_label__ts_meta_sequence_num_idx;
  CREATE INDEX covering_index ON _timescaledb_internal.compress_hyper_6_6_chunk (label, device, _ts_meta_sequence_num);
  INSERT INTO comp_conflicts_3 VALUES ('2020-01-01','d1', 'label', 0.1);
ROLLBACK;

-- ignore index with sequence number in the middle
BEGIN;
  DROP INDEX _timescaledb_internal.compress_hyper_6_6_chunk_device_label__ts_meta_sequence_num_idx;
  CREATE INDEX covering_index ON _timescaledb_internal.compress_hyper_6_6_chunk (device, _ts_meta_sequence_num, label);
  INSERT INTO comp_conflicts_3 VALUES ('2020-01-01','d1', 'label', 0.1);
ROLLBACK;

-- ignore expression index
BEGIN;
  DROP INDEX _timescaledb_internal.compress_hyper_6_6_chunk_device_label__ts_meta_sequence_num_idx;
  CREATE INDEX partial_index ON _timescaledb_internal.compress_hyper_6_6_chunk (device, lower(label), _ts_meta_sequence_num);
  INSERT INTO comp_conflicts_3 VALUES ('2020-01-01','d1', 'label', 0.1);
ROLLBACK;

-- ignore non-btree index
BEGIN;
  DROP INDEX _timescaledb_internal.compress_hyper_6_6_chunk_device_label__ts_meta_sequence_num_idx;
  CREATE INDEX partial_index ON _timescaledb_internal.compress_hyper_6_6_chunk USING brin (device, label, _ts_meta_sequence_num);
  INSERT INTO comp_conflicts_3 VALUES ('2020-01-01','d1', 'label', 0.1);
ROLLBACK;
\set ON_ERROR_STOP 1
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

-- no data should be in uncompressed chunk since the inserts failed and their transaction rolled back
SELECT count(*) FROM ONLY :CHUNK;

-- NULL is considered distinct from other NULL so even though the next INSERT looks
-- like a conflict it is not a constraint violation (PG15 makes NULL behaviour configurable)
BEGIN;
  INSERT INTO comp_conflicts_3 VALUES ('2020-01-01',NULL, 'label', 0.3);

  -- data for 1 segment (count = 1 value + 1 inserted) should be present in uncompressed chunk
  -- we treat NULLs as NOT DISTINCT and let the constraint configuration handle the check
  SELECT count(*) FROM ONLY :CHUNK;
ROLLBACK;

-- check if NULL handling works the same with the compressed index dropped
BEGIN;
  DROP INDEX _timescaledb_internal.compress_hyper_6_6_chunk_device_label__ts_meta_sequence_num_idx;
  INSERT INTO comp_conflicts_3 VALUES ('2020-01-01',NULL, 'label', 0.3);

  -- data for 1 segment (count = 1 value + 1 inserted) should be present in uncompressed chunk
  -- we treat NULLs as NOT DISTINCT and let the constraint configuration handle the check
  SELECT count(*) FROM ONLY :CHUNK;
ROLLBACK;

-- should succeed since there are no conflicts in the values
BEGIN;

  INSERT INTO comp_conflicts_3 VALUES ('2020-01-01 0:00:01','d1', 'label', 0.1);

  -- no data should have move into uncompressed chunk for conflict check
  -- since we used metadata optimization to guarantee uniqueness
  SELECT count(*) FROM ONLY :CHUNK;

ROLLBACK;

-- same as above but no index
-- should succeed since there are no conflicts in the values
BEGIN;

  DROP INDEX _timescaledb_internal.compress_hyper_6_6_chunk_device_label__ts_meta_sequence_num_idx;
  INSERT INTO comp_conflicts_3 VALUES ('2020-01-01 0:00:01','d1', 'label', 0.1);

  -- no data should have move into uncompressed chunk for conflict check
  -- since we used metadata optimization to guarantee uniqueness
  SELECT count(*) FROM ONLY :CHUNK;

ROLLBACK;

BEGIN;
  INSERT INTO comp_conflicts_3 VALUES
  ('2020-01-01 0:00:01','d1', 'label', 0.1),
  ('2020-01-01 0:00:01','d2', 'label', 0.2),
  ('2020-01-01 0:00:01','d3', 'label', 0.3);

  -- no data for should have move into uncompressed chunk for conflict check
  -- since we used metadata optimization to guarantee uniqueness
  SELECT count(*) FROM ONLY :CHUNK;
ROLLBACK;

-- same as above but no index
BEGIN;
  DROP INDEX _timescaledb_internal.compress_hyper_6_6_chunk_device_label__ts_meta_sequence_num_idx;
  INSERT INTO comp_conflicts_3 VALUES
  ('2020-01-01 0:00:01','d1', 'label', 0.1),
  ('2020-01-01 0:00:01','d2', 'label', 0.2),
  ('2020-01-01 0:00:01','d3', 'label', 0.3);

  -- no data for should have move into uncompressed chunk for conflict check
  -- since we used metadata optimization to guarantee uniqueness
  SELECT count(*) FROM ONLY :CHUNK;
ROLLBACK;

BEGIN;
  INSERT INTO comp_conflicts_3 VALUES ('2020-01-01 0:00:01','d3', 'label', 0.2);

  -- count = 1 since no data should have move into uncompressed chunk for conflict check since d3 is new segment
  SELECT count(*) FROM ONLY :CHUNK;
ROLLBACK;

-- no data should be in uncompressed chunk since we did rollback
SELECT count(*) FROM ONLY :CHUNK;

-- should fail since it conflicts with existing row
\set ON_ERROR_STOP 0
INSERT INTO comp_conflicts_3 VALUES ('2020-01-01','d1', 'label', 0.1);
\set ON_ERROR_STOP 1

INSERT INTO comp_conflicts_3 VALUES ('2020-01-01','d1', 'label', 0.1) ON CONFLICT DO NOTHING;

-- data should have move into uncompressed chunk for conflict check
SELECT count(*) FROM ONLY :CHUNK;

-- test 4: multi-column primary key with multi-column orderby compression
CREATE TABLE comp_conflicts_4(time timestamptz NOT NULL, device text, value float, UNIQUE(time, device));

SELECT table_name FROM create_hypertable('comp_conflicts_4','time');
ALTER TABLE comp_conflicts_4 SET (timescaledb.compress,timescaledb.compress_segmentby='',timescaledb.compress_orderby='time,device');

-- implicitly create chunk
INSERT INTO comp_conflicts_4 SELECT generate_series('2020-01-01'::timestamp, '2020-01-01 2:00:00', '1s'), 'd1',0.1;
INSERT INTO comp_conflicts_4 VALUES ('2020-01-01','d2',0.2);
INSERT INTO comp_conflicts_4 VALUES ('2020-01-01',NULL,0.3);

SELECT compress_chunk(c) AS "CHUNK" FROM show_chunks('comp_conflicts_4') c
\gset

-- after compression no data should be in uncompressed chunk
SELECT count(*) FROM ONLY :CHUNK;

-- NULL is considered distinct from other NULL so even though the next INSERT looks
-- like a conflict it is not a constraint violation (PG15 makes NULL behaviour configurable)
BEGIN;
  INSERT INTO comp_conflicts_4 VALUES ('2020-01-01',NULL,0.3);

  -- data for 1 segment (count = 1000 values + 1 inserted) should be present in uncompressed chunk
  -- we treat NULLs as NOT DISTINCT and let the constraint configuration handle the check
  SELECT count(*) FROM ONLY :CHUNK;
ROLLBACK;

-- should succeed since there are no conflicts in the values
BEGIN;

  INSERT INTO comp_conflicts_4 VALUES ('2020-01-01 2:00:01','d1',0.1);

  -- no data should have move into uncompressed chunk for conflict check
  -- since we used metadata optimization to guarantee uniqueness
  SELECT count(*) FROM ONLY :CHUNK;

ROLLBACK;

BEGIN;
  INSERT INTO comp_conflicts_4 VALUES
  ('2020-01-01 2:00:01','d1',0.1),
  ('2020-01-01 2:00:01','d2',0.2),
  ('2020-01-01 2:00:01','d3',0.3);

  -- no data for should have move into uncompressed chunk for conflict check
  -- since we used metadata optimization to guarantee uniqueness
  SELECT count(*) FROM ONLY :CHUNK;
ROLLBACK;

BEGIN;
  INSERT INTO comp_conflicts_4 VALUES ('2020-01-01 0:00:01','d3',0.2);

  -- count = 1 since no data should have move into uncompressed chunk for conflict check since d3 is new segment
  SELECT count(*) FROM ONLY :CHUNK;
ROLLBACK;

-- no data should be in uncompressed chunk since we did rollback
SELECT count(*) FROM ONLY :CHUNK;

-- should fail since it conflicts with existing row
\set ON_ERROR_STOP 0
INSERT INTO comp_conflicts_4 VALUES ('2020-01-01','d1',0.1);
\set ON_ERROR_STOP 1

-- data not should have move into uncompressed chunk for conflict check
SELECT count(*) FROM ONLY :CHUNK;

INSERT INTO comp_conflicts_4 VALUES ('2020-01-01 0:00:01','d1',0.1) ON CONFLICT DO NOTHING;
INSERT INTO comp_conflicts_4 VALUES ('2020-01-01 0:30:00','d1',0.1) ON CONFLICT DO NOTHING;

-- data should have move into uncompressed chunk for conflict check
-- 2 segments (count = 2000)
SELECT count(*) FROM ONLY :CHUNK;

CREATE OR REPLACE VIEW compressed_chunk_info_view AS
SELECT
   h.schema_name AS hypertable_schema,
   h.table_name AS hypertable_name,
   c.schema_name as chunk_schema,
   c.table_name as chunk_name,
   c.status as chunk_status,
   comp.schema_name as compressed_chunk_schema,
   comp.table_name as compressed_chunk_name
FROM
   _timescaledb_catalog.hypertable h JOIN
  _timescaledb_catalog.chunk c ON h.id = c.hypertable_id
   LEFT JOIN _timescaledb_catalog.chunk comp
ON comp.id = c.compressed_chunk_id;

CREATE TABLE compressed_ht (
       time TIMESTAMP WITH TIME ZONE NOT NULL,
       sensor_id INTEGER NOT NULL,
       cpu double precision null,
       temperature double precision null,
       name varchar(100) default 'this is a default string value'
);
CREATE UNIQUE INDEX sensor_id_time_idx on compressed_ht(time, sensor_id);

SELECT * FROM create_hypertable('compressed_ht', 'time',
       chunk_time_interval => INTERVAL '2 months');

-- create chunk 1
INSERT INTO compressed_ht VALUES ('2017-12-28 01:10:28.192199+05:30', '1', 0.876, 4.123, 'chunk 1');
INSERT INTO compressed_ht VALUES ('2017-12-24 01:10:28.192199+05:30', '1', 0.876, 4.123, 'chunk 1');

-- create chunk 2
INSERT INTO compressed_ht VALUES ('2017-03-28 01:10:28.192199+05:30', '2', 0.876, 4.123, 'chunk 2');
INSERT INTO compressed_ht VALUES ('2017-03-12 01:10:28.192199+05:30', '3', 0.876, 4.123, 'chunk 2');

-- create chunk 3
INSERT INTO compressed_ht VALUES ('2022-01-18 01:10:28.192199+05:30', '4', 0.876, 4.123, 'chunk 3');
INSERT INTO compressed_ht VALUES ('2022-01-08 01:10:28.192199+05:30', '4', 0.876, 4.123, 'chunk 3');
INSERT INTO compressed_ht VALUES ('2022-01-11 01:10:28.192199+05:30', '5', 0.876, 4.123, 'chunk 3');
INSERT INTO compressed_ht VALUES ('2022-01-24 01:10:28.192199+05:30', '6', 0.876, 4.123, 'chunk 3');

ALTER TABLE compressed_ht SET (
	timescaledb.compress,
	timescaledb.compress_segmentby = 'sensor_id'
);

SELECT COMPRESS_CHUNK(SHOW_CHUNKS('compressed_ht'));

-- check compression status
SELECT chunk_status,
       chunk_name as "CHUNK_NAME"
FROM compressed_chunk_info_view
WHERE hypertable_name = 'compressed_ht' ORDER BY chunk_name;

-- should report 0 row
SELECT COUNT(*) FROM compressed_ht WHERE name = 'ON CONFLICT DO UPDATE';

INSERT INTO compressed_ht VALUES ('2017-12-28 01:10:28.192199+05:30', '1', 0.876, 4.123, 'new insert row')
  ON conflict(sensor_id, time)
DO UPDATE SET sensor_id = excluded.sensor_id , name = 'ON CONFLICT DO UPDATE';

-- should report 1 row
SELECT COUNT(*) FROM compressed_ht WHERE name = 'ON CONFLICT DO UPDATE';

-- check that chunk 1 compression status is set to partial
SELECT chunk_status,
       chunk_name as "CHUNK_NAME"
FROM compressed_chunk_info_view
WHERE hypertable_name = 'compressed_ht' ORDER BY chunk_name;

INSERT INTO compressed_ht VALUES ('2022-01-24 01:10:28.192199+05:30', '6', 0.876, 4.123, 'new insert row')
  ON conflict(sensor_id, time)
DO UPDATE SET sensor_id = excluded.sensor_id , name = 'ON CONFLICT DO UPDATE' RETURNING *;

-- check that chunks 1 and 3 compression status is set to partial
SELECT chunk_status,
       chunk_name as "CHUNK_NAME"
FROM compressed_chunk_info_view
WHERE hypertable_name = 'compressed_ht' ORDER BY chunk_name;

-- test for disabling DML decompression
SHOW timescaledb.enable_dml_decompression;
SET timescaledb.enable_dml_decompression = false;

\set ON_ERROR_STOP 0
-- Should error because we disabled the DML decompression
INSERT INTO compressed_ht VALUES ('2022-01-24 01:10:28.192199+05:30', '6', 0.876, 4.123, 'new insert row')
  ON conflict(sensor_id, time)
DO UPDATE SET sensor_id = excluded.sensor_id , name = 'ON CONFLICT DO UPDATE' RETURNING *;

INSERT INTO compressed_ht VALUES ('2022-01-24 01:10:28.192199+05:30', '6', 0.876, 4.123, 'new insert row')
  ON conflict(sensor_id, time)
DO NOTHING;

-- Even a regular insert will fail due to unique constrant checks for dml decompression
INSERT INTO compressed_ht VALUES ('2022-01-24 01:10:28.192199+05:30', '7', 0.876, 4.123, 'new insert row');
\set ON_ERROR_STOP 1
