-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- test conflict handling on compressed hypertables with unique constraints
set timescaledb.debug_compression_path_info to on;

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

SELECT cs.compress_relid AS "COMPRESSED_CHUNK" FROM _timescaledb_catalog.compression_settings cs WHERE cs.relid = :'CHUNK'::regclass \gset

SELECT format('%I.%I', n.nspname, i.relname) AS "COMPRESSED_CHUNK_INDEX"
FROM pg_index idx
JOIN pg_class i ON i.oid = idx.indexrelid
JOIN pg_namespace n ON n.oid = i.relnamespace
WHERE idx.indrelid = :'COMPRESSED_CHUNK'::regclass
LIMIT 1 \gset

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
  DROP INDEX :COMPRESSED_CHUNK_INDEX;
  INSERT INTO comp_conflicts_3 VALUES ('2020-01-01','d1', 'label', 0.1);
ROLLBACK;
BEGIN;
  DROP INDEX :COMPRESSED_CHUNK_INDEX;
  INSERT INTO comp_conflicts_3 VALUES ('2020-01-01','d2', 'label', 0.2);
ROLLBACK;
BEGIN;
  DROP INDEX :COMPRESSED_CHUNK_INDEX;
  INSERT INTO comp_conflicts_3 VALUES
  ('2020-01-01','d1', 'label', 0.1),
  ('2020-01-01','d2', 'label', 0.2),
  ('2020-01-01','d3', 'label', 0.3);
ROLLBACK;

-- using superuser to create indexes on compressed chunks
\c :TEST_DBNAME :ROLE_SUPERUSER
set timescaledb.debug_compression_path_info to on;
-- ignore matching partial index
BEGIN;
  DROP INDEX :COMPRESSED_CHUNK_INDEX;
  CREATE INDEX partial_index ON :COMPRESSED_CHUNK (device, label, _ts_meta_min_1 DESC, _ts_meta_max_1 DESC)
	WHERE label LIKE 'missing';
  INSERT INTO comp_conflicts_3 VALUES ('2020-01-01','d1', 'label', 0.1);
ROLLBACK;

-- ignore matching covering index
BEGIN;
  DROP INDEX :COMPRESSED_CHUNK_INDEX;
  CREATE INDEX covering_index ON :COMPRESSED_CHUNK (device) INCLUDE (label, _ts_meta_min_1, _ts_meta_max_1);
  INSERT INTO comp_conflicts_3 VALUES ('2020-01-01','d1', 'label', 0.1);
ROLLBACK;

-- out of order segmentby index, index is still usable
BEGIN;
  DROP INDEX :COMPRESSED_CHUNK_INDEX;
  CREATE INDEX partial_index ON :COMPRESSED_CHUNK (label, device, _ts_meta_min_1 DESC, _ts_meta_max_1 DESC);
  INSERT INTO comp_conflicts_3 VALUES ('2020-01-01','d1', 'label', 0.1);
ROLLBACK;

-- index with sequence number in the middle, index should be usable with single index scan key
BEGIN;
  DROP INDEX :COMPRESSED_CHUNK_INDEX;
  CREATE INDEX covering_index ON :COMPRESSED_CHUNK (device, _ts_meta_min_1 DESC, _ts_meta_max_1 DESC, label);
  INSERT INTO comp_conflicts_3 VALUES ('2020-01-01','d1', 'label', 0.1);
ROLLBACK;

-- ignore expression index
BEGIN;
  DROP INDEX :COMPRESSED_CHUNK_INDEX;
  CREATE INDEX partial_index ON :COMPRESSED_CHUNK (device, lower(label), _ts_meta_min_1 DESC, _ts_meta_max_1 DESC);
  INSERT INTO comp_conflicts_3 VALUES ('2020-01-01','d1', 'label', 0.1);
ROLLBACK;

-- ignore non-btree index
BEGIN;
  DROP INDEX :COMPRESSED_CHUNK_INDEX;
  CREATE INDEX partial_index ON :COMPRESSED_CHUNK USING brin (device, label, _ts_meta_min_1, _ts_meta_max_1);
  INSERT INTO comp_conflicts_3 VALUES ('2020-01-01','d1', 'label', 0.1);
ROLLBACK;
\set ON_ERROR_STOP 1
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
set timescaledb.debug_compression_path_info to on;

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
  DROP INDEX :COMPRESSED_CHUNK_INDEX;
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

  DROP INDEX :COMPRESSED_CHUNK_INDEX;
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
  DROP INDEX :COMPRESSED_CHUNK_INDEX;
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
   c.relid::text as chunk_name,
   c.status as chunk_status,
   comp.relid::text as compressed_chunk_name
FROM
   _timescaledb_catalog.hypertable h JOIN
  _timescaledb_catalog.chunk c ON h.id = c.hypertable_id
   LEFT JOIN _timescaledb_catalog.compression_settings cs
ON cs.relid = c.relid
   LEFT JOIN _timescaledb_catalog.chunk comp
ON cs.compress_relid = comp.relid;

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

RESET timescaledb.enable_dml_decompression;

-- gh issue #7342
CREATE TABLE test_collation (
        time int8 NOT NULL,
        device_id int4 NOT NULL,
        name TEXT NOT NULL,
        CONSTRAINT test_collation_pkey PRIMARY KEY (time, device_id, name)
);
SELECT create_hypertable('test_collation', 'time', chunk_time_interval => 2419200000);
ALTER TABLE test_collation
SET (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'device_id',
        timescaledb.compress_orderby = 'time DESC, name'
);
INSERT INTO "test_collation"
  ("time", "device_id", "name")
VALUES
  (1609477200000, 41, 'val1'),
  (1609478100000, 41, 'val1')
ON CONFLICT DO NOTHING;
SELECT compress_chunk(ch) FROM show_chunks('test_collation') ch;
INSERT INTO "test_collation"
  ("device_id", "time", "name")
VALUES
  (41, 1609477200000, 'val1'),
  (41, 1609478100000, 'val1')
ON CONFLICT DO NOTHING;

RESET timescaledb.debug_compression_path_info;

-- gh issue #7672
-- check additional INSERTS after hitting ON CONFLICT clause still go through
CREATE TABLE test_i7672(time timestamptz, device text, primary key(time,device));
SELECT create_hypertable('test_i7672', 'time');
ALTER TABLE test_i7672 SET (timescaledb.compress, timescaledb.compress_orderby='time DESC', timescaledb.compress_segmentby='device');
INSERT INTO test_i7672 VALUES ('2025-01-01','d1');

SELECT count(*) FROM (SELECT compress_chunk(show_chunks('test_i7672'))) c;

INSERT INTO test_i7672 VALUES
('2025-01-01','d1'),
('2025-01-01','d2')
ON CONFLICT DO NOTHING;

SELECT * FROM test_i7672 t ORDER BY t;

-- UPDATE of a unique constraint column on a compressed chunk
-- a single chunk holds all rows so conflicts stay within the chunk
CREATE TABLE comp_update_unique(time timestamptz NOT NULL, device text, value float, UNIQUE(time, device));
SELECT table_name FROM create_hypertable('comp_update_unique', 'time', chunk_time_interval => INTERVAL '10 years');
ALTER TABLE comp_update_unique SET (timescaledb.compress, timescaledb.compress_segmentby='device');
INSERT INTO comp_update_unique VALUES
('2020-01-01','d1',0.1),
('2020-01-01','d2',0.2),
('2020-01-02','d1',0.3);
SELECT count(compress_chunk(c)) FROM show_chunks('comp_update_unique') c;

-- updating a non-unique column is allowed
UPDATE comp_update_unique SET value = 1.0 WHERE device = 'd1';
SELECT * FROM comp_update_unique ORDER BY time, device;

-- updating the segmentby unique column to a free value works; the new segment
-- is decompressed up front so the unique index is enforced
UPDATE comp_update_unique SET device = 'd3' WHERE device = 'd1' AND time = '2020-01-02';
SELECT * FROM comp_update_unique ORDER BY time, device;

-- updating the orderby unique column (time) within the chunk works
UPDATE comp_update_unique SET time = '2020-01-03' WHERE device = 'd2';
SELECT * FROM comp_update_unique ORDER BY time, device;

-- a new value that is not constant cannot be derived and is rejected
\set ON_ERROR_STOP 0
UPDATE comp_update_unique SET device = device || '_x' WHERE device = 'd1';
\set ON_ERROR_STOP 1

DROP TABLE comp_update_unique;

-- setting a unique segmentby column to NULL cannot prune batches and is rejected
CREATE TABLE comp_update_null(time timestamptz NOT NULL, device text, value float, UNIQUE(time, device));
SELECT table_name FROM create_hypertable('comp_update_null', 'time', chunk_time_interval => INTERVAL '10 years');
ALTER TABLE comp_update_null SET (timescaledb.compress, timescaledb.compress_segmentby='device');
INSERT INTO comp_update_null VALUES ('2020-01-01','d1',0.1),('2020-01-01','d2',0.2);
SELECT count(compress_chunk(c)) FROM show_chunks('comp_update_null') c;
\set ON_ERROR_STOP 0
UPDATE comp_update_null SET device = NULL WHERE device = 'd1' AND time = '2020-01-01';
\set ON_ERROR_STOP 1
SELECT * FROM comp_update_null ORDER BY time, device;
DROP TABLE comp_update_null;

-- updating a unique column into a value that already exists in a row that is
-- still compressed (a different segment) must be rejected as a unique violation
CREATE TABLE comp_update_conflict(time timestamptz NOT NULL, device text, UNIQUE(time, device));
SELECT table_name FROM create_hypertable('comp_update_conflict', 'time', chunk_time_interval => INTERVAL '10 years');
ALTER TABLE comp_update_conflict SET (timescaledb.compress, timescaledb.compress_segmentby='device');
INSERT INTO comp_update_conflict VALUES ('2020-01-01','d1'),('2020-01-01','d2');
SELECT count(compress_chunk(c)) FROM show_chunks('comp_update_conflict') c;
-- (2020-01-01,d2) is in the d2 segment and stays compressed; moving the d1 row
-- onto it must still be detected as a conflict
\set ON_ERROR_STOP 0
UPDATE comp_update_conflict SET device = 'd2' WHERE device = 'd1';
\set ON_ERROR_STOP 1
SELECT * FROM comp_update_conflict ORDER BY time, device;
DROP TABLE comp_update_conflict;

-- a dropped column shifts attribute numbers; the constant value is still derived
CREATE TABLE comp_update_dropped(time timestamptz NOT NULL, junk int, device text, value float, UNIQUE(time, device));
SELECT table_name FROM create_hypertable('comp_update_dropped', 'time', chunk_time_interval => INTERVAL '10 years');
ALTER TABLE comp_update_dropped DROP COLUMN junk;
ALTER TABLE comp_update_dropped SET (timescaledb.compress, timescaledb.compress_segmentby='device');
INSERT INTO comp_update_dropped VALUES ('2020-01-01','d1',0.1),('2020-01-01','d2',0.2);
SELECT count(compress_chunk(c)) FROM show_chunks('comp_update_dropped') c;
UPDATE comp_update_dropped SET device = 'd3' WHERE device = 'd1';
SELECT * FROM comp_update_dropped ORDER BY time, device;
DROP TABLE comp_update_dropped;

-- a unique column that is neither segmentby nor orderby cannot prune batches and is rejected
CREATE TABLE comp_update_noprune(time timestamptz NOT NULL, device text, tag text, value float, UNIQUE(time, device, tag));
SELECT table_name FROM create_hypertable('comp_update_noprune', 'time', chunk_time_interval => INTERVAL '10 years');
ALTER TABLE comp_update_noprune SET (timescaledb.compress, timescaledb.compress_segmentby='device', timescaledb.compress_orderby='time');
INSERT INTO comp_update_noprune VALUES ('2020-01-01','d1','a',0.1),('2020-01-01','d2','b',0.2);
SELECT count(compress_chunk(c)) FROM show_chunks('comp_update_noprune') c;
-- updating tag (not segmentby, not orderby) cannot be pruned to specific batches
\set ON_ERROR_STOP 0
UPDATE comp_update_noprune SET tag = 'z' WHERE device = 'd1';
\set ON_ERROR_STOP 1
DROP TABLE comp_update_noprune;

-- two unique indexes: only the index whose key changes is decompressed
CREATE TABLE comp_update_multi(time timestamptz NOT NULL, a text, b int, UNIQUE(time, a), UNIQUE(time, b));
SELECT table_name FROM create_hypertable('comp_update_multi', 'time', chunk_time_interval => INTERVAL '10 years');
ALTER TABLE comp_update_multi SET (timescaledb.compress, timescaledb.compress_segmentby='a', timescaledb.compress_orderby='time, b');
INSERT INTO comp_update_multi VALUES ('2020-01-01','a1',1),('2020-01-01','a2',2);
SELECT count(compress_chunk(c)) FROM show_chunks('comp_update_multi') c;
-- a is segmentby (UNIQUE(time,a)); b has min/max metadata (UNIQUE(time,b)); both prunable
UPDATE comp_update_multi SET a = 'a3' WHERE a = 'a1';
UPDATE comp_update_multi SET b = 9 WHERE a = 'a2';
SELECT * FROM comp_update_multi ORDER BY time, a;
DROP TABLE comp_update_multi;

-- updating the time column across a chunk boundary is row movement and stays unsupported
CREATE TABLE comp_update_move(time timestamptz NOT NULL, device text, UNIQUE(time, device));
SELECT table_name FROM create_hypertable('comp_update_move', 'time', chunk_time_interval => INTERVAL '1 day');
ALTER TABLE comp_update_move SET (timescaledb.compress, timescaledb.compress_segmentby='device');
INSERT INTO comp_update_move VALUES ('2020-01-01','d1'),('2020-01-05','d1');
SELECT count(compress_chunk(c)) FROM show_chunks('comp_update_move') c;
\set ON_ERROR_STOP 0
UPDATE comp_update_move SET time = '2020-01-05' WHERE time = '2020-01-01';
\set ON_ERROR_STOP 1
DROP TABLE comp_update_move;

-- UPSERT (INSERT ... ON CONFLICT DO UPDATE) on compressed chunks
CREATE TABLE comp_upsert(time timestamptz NOT NULL, device text, value float, UNIQUE(time, device));
SELECT table_name FROM create_hypertable('comp_upsert', 'time');
ALTER TABLE comp_upsert SET (timescaledb.compress, timescaledb.compress_segmentby='device');
INSERT INTO comp_upsert VALUES ('2020-01-01','d1',0.1),('2020-01-01','d2',0.2);
SELECT count(compress_chunk(c)) FROM show_chunks('comp_upsert') c;

-- positive: ON CONFLICT DO UPDATE of a non-unique column resolves against compressed data
INSERT INTO comp_upsert VALUES ('2020-01-01','d1',9.9)
  ON CONFLICT (time, device) DO UPDATE SET value = excluded.value;
SELECT * FROM comp_upsert ORDER BY time, device;

-- positive: setting the conflict key columns to their own excluded value is a no-op and allowed
INSERT INTO comp_upsert VALUES ('2020-01-01','d2',7.7)
  ON CONFLICT (time, device) DO UPDATE SET device = excluded.device, value = excluded.value;
SELECT * FROM comp_upsert ORDER BY time, device;

-- positive: an insert that does not conflict is added normally
INSERT INTO comp_upsert VALUES ('2020-01-01','d3',0.3)
  ON CONFLICT (time, device) DO UPDATE SET value = excluded.value;
SELECT * FROM comp_upsert ORDER BY time, device;

-- negative: ON CONFLICT DO UPDATE that changes a unique column is rejected
-- (it could move a row onto a key held by a still-compressed row, see #9978)
\set ON_ERROR_STOP 0
INSERT INTO comp_upsert VALUES ('2020-01-01','d1',1.1)
  ON CONFLICT (time, device) DO UPDATE SET device = 'd9';
\set ON_ERROR_STOP 1
SELECT * FROM comp_upsert ORDER BY time, device;

-- negative: with DML decompression disabled the conflict check cannot run
SET timescaledb.enable_dml_decompression = false;
\set ON_ERROR_STOP 0
INSERT INTO comp_upsert VALUES ('2020-01-01','d1',5.5)
  ON CONFLICT (time, device) DO UPDATE SET value = excluded.value;
\set ON_ERROR_STOP 1
RESET timescaledb.enable_dml_decompression;

DROP TABLE comp_upsert;

