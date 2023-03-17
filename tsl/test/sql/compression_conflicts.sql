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

  -- data should have move into uncompressed chunk for conflict check
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
ALTER TABLE comp_conflicts_2 SET (timescaledb.compress);

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

  -- data should have move into uncompressed chunk for conflict check
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
CREATE TABLE comp_conflicts_3(time timestamptz NOT NULL, device text, value float, UNIQUE(time, device));

SELECT table_name FROM create_hypertable('comp_conflicts_3','time');
ALTER TABLE comp_conflicts_3 SET (timescaledb.compress,timescaledb.compress_segmentby='device');

-- implicitly create chunk
INSERT INTO comp_conflicts_3 VALUES ('2020-01-01','d1',0.1);
INSERT INTO comp_conflicts_3 VALUES ('2020-01-01','d2',0.2);
INSERT INTO comp_conflicts_3 VALUES ('2020-01-01',NULL,0.3);

SELECT compress_chunk(c) AS "CHUNK" FROM show_chunks('comp_conflicts_3') c
\gset

-- after compression no data should be in uncompressed chunk
SELECT count(*) FROM ONLY :CHUNK;

-- should fail due to multiple entries with same time, device value
\set ON_ERROR_STOP 0
INSERT INTO comp_conflicts_3 VALUES ('2020-01-01','d1',0.1);
INSERT INTO comp_conflicts_3 VALUES ('2020-01-01','d2',0.2);
INSERT INTO comp_conflicts_3 VALUES
('2020-01-01','d1',0.1),
('2020-01-01','d2',0.2),
('2020-01-01','d3',0.3);
\set ON_ERROR_STOP 1

-- no data should be in uncompressed chunk since the inserts failed and their transaction rolled back
SELECT count(*) FROM ONLY :CHUNK;

-- NULL is considered distinct from other NULL so even though the next INSERT looks
-- like a conflict it is not a constraint violation (PG15 makes NULL behaviour configurable)
BEGIN;
  INSERT INTO comp_conflicts_3 VALUES ('2020-01-01',NULL,0.3);

  -- data for 1 segment (count = 1 value + 1 inserted) should be present in uncompressed chunk
  SELECT count(*) FROM ONLY :CHUNK;
ROLLBACK;

-- should succeed since there are no conflicts in the values
BEGIN;

  INSERT INTO comp_conflicts_3 VALUES ('2020-01-01 0:00:01','d1',0.1);

  -- data for 1 segment (count = 1 value + 1 inserted) should have move into uncompressed chunk for conflict check
  SELECT count(*) FROM ONLY :CHUNK;

ROLLBACK;

BEGIN;
  INSERT INTO comp_conflicts_3 VALUES
  ('2020-01-01 0:00:01','d1',0.1),
  ('2020-01-01 0:00:01','d2',0.2),
  ('2020-01-01 0:00:01','d3',0.3);

  -- data for 2 segment (count = 2 value + 2 inserted) should have move into uncompressed chunk for conflict check
  SELECT count(*) FROM ONLY :CHUNK;
ROLLBACK;

BEGIN;
  INSERT INTO comp_conflicts_3 VALUES ('2020-01-01 0:00:01','d3',0.2);

  -- count = 1 since no data should have move into uncompressed chunk for conflict check since d3 is new segment
  SELECT count(*) FROM ONLY :CHUNK;
ROLLBACK;

-- no data should be in uncompressed chunk since we did rollback
SELECT count(*) FROM ONLY :CHUNK;

-- should fail since it conflicts with existing row
\set ON_ERROR_STOP 0
INSERT INTO comp_conflicts_3 VALUES ('2020-01-01','d1',0.1);
\set ON_ERROR_STOP 1

INSERT INTO comp_conflicts_3 VALUES ('2020-01-01','d1',0.1) ON CONFLICT DO NOTHING;

-- data should have move into uncompressed chunk for conflict check
SELECT count(*) FROM ONLY :CHUNK;

