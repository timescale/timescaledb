-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set PREFIX 'EXPLAIN (ANALYZE,VERBOSE,SUMMARY OFF,TIMING OFF,COSTS OFF)'

CREATE TABLE metrics_int2(c1 int,c2 int, c3 int, c4 int, c5 int, time int2 NOT NULL);
CREATE TABLE metrics_int4(c1 int,c2 int, c3 int, c4 int, c5 int, time int4 NOT NULL);
CREATE TABLE metrics_int8(c1 int,c2 int, c3 int, c4 int, c5 int, time int8 NOT NULL);
CREATE TABLE metrics_date(c1 int,c2 int, c3 int, c4 int, c5 int, time date NOT NULL);
CREATE TABLE metrics_timestamp(c1 int,c2 int, c3 int, c4 int, c5 int, time timestamp NOT NULL);
CREATE TABLE metrics_timestamptz(c1 int,c2 int, c3 int, c4 int, c5 int, time timestamptz NOT NULL);
CREATE TABLE metrics_space(c1 int,c2 int, c3 int, c4 int, c5 int, time timestamp NOT NULL, device text);

SELECT table_name FROM create_hypertable('metrics_int2','time',chunk_time_interval:=10);
SELECT table_name FROM create_hypertable('metrics_int4','time',chunk_time_interval:=10);
SELECT table_name FROM create_hypertable('metrics_int8','time',chunk_time_interval:=10);
SELECT table_name FROM create_hypertable('metrics_date','time');
SELECT table_name FROM create_hypertable('metrics_timestamp','time');
SELECT table_name FROM create_hypertable('metrics_timestamptz','time');
SELECT table_name FROM create_hypertable('metrics_space','time','device',4);

CREATE FUNCTION drop_column(text) RETURNS VOID LANGUAGE PLPGSQL AS $$
DECLARE
  tbl name;
BEGIN
  FOR tbl IN SELECT 'metrics_' || unnest(ARRAY['int2','int4','int8','date','timestamp','timestamptz','space'])
  LOOP
    EXECUTE format('ALTER TABLE %I DROP COLUMN %I;', tbl, $1);
  END LOOP;
END;
$$;

-- create 4 chunks each with different physical layout
SELECT drop_column('c1');
INSERT INTO metrics_int2(time) VALUES (0);
INSERT INTO metrics_int4(time) VALUES (0);
INSERT INTO metrics_int8(time) VALUES (0);
INSERT INTO metrics_date(time) VALUES ('2000-01-01');
INSERT INTO metrics_timestamp(time) VALUES ('2000-01-01');
INSERT INTO metrics_timestamptz(time) VALUES ('2000-01-01');
INSERT INTO metrics_space(time,device) VALUES ('2000-01-01','1'),('2000-01-01','2');

SELECT drop_column('c2');
INSERT INTO metrics_int2(time) VALUES (10);
INSERT INTO metrics_int4(time) VALUES (10);
INSERT INTO metrics_int8(time) VALUES (10);
INSERT INTO metrics_date(time) VALUES ('2001-01-01');
INSERT INTO metrics_timestamp(time) VALUES ('2001-01-01');
INSERT INTO metrics_timestamptz(time) VALUES ('2001-01-01');
INSERT INTO metrics_space(time,device) VALUES ('2001-01-01','1'),('2001-01-01','2');

SELECT drop_column('c3');
INSERT INTO metrics_int2(time) VALUES (20);
INSERT INTO metrics_int4(time) VALUES (20);
INSERT INTO metrics_int8(time) VALUES (20);
INSERT INTO metrics_date(time) VALUES ('2002-01-01');
INSERT INTO metrics_timestamp(time) VALUES ('2002-01-01');
INSERT INTO metrics_timestamptz(time) VALUES ('2002-01-01');
INSERT INTO metrics_space(time,device) VALUES ('2002-01-01','1'),('2002-01-01','2');

SELECT drop_column('c4');
INSERT INTO metrics_int2(time) VALUES (30);
INSERT INTO metrics_int4(time) VALUES (30);
INSERT INTO metrics_int8(time) VALUES (30);
INSERT INTO metrics_date(time) VALUES ('2003-01-01');
INSERT INTO metrics_timestamp(time) VALUES ('2003-01-01');
INSERT INTO metrics_timestamptz(time) VALUES ('2003-01-01');
INSERT INTO metrics_space(time,device) VALUES ('2003-01-01','1'),('2003-01-01','2');

SELECT drop_column('c5');

-- immutable constraints
-- should not have ChunkAppend since constraint is immutable and postgres already does the exclusion
-- should only hit 1 chunk and base table
:PREFIX DELETE FROM metrics_int2 WHERE time = 15;
:PREFIX DELETE FROM metrics_int4 WHERE time = 15;
:PREFIX DELETE FROM metrics_int8 WHERE time = 15;

-- stable constraints
-- should have ChunkAppend since constraint is stable
-- should only hit 1 chunk and base table
:PREFIX DELETE FROM metrics_int2 WHERE time = length(substring(version(),1,23));
:PREFIX DELETE FROM metrics_int4 WHERE time = length(substring(version(),1,23));
:PREFIX DELETE FROM metrics_int8 WHERE time = length(substring(version(),1,23));

-- should have ChunkAppend since constraint is stable
-- should only hit 1 chunk and base table, toplevel rows should be 1
:PREFIX DELETE FROM metrics_int2 WHERE time = length(substring(version(),1,20)) RETURNING 'returning', time;
:PREFIX DELETE FROM metrics_int4 WHERE time = length(substring(version(),1,20)) RETURNING 'returning', time;
:PREFIX DELETE FROM metrics_int8 WHERE time = length(substring(version(),1,20)) RETURNING 'returning', time;

-- immutable constraints
-- should not have ChunkAppend since constraint is immutable and postgres already does the exclusion
-- should only hit 1 chunk and base table
BEGIN;
:PREFIX DELETE FROM metrics_date WHERE time = '2000-01-01';
:PREFIX DELETE FROM metrics_timestamp WHERE time = '2000-01-01';
:PREFIX DELETE FROM metrics_timestamptz WHERE time = '2000-01-01';
:PREFIX DELETE FROM metrics_space WHERE time = '2000-01-01' AND device = '1';
:PREFIX DELETE FROM metrics_space WHERE time = '2000-01-01';
ROLLBACK;

-- stable constraints
-- should have ChunkAppend since constraint is stable
-- should only hit 1 chunk and base table
BEGIN;
:PREFIX DELETE FROM metrics_date WHERE time = '2000-01-01'::text::date;
:PREFIX DELETE FROM metrics_timestamp WHERE time = '2000-01-01'::text::timestamp;
:PREFIX DELETE FROM metrics_timestamptz WHERE time = '2000-01-01'::text::timestamptz;
ROLLBACK;

-- space partitioning
-- should have ChunkAppend since constraint is stable
-- should only hit 1 chunk and base table
BEGIN;
:PREFIX DELETE FROM metrics_space WHERE time = '2000-01-01'::text::timestamptz AND device = format('1');
ROLLBACK;
BEGIN;
:PREFIX DELETE FROM metrics_space WHERE device = format('1');
ROLLBACK;

-- should have ChunkAppend since constraint is stable
-- should only hit 1 chunk and base table, toplevel rows should be 1
BEGIN;
:PREFIX DELETE FROM metrics_date WHERE time = '2000-01-01'::text::date RETURNING 'returning', time;
:PREFIX DELETE FROM metrics_timestamp WHERE time = '2000-01-01'::text::timestamp RETURNING 'returning', time;
:PREFIX DELETE FROM metrics_timestamptz WHERE time = '2000-01-01'::text::timestamptz RETURNING 'returning', time;
ROLLBACK;

-- subselects
-- no chunk exclusion for subqueries joins atm
:PREFIX DELETE FROM metrics_int4 WHERE time IN (SELECT time FROM metrics_int2) AND time < length(version());
:PREFIX DELETE FROM metrics_int4 WHERE time IN (SELECT time FROM metrics_int2 WHERE time < length(version()));
:PREFIX DELETE FROM metrics_int4 WHERE time IN (SELECT time FROM metrics_int2 WHERE time < length(version())) AND time < length(version());

-- join
-- no chunk exclusion for subqueries joins atm
:PREFIX DELETE FROM metrics_int4 m4 USING metrics_int2 m2;
:PREFIX DELETE FROM metrics_int4 m4 USING metrics_int2 m2 WHERE m4.time = m2.time;
:PREFIX DELETE FROM metrics_int4 m4 USING metrics_int2 m2 WHERE m4.time = m2.time AND m4.time < length(version());

-- test interaction with compression
-- with chunk exclusion for compressed chunks operations that would
-- error because they hit compressed chunks before can succeed now
-- if those chunks get excluded
CREATE TABLE metrics_compressed(time timestamptz NOT NULL, device int, value float);
SELECT table_name FROM create_hypertable('metrics_compressed','time');
ALTER TABLE metrics_compressed SET (timescaledb.compress);

-- create first chunk and compress
INSERT INTO metrics_compressed VALUES ('2000-01-01',1,0.5);
SELECT count(compress_chunk(chunk)) FROM show_chunks('metrics_compressed') chunk;

-- create more chunks
INSERT INTO metrics_compressed VALUES ('2010-01-01',1,0.5),('2011-01-01',1,0.5),('2012-01-01',1,0.5);

-- delete from uncompressed chunks with non-immutable constraints
BEGIN;
:PREFIX DELETE FROM metrics_compressed WHERE time > '2005-01-01'::text::timestamptz;
ROLLBACK;
