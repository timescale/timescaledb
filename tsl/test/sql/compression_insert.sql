-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE TABLE test1 (timec timestamptz , i integer ,
      b bigint, t text);
SELECT table_name from create_hypertable('test1', 'timec', chunk_time_interval=> INTERVAL '7 days');

INSERT INTO test1 select q, 10, 11, 'hello' FROM generate_series( '2020-01-03 10:00:00-05', '2020-01-03 12:00:00-05' , '5 min'::interval) q;
ALTER TABLE test1 set (timescaledb.compress, 
timescaledb.compress_segmentby = 'b', 
timescaledb.compress_orderby = 'timec DESC');

SELECT compress_chunk(c)
FROM show_chunks('test1') c;

SELECT count(*) FROM  test1;

--we have 1 compressed row --
SELECT COUNT(*) from _timescaledb_internal.compress_hyper_2_2_chunk;

-- single and multi row insert into the compressed chunk --
INSERT INTO test1 SELECT '2020-01-02 11:16:00-05' , 11, 16, 'new' ;
SELECT COUNT(*) from _timescaledb_internal.compress_hyper_2_2_chunk;

INSERT INTO test1 SELECT '2020-01-02 11:16:00-05' , i, i +5, 'NEW'
FROM (Select generate_series(10, 20, 1) i ) q;
SELECT COUNT(*) from _timescaledb_internal.compress_hyper_2_2_chunk;

SELECT count(*) from test1;

--Verify that all the data went into the initial chunk
SELECT count(*)
FROM show_chunks('test1') c;

SELECT * FROM test1 WHERE b = 11 order by i, timec ;

SELECT * FROM test1 WHERE i = 11;

DROP TABLE test1;

-- TEST 3 add tests with dropped columns on hypertable
-- also tests defaults
CREATE TABLE test2 ( itime integer, b bigint, t text);
SELECT table_name from create_hypertable('test2', 'itime', chunk_time_interval=> 10::integer);

--create a chunk 
INSERT INTO test2 SELECT t, 10,  'first'::text FROM generate_series(1, 7, 1) t;

ALTER TABLE test2 DROP COLUMN b;
-- TODO fix defaults ---
ALTER TABLE test2 ADD COLUMN c INT DEFAULT -15;
ALTER TABLE test2 ADD COLUMN d INT;

--create a new chunk
INSERT INTO test2 SELECT t, 'second'::text, 120, 1 FROM generate_series(11, 15, 1) t;
ALTER TABLE test2 set (timescaledb.compress, timescaledb.compress_segmentby = '', timescaledb.compress_orderby = 'c, itime DESC');

SELECT count(*) from ( select compress_chunk(c)
FROM show_chunks('test2') c ) q;

--write to both old chunks and new chunks 
INSERT INTO test2(itime ,t , d)  SELECT 9, '9', 90 ;
INSERT INTO test2(itime ,t , d)  SELECT 17, '17', 1700 ;

SELECT count(*) FROM show_chunks('test2') q;

SELECT * from test2 WHERE itime >= 9 and itime <= 17 
ORDER BY itime;
