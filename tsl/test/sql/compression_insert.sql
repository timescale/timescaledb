-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE TABLE test1 (timec timestamptz , i integer ,
      b bigint, t text,  unique ( b, timec));
SELECT table_name from create_hypertable('test1', 'timec', chunk_time_interval=> INTERVAL '7 days');

INSERT INTO test1 select q, 10, 11, 'hello' FROM generate_series( '2020-01-03 10:00:00-05', '2020-01-03 12:00:00-05' , '5 min'::interval) q;
---INSERT INTO test1 SELECT '2021-01-01 10:00:00' , i, i *10, 'hello' FROM (Select generate_series(1, 100, 1) i ) q;

ALTER TABLE test1 set (timescaledb.compress, 
timescaledb.compress_segmentby = 'b', 
timescaledb.compress_orderby = 'timec DESC');

--other variants
--ALTER TABLE test1 set (timescaledb.compress, 
--timescaledb.compress_orderby = 'b, timec DESC');

--ALTER TABLE test1 set (timescaledb.compress, 
--timescaledb.compress_orderby = 'timec DESC');

SELECT compress_chunk(c)
FROM show_chunks('test1') c;

-- single and multi row insert into the compressed chunk --
INSERT INTO test1 SELECT '2020-01-02 11:16:00-05' , i, i +5, 'new' FROM (Select generate_series(11, 12, 1) i ) q;

INSERT INTO test1 SELECT '2020-01-02 11:16:00-05' , i, i +5, 'NEW'
FROM (Select generate_series(10, 20, 1) i ) q;

SELECT count(*) from test1;

explain verbose
SELECT * FROM test1 WHERE b = 11;

SELECT * FROM test1 WHERE b = 11 order by i, timec ;

explain verbose
SELECT * FROM test1 WHERE i = 11;
-- TODO fix this
--SELECT * FROM test1 WHERE i = 11;


--- TESTS for tables with defaults ---
--check sequences , generated etc. ....
--cannot check unique constraints yet
CREATE TABLE test2 (timec timestamptz , 
      i integer CHECK ( i > 10) ,
      b bigint default 20 , 
      t text NOT NULL,  unique ( b, timec),
      CONSTRAINT rowconstr CHECK ( b > i )
);

SELECT table_name from create_hypertable('test2', 'timec', chunk_time_interval=> INTERVAL '7 days');

ALTER TABLE test2 set (timescaledb.compress, 
timescaledb.compress_segmentby = 'b', 
timescaledb.compress_orderby = 'timec DESC');

INSERT INTO test2 values('2020-01-02 11:16:00-05' , 100, 105, 'first' );
SELECT compress_chunk(c)
FROM show_chunks('test2') c;

-- test if default value for b is used
INSERT INTO test2(timec, i, t) values('2020-01-02 10:16:00-05' , 11, 'default' );

SELECT b from test2 ORDER BY 1;

\set ON_ERROR_STOP 0
--null value for t, should fail
INSERT INTO test2 values ( '2020-01-02 01:00:00-05', 100, 200, NULL);
-- i=1, should fail
INSERT INTO test2 values ( '2020-01-02 01:00:00-05', 1, 10, 'null i');
-- b < i, should fail
INSERT INTO test2 values ( '2020-01-02 01:00:00-05', 22, 1, 'null i');
\set ON_ERROR_STOP 1
--verify we are still inserting into the compressed chunk i.e did not
--create a new chunk
SELECT count(c)
FROM show_chunks('test2') c;

-- need tests with dropped columns on hypertable and then adding data
-- to chunk
