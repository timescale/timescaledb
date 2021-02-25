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
INSERT INTO test1 SELECT '2020-01-02 11:16:00-05' , i, i +5, 'new' FROM (Select generate_series(11, 22, 1) i ) q;

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
