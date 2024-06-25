-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set PREFIX 'EXPLAIN (costs off, summary off, timing off) '
CREATE TABLE test1 (timec timestamptz , i integer ,
      b bigint, t text);
SELECT table_name from create_hypertable('test1', 'timec', chunk_time_interval=> INTERVAL '7 days');

INSERT INTO test1 SELECT q, 10, 11, 'hello' FROM generate_series( '2020-01-03 10:00:00-05', '2020-01-03 12:00:00-05' , '5 min'::interval) q;
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

INSERT INTO test1 SELECT '2020-01-02 11:16:00-05' , i, i +5, 'clay'
FROM (Select generate_series(10, 20, 1) i ) q;
SELECT COUNT(*) from _timescaledb_internal.compress_hyper_2_2_chunk;

SELECT count(*) from test1;

-- single row copy
COPY test1 FROM STDIN DELIMITER ',';
2020-01-02 11:16:00-05,11,16,copy
\.
SELECT COUNT(*) from _timescaledb_internal.compress_hyper_2_2_chunk;

-- multi row copy
COPY test1 FROM STDIN DELIMITER ',';
2020-01-02 11:16:00-05,11,16,multicopy
2020-01-02 11:16:00-05,12,17,multicopy
2020-01-02 11:16:00-05,13,18,multicopy
\.
SELECT COUNT(*) from _timescaledb_internal.compress_hyper_2_2_chunk;

--Verify that all the data went into the initial chunk
SELECT count(*)
FROM show_chunks('test1') c;

SELECT * FROM test1 WHERE b = 11 order by i, timec ;

SELECT * FROM test1 WHERE i = 11 order by 1, 2, 3, 4;

-- insert nulls except for timec
INSERT INTO test1 SELECT '2020-01-02 11:46:00-05' , NULL, NULL, NULL;

SELECT count(*)
FROM show_chunks('test1') c;

-- copy NULL
COPY test1 FROM STDIN DELIMITER ',' NULL 'NULL';
2020-01-02 11:46:00-05,NULL,NULL,NULL
\.

SELECT count(*)
FROM show_chunks('test1') c;

SELECT * from test1 WHERE i is NULL;

--TEST 2 now alter the table and add a new column to it
ALTER TABLE test1 ADD COLUMN newtcol varchar(400);
--add rows with segments that overlap some of the previous ones
SELECT count(*) from _timescaledb_internal.compress_hyper_2_2_chunk;
INSERT INTO test1 SELECT '2020-01-02 11:16:00-05' , 100, 101, 'prev101', 'this is the newtcol101';
INSERT INTO test1 SELECT '2020-01-02 11:16:00-05' , i, 16, 'prev16', 'this is the newtcol16'
FROM (Select generate_series(11, 16, 1) i ) q;
SELECT * FROM test1 WHERE b = 16 order by 1, 2, 3, 4, 5;

--number of rows in the chunk
SELECT count(*) from _timescaledb_internal.compress_hyper_2_2_chunk;
SELECT count(*)
FROM show_chunks('test1') c;

COPY test1 FROM STDIN DELIMITER ',';
2020-01-02 11:16:00-05,100,101,prev101,newtcol101
\.

COPY test1 FROM STDIN DELIMITER ',';
2020-01-02 11:16:00-05,11,16,prev16,newtcol16
2020-01-02 11:16:00-05,12,16,prev16,newtcol16
2020-01-02 11:16:00-05,13,16,prev16,newtcol16
\.

--number of rows in the chunk
SELECT count(*) from _timescaledb_internal.compress_hyper_2_2_chunk;
SELECT count(*)
FROM show_chunks('test1') c;

SELECT * FROM test1 WHERE newtcol IS NOT NULL ORDER BY 1,2,3;

DROP TABLE test1;

-- TEST 3 add tests with dropped columns on hypertable
-- also tests defaults
CREATE TABLE test2 ( itime integer, b bigint, t text);
SELECT table_name from create_hypertable('test2', 'itime', chunk_time_interval=> 10::integer);

--create a chunk
INSERT INTO test2 SELECT t, 10,  'first'::text FROM generate_series(1, 7) t;

ALTER TABLE test2 DROP COLUMN b;
ALTER TABLE test2 ADD COLUMN c INT DEFAULT -15;
ALTER TABLE test2 ADD COLUMN d INT;

--create a new chunk
INSERT INTO test2 SELECT t, 'second'::text, 120, 1 FROM generate_series(11, 15) t;

ALTER TABLE test2 set (timescaledb.compress, timescaledb.compress_segmentby = '', timescaledb.compress_orderby = 'c, itime DESC');

SELECT count(*) from ( SELECT compress_chunk(c)
FROM show_chunks('test2') c ) q;

--write to both old chunks and new chunks
INSERT INTO test2(itime ,t , d)  SELECT 9, '9', 90 ;
INSERT INTO test2(itime ,t , d)  SELECT 17, '17', 1700 ;

COPY test2(itime,t,d) FROM STDIN DELIMITER ',';
9,9copy,90
17,17copy,1700
\.

SELECT count(*) FROM show_chunks('test2') q;

SELECT * from test2 WHERE itime >= 9 and itime <= 17
ORDER BY 1,2,3;

-- now add a column to the compressed hypertable
-- we have dropped columns and newly added columns now
ALTER TABLE test2 ADD COLUMN charcol varchar(45);
INSERT INTO test2(itime ,t , d, charcol)
values (2, '2', 22, 'thisis22'), (17, '17', 1701, 'thisis1700') ;

COPY test2(itime,t,d,charcol) FROM STDIN DELIMITER ',';
2,2copy,22,22copy
17,17copy,1701,1700copy
\.

SELECT * from test2 where itime = 2 or itime =17
ORDER BY 1, 2, 3, 4, 5;

DROP TABLE test2;

--- TEST3 tables with defaults ---
--  sequences, generated values, check constraints into compressed chunks
CREATE TABLE test2 (timec timestamptz ,
      i integer CHECK ( i > 10) ,
      b bigint default 20 ,
      t text NOT NULL,
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
COPY test2(timec,i,t) FROM STDIN DELIMITER ',';
2020-01-02 10:16:00-05,11,defaultcopy
\.

SELECT b from test2 ORDER BY 1;

\set ON_ERROR_STOP 0
--null value for t, should fail
INSERT INTO test2 values ( '2020-01-02 01:00:00-05', 100, 200, NULL);
COPY test2 FROM STDIN DELIMITER ',' NULL 'NULL';
2020-01-02 01:00:00-05,100,200,NULL
\.

-- i=1, should fail
INSERT INTO test2 values ( '2020-01-02 01:00:00-05', 1, 10, 'null i');
COPY test2 FROM STDIN DELIMITER ',';
2020-01-02 01:00:00-05,1,10,null i
\.
-- b < i, should fail
INSERT INTO test2 values ( '2020-01-02 01:00:00-05', 22, 1, 'null i');
COPY test2 FROM STDIN DELIMITER ',';
2020-01-02 01:00:00-05,22,1,null i
\.
\set ON_ERROR_STOP 1
--verify we are still INSERTing into the compressed chunk i.e did not
--create a new chunk
SELECT count(c)
FROM show_chunks('test2') c;

-- TEST4 with sequence .
CREATE SEQUENCE vessel_id_seq
    INCREMENT 1
    START 1    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1;

CREATE TABLE vessels (timec timestamptz ,
      id bigint NOT NULL DEFAULT nextval('vessel_id_seq'::regclass),
      i integer CHECK ( i > 10) ,
      b bigint default 20 ,
      t text NOT NULL,
      CONSTRAINT rowconstr CHECK ( b > i )
);

SELECT table_name from create_hypertable('vessels', 'timec', chunk_time_interval=> INTERVAL '7 days');

ALTER TABLE vessels set (timescaledb.compress,
timescaledb.compress_segmentby = 'b',
timescaledb.compress_orderby = 'timec DESC');

INSERT INTO vessels(timec, i, b, t) values('2020-01-02 11:16:00-05' , 100, 105, 'first' );
SELECT compress_chunk(c)
FROM show_chunks('vessels') c;

-- test if default value for b and sequence value for id is used
INSERT INTO vessels(timec, i, t) values('2020-01-02 10:16:00-05' , 11, 'default' );
COPY vessels(timec,i,t )FROM STDIN DELIMITER ',';
2020-01-02 10:16:00-05,11,default copy
\.

SELECT timec, id, b from vessels order by 2, 1;

-- TEST5 generated values
CREATE table test_gen (
    id int generated by default AS IDENTITY ,
    payload text
);

SELECT create_hypertable('test_gen', 'id', chunk_time_interval=>10);
ALTER TABLE test_gen set (timescaledb.compress);

INSERT into test_gen (payload) SELECT generate_series(1,15) ;

SELECT max(id) from test_gen;

SELECT compress_chunk(c)
FROM show_chunks('test_gen') c;

INSERT INTO test_gen (payload) values(17);
SELECT * from test_gen WHERE id = (Select max(id) from test_gen);

COPY test_gen(payload) FROM STDIN DELIMITER ',';
18
\.

SELECT * from test_gen WHERE id = (Select max(id) from test_gen);

-- TEST triggers
-- insert into compressed hypertables with triggers
CREATE OR REPLACE FUNCTION row_trig_value_gt_0() RETURNS TRIGGER AS $$
BEGIN
  RAISE NOTICE 'Trigger % % % % on %: % %', TG_NAME, TG_WHEN, TG_OP, TG_LEVEL, TG_TABLE_NAME, NEW, OLD;
  IF NEW.value <= 0 THEN
    RAISE NOTICE 'Skipping insert';
    RETURN NULL;
  END IF;
  RETURN NEW;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION row_trig_value_mod() RETURNS TRIGGER AS $$
BEGIN
  RAISE NOTICE 'Trigger % % % % on %: % %', TG_NAME, TG_WHEN, TG_OP, TG_LEVEL, TG_TABLE_NAME, NEW, OLD;
  NEW.value = NEW.value + 100;
  RETURN NEW;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION stmt_trig_info() RETURNS TRIGGER AS $$
BEGIN
  RAISE NOTICE 'Trigger % % % % on %: % %', TG_NAME, TG_WHEN, TG_OP, TG_LEVEL, TG_TABLE_NAME, NEW, OLD;
  RETURN NEW;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION stmt_trig_error() RETURNS TRIGGER AS $$
BEGIN
  RAISE EXCEPTION 'Trigger % % % % on %: % %', TG_NAME, TG_WHEN, TG_OP, TG_LEVEL, TG_TABLE_NAME, NEW, OLD;
  RETURN NULL;
END
$$ LANGUAGE plpgsql;

CREATE TABLE trigger_test(time timestamptz NOT NULL, device int, value int, dropcol1 int);
SELECT create_hypertable('trigger_test','time');

--create chunk and compress
--the first chunk is created with dropcol1
INSERT INTO trigger_test(time, device, value,dropcol1) SELECT '2000-01-01',1,1,1;
-- drop the column before we compress
ALTER TABLE trigger_test DROP COLUMN dropcol1;

ALTER TABLE trigger_test SET (timescaledb.compress,timescaledb.compress_segmentby='device');
SELECT compress_chunk(c) FROM show_chunks('trigger_test') c;

-- BEFORE ROW trigger
CREATE TRIGGER t1 BEFORE INSERT ON trigger_test FOR EACH ROW EXECUTE FUNCTION row_trig_value_gt_0();

-- should be 1
SELECT count(*) FROM trigger_test;

-- try insert that gets skipped by trigger
INSERT INTO trigger_test SELECT '2000-01-01',1,0;
COPY trigger_test FROM STDIN DELIMITER ',';
2000-01-01 01:00:00-05,1,0
\.

-- should not insert rows. count is 1
SELECT count(*) FROM trigger_test;

-- try again without being skipped
BEGIN;
INSERT INTO trigger_test SELECT '2000-01-01',1,1;
COPY trigger_test FROM STDIN DELIMITER ',';
2000-01-01 01:00:00-05,1,1
\.
-- should be 3
SELECT count(*) FROM trigger_test;
ROLLBACK;

DROP TRIGGER t1 ON trigger_test;

-- BEFORE ROW trigger that modifies tuple
CREATE TRIGGER t1_mod BEFORE INSERT ON trigger_test FOR EACH ROW EXECUTE FUNCTION row_trig_value_mod();

BEGIN;
INSERT INTO trigger_test SELECT '2000-01-01',1,11;
COPY trigger_test FROM STDIN DELIMITER ',';
2000-01-01 01:00:00-05,1,12
\.

-- value for both new tuples should be > 100
SELECT * FROM trigger_test ORDER BY 3;
ROLLBACK;

DROP TRIGGER t1_mod ON trigger_test;

-- BEFORE ROW conditional trigger
CREATE TRIGGER t1_cond BEFORE INSERT ON trigger_test FOR EACH ROW WHEN (NEW.value > 10) EXECUTE FUNCTION row_trig_value_mod();

-- test with condition being false
BEGIN;
INSERT INTO trigger_test SELECT '2000-01-01',1,1;
COPY trigger_test FROM STDIN DELIMITER ',';
2000-01-01 01:00:00-05,1,2
\.

-- value for both new tuples should not be > 100
SELECT * FROM trigger_test ORDER BY 3;
ROLLBACK;

-- test with condition being true
BEGIN;
INSERT INTO trigger_test SELECT '2000-01-01',1,11;
COPY trigger_test FROM STDIN DELIMITER ',';
2000-01-01 01:00:00-05,1,12
\.

-- value for both new tuples should be > 100
SELECT * FROM trigger_test ORDER BY 3;
ROLLBACK;

DROP TRIGGER t1_cond ON trigger_test;

-- BEFORE ROW error in trigger
CREATE TRIGGER t1_error BEFORE INSERT ON trigger_test FOR EACH ROW EXECUTE FUNCTION stmt_trig_error();

\set ON_ERROR_STOP 0
INSERT INTO trigger_test SELECT '2000-01-01',1,11;
COPY trigger_test FROM STDIN DELIMITER ',';
2000-01-01 01:00:00-05,1,12
\.
\set ON_ERROR_STOP 1

-- should not insert rows. count is 1
SELECT count(*) FROM trigger_test;

DROP TRIGGER t1_error ON trigger_test;

-- BEFORE STATEMENT trigger
CREATE TRIGGER t2 BEFORE INSERT ON trigger_test FOR EACH STATEMENT EXECUTE FUNCTION stmt_trig_info();

BEGIN;
INSERT INTO trigger_test SELECT '2000-01-01',1,0;
COPY trigger_test FROM STDIN DELIMITER ',';
2000-01-01 01:00:00-05,1,0
\.
-- should be 3
SELECT count(*) FROM trigger_test;
ROLLBACK;

DROP TRIGGER t2 ON trigger_test;

-- BEFORE STATEMENT error in trigger
CREATE TRIGGER t2_error BEFORE INSERT ON trigger_test FOR EACH STATEMENT EXECUTE FUNCTION stmt_trig_error();

\set ON_ERROR_STOP 0
INSERT INTO trigger_test SELECT '2000-01-01',1,11;
COPY trigger_test FROM STDIN DELIMITER ',';
2000-01-01 01:00:00-05,1,12
\.
\set ON_ERROR_STOP 1

-- should not insert rows. count is 1
SELECT count(*) FROM trigger_test;

DROP TRIGGER t2_error ON trigger_test;

-- AFTER STATEMENT trigger
CREATE TRIGGER t3 AFTER INSERT ON trigger_test FOR EACH STATEMENT EXECUTE FUNCTION stmt_trig_info();

BEGIN;
INSERT INTO trigger_test SELECT '2000-01-01',1,0;
COPY trigger_test FROM STDIN DELIMITER ',';
2000-01-01 01:00:00-05,1,0
\.
-- should be 3
SELECT count(*) FROM trigger_test;
ROLLBACK;

DROP TRIGGER t3 ON trigger_test;

-- AFTER STATEMENT error in trigger
CREATE TRIGGER t3_error AFTER INSERT ON trigger_test FOR EACH STATEMENT EXECUTE FUNCTION stmt_trig_error();

\set ON_ERROR_STOP 0
INSERT INTO trigger_test SELECT '2000-01-01',1,11;
COPY trigger_test FROM STDIN DELIMITER ',';
2000-01-01 01:00:00-05,1,12
\.
\set ON_ERROR_STOP 1

-- should not insert rows. count is 1
SELECT count(*) FROM trigger_test;

DROP TRIGGER t3_error ON trigger_test;

-- test unsupported features are blocked

-- INSTEAD OF INSERT is only supported for VIEWs
\set ON_ERROR_STOP 0
CREATE TRIGGER t4_instead INSTEAD OF INSERT ON trigger_test FOR EACH STATEMENT EXECUTE FUNCTION stmt_trig_info();
\set ON_ERROR_STOP 1

-- AFTER INSERT ROW trigger not supported on compressed chunk
CREATE TRIGGER t4_ar AFTER INSERT ON trigger_test FOR EACH ROW EXECUTE FUNCTION stmt_trig_info();

\set ON_ERROR_STOP 0
\set VERBOSITY default
INSERT INTO trigger_test SELECT '2000-01-01',1,0;
COPY trigger_test FROM STDIN DELIMITER ',';
2000-01-01 01:00:00-05,1,0
\.
\set VERBOSITY terse
\set ON_ERROR_STOP 1

-- should not insert rows. count is 1
SELECT count(*) FROM trigger_test;

BEGIN;
INSERT INTO trigger_test SELECT '2001-01-01',1,0;
COPY trigger_test FROM STDIN DELIMITER ',';
2001-01-01 01:00:00-05,1,0
\.
-- insert into new uncompressed chunk should not be blocked
SELECT count(*) FROM trigger_test;
ROLLBACK;

DROP TRIGGER t4_ar ON trigger_test;

-- CONSTRAINT trigger not supported on compressed chunk
CREATE CONSTRAINT TRIGGER t4_constraint AFTER INSERT ON trigger_test FOR EACH ROW EXECUTE FUNCTION stmt_trig_info();

\set ON_ERROR_STOP 0
INSERT INTO trigger_test SELECT '2000-01-01',1,0;
COPY trigger_test FROM STDIN DELIMITER ',';
2000-01-01 01:00:00-05,1,0
\.
\set ON_ERROR_STOP 1

-- should not insert rows. count is 1
SELECT count(*) FROM trigger_test;
DROP trigger t4_constraint ON trigger_test;

-- test row triggers after adding/dropping columns
-- now add a new column to the table and insert into a new chunk
ALTER TABLE trigger_test ADD COLUMN addcolv varchar(10);
ALTER TABLE trigger_test ADD COLUMN addcoli integer;
INSERT INTO trigger_test(time, device, value, addcolv, addcoli)
VALUES ( '2010-01-01', 10, 10, 'ten', 222);
SELECT compress_chunk(c, true) FROM show_chunks('trigger_test') c;

CREATE TRIGGER t1_mod BEFORE INSERT ON trigger_test FOR EACH ROW EXECUTE FUNCTION row_trig_value_mod();

SELECT count(*) FROM trigger_test;

BEGIN;
INSERT INTO trigger_test VALUES
                   ( '2000-01-01',1,11, 'eleven', 111),
                   ( '2010-01-01',10,10, 'ten', 222);
SELECT * FROM trigger_test ORDER BY 1 ,2, 3, 5;
ROLLBACK;

DROP TABLE trigger_test;

-- test interaction between newly inserted batches and pathkeys/ordered append
--
-- The following test operates on a small relation. By using the
-- timescaledb.enable_decompression_sorted_merge optimization, we are pushing a sort node
-- below the DecompressChunk node, which operates on the batches. This could lead to flaky
-- tests because the input data is small and PostgreSQL switches from IndexScans to
-- SequentialScans. Disable the optimization for the following test to ensure we have
-- stable query plans in all CI environments.
SET timescaledb.enable_decompression_sorted_merge = 0;

CREATE TABLE test_ordering(time int);
SELECT table_name FROM create_hypertable('test_ordering','time',chunk_time_interval:=100);
ALTER TABLE test_ordering SET (timescaledb.compress,timescaledb.compress_orderby='time desc');
INSERT INTO test_ordering VALUES (5),(4),(3);

-- should be ordered append
:PREFIX SELECT * FROM test_ordering ORDER BY 1;
SELECT compress_chunk(format('%I.%I',chunk_schema,chunk_name), true) FROM timescaledb_information.chunks WHERE hypertable_name = 'test_ordering';

-- should be ordered append
:PREFIX SELECT * FROM test_ordering ORDER BY 1;
INSERT INTO test_ordering SELECT 1;

-- should not be ordered append
-- regression introduced by #5599:
-- for the case of a single chunk, there is an additional redundant sort node below decompress chunk
-- this is due to pushing down the sort node below decompress chunk. In most cases that is beneficial,
-- but here a more optimal plan would have been a mergeAppend.
-- It was hard to include a path without pushed down sort for consideration, as `add_path` would reject
-- the path with sort pushdown, which is desirable in most cases
:PREFIX SELECT * FROM test_ordering ORDER BY 1;

INSERT INTO test_ordering VALUES (105),(104),(103);
-- should be ordered append
:PREFIX SELECT * FROM test_ordering ORDER BY 1;

--insert into compressed + uncompressed chunk
INSERT INTO test_ordering VALUES (21), (22),(113);
SELECT count(*) FROM test_ordering;
INSERT INTO test_ordering VALUES (106) RETURNING *;
-- insert into compressed chunk does support RETURNING
INSERT INTO test_ordering VALUES (23), (24), (115) RETURNING *;
INSERT INTO test_ordering VALUES (23), (24), (115) RETURNING tableoid::regclass, *;

SELECT compress_chunk(format('%I.%I',chunk_schema,chunk_name), true) FROM timescaledb_information.chunks WHERE hypertable_name = 'test_ordering';

-- should be ordered append
:PREFIX SELECT * FROM test_ordering ORDER BY 1;
SET timescaledb.enable_decompression_sorted_merge = 1;

-- TEST cagg triggers with insert into compressed chunk
CREATE TABLE conditions (
      timec        TIMESTAMPTZ       NOT NULL,
      temperature DOUBLE PRECISION  NULL,
      humidity    DOUBLE PRECISION  NULL
    );
SELECT table_name from create_hypertable( 'conditions', 'timec');
INSERT INTO conditions
SELECT generate_series('2010-01-01 09:00:00-08'::timestamptz, '2010-01-03 09:00:00-08'::timestamptz, '1 day'), 55 , 45;

CREATE MATERIALIZED VIEW cagg_conditions WITH (timescaledb.continuous,
   timescaledb.materialized_only = true)
AS
SELECT time_bucket( '7 days', timec) bkt, count(*) cnt, sum(temperature) sumb
FROM conditions
GROUP BY time_bucket('7 days', timec);

SELECT * FROM cagg_conditions ORDER BY 1;

ALTER TABLE conditions SET (timescaledb.compress);
SELECT  compress_chunk(ch) FROM show_chunks('conditions') ch;

SELECT chunk_name, range_start, range_end, is_compressed
FROM timescaledb_information.chunks
WHERE hypertable_name = 'conditions';

--now insert into compressed chunk
INSERT INTO conditions VALUES('2010-01-01 12:00:00-08', 10, 20);
INSERT INTO conditions VALUES('2010-01-01 12:00:00-08', 10, 20);

--refresh cagg, should have updated info
CALL refresh_continuous_aggregate('cagg_conditions', NULL, '2011-01-01 12:00:00-08' );
SELECT * FROM cagg_conditions ORDER BY 1;

-- TEST cagg triggers with copy into compressed chunk
COPY conditions FROM STDIN DELIMITER ',';
2010-01-01 11:16:00-05,100,100
2010-01-01 11:17:00-05,100,100
2010-01-01 11:18:00-05,100,100
\.

--refresh cagg, should have updated info
CALL refresh_continuous_aggregate('cagg_conditions', NULL, '2011-01-01 12:00:00-08' );
SELECT * FROM cagg_conditions ORDER BY 1;

-- TEST direct insert into internal compressed hypertable should be blocked
CREATE TABLE direct_insert(time timestamptz not null);
SELECT table_name FROM create_hypertable('direct_insert','time');
ALTER TABLE direct_insert SET(timescaledb.compress);

SELECT
  format('%I.%I', ht.schema_name, ht.table_name) AS "TABLENAME"
FROM
  _timescaledb_catalog.hypertable ht
  INNER JOIN _timescaledb_catalog.hypertable uncompress ON (ht.id = uncompress.compressed_hypertable_id
      AND uncompress.table_name = 'direct_insert') \gset

\set ON_ERROR_STOP 0
INSERT INTO :TABLENAME SELECT;
\set ON_ERROR_STOP 1

-- Test that inserting into a compressed table works even when the
-- column has been dropped.
CREATE TABLE test4 (
    timestamp timestamp without time zone not null,
    ident text not null,
    one double precision,
    two double precision
);

SELECT * FROM create_hypertable('test4', 'timestamp');

INSERT INTO test4 ( timestamp, ident ) VALUES ( '2021-10-14 17:50:16.207', '2' );
INSERT INTO test4 ( timestamp, ident ) VALUES ( '2021-11-14 17:50:16.207', '1' );
INSERT INTO test4 ( timestamp, ident ) VALUES ( '2021-12-14 17:50:16.207', '3' );
INSERT INTO test4 ( timestamp, ident ) VALUES ( '2022-01-14 17:50:16.207', '4' );
INSERT INTO test4 ( timestamp, ident ) VALUES ( '2022-02-14 17:50:16.207', '5' );
INSERT INTO test4 ( timestamp, ident ) VALUES ( '2022-03-14 17:50:16.207', '6' );
INSERT INTO test4 ( timestamp, ident ) VALUES ( '2022-04-14 17:50:16.207', '7' );

ALTER TABLE test4 SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'timestamp',
    timescaledb.compress_segmentby = 'ident'
);

select count(compress_chunk(ch)) FROM show_chunks('test4') ch;

ALTER TABLE test4 DROP COLUMN two;

INSERT INTO test4 VALUES ('2021-10-14 17:50:16.207', '7', NULL);
INSERT INTO test4 (timestamp, ident) VALUES ('2021-10-14 17:50:16.207', '7');

DROP TABLE test4;


-- Test COPY when trying to flush an empty buffer
-- In this case we send an empty slot used to
-- search for compressed tuples.

CREATE TABLE test_copy (
    timestamp int not null,
    id bigint
);

CREATE UNIQUE INDEX timestamp_id_idx ON test_copy(timestamp, id);

SELECT * FROM create_hypertable('test_copy', 'timestamp', chunk_time_interval=>10);

ALTER TABLE test_copy SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'timestamp',
    timescaledb.compress_segmentby = 'id'
);

INSERT INTO test_copy SELECT generate_series(1,25,1), -1;

SELECT count(compress_chunk(ch)) FROM show_chunks('test_copy') ch;

\copy test_copy FROM data/copy_data.csv WITH CSV HEADER;

-- Also test the code path where the chunk insert state goes out of cache.
set timescaledb.max_open_chunks_per_insert = 1;

truncate table test_copy;

INSERT INTO test_copy SELECT generate_series(1,25,1), -1;

SELECT count(compress_chunk(ch)) FROM show_chunks('test_copy') ch;

\copy test_copy FROM data/copy_data.csv WITH CSV HEADER;

reset timescaledb.max_open_chunks_per_insert;

DROP TABLE test_copy;

-- Text limitting decompressed tuple during an insert
CREATE TABLE test_limit (
    timestamp int not null,
    id bigint
);
SELECT * FROM create_hypertable('test_limit', 'timestamp', chunk_time_interval=>1000);
INSERT INTO test_limit SELECT t, i FROM generate_series(1,10000,1) t CROSS JOIN generate_series(1,3,1) i;
CREATE UNIQUE INDEX timestamp_id_idx ON test_limit(timestamp, id);

ALTER TABLE test_limit SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = '',
    timescaledb.compress_orderby = 'timestamp'
);
SELECT count(compress_chunk(ch)) FROM show_chunks('test_limit') ch;

SET timescaledb.max_tuples_decompressed_per_dml_transaction = 5000;
\set VERBOSITY default
\set ON_ERROR_STOP 0
-- Inserting in the same period should decompress tuples
INSERT INTO test_limit SELECT t, 11 FROM generate_series(1,6000,1000) t;
-- Setting to 0 should remove the limit.
SET timescaledb.max_tuples_decompressed_per_dml_transaction = 0;
INSERT INTO test_limit SELECT t, 11 FROM generate_series(1,6000,1000) t;
\set ON_ERROR_STOP 1

DROP TABLE test_limit;
RESET timescaledb.max_tuples_decompressed_per_dml_transaction;

-- test multiple unique constraints
CREATE TABLE multi_unique (time timestamptz NOT NULL, u1 int, u2 int, value float, unique(time, u1), unique(time, u2));
SELECT table_name FROM create_hypertable('multi_unique', 'time');
ALTER TABLE multi_unique SET (timescaledb.compress, timescaledb.compress_segmentby = 'u1, u2');

INSERT INTO multi_unique VALUES('2024-01-01', 0, 0, 1.0);
SELECT count(compress_chunk(c)) FROM show_chunks('multi_unique') c;

\set ON_ERROR_STOP 0
-- all INSERTS should fail with constraint violation
BEGIN; INSERT INTO multi_unique VALUES('2024-01-01', 0, 0, 1.0); ROLLBACK;
BEGIN; INSERT INTO multi_unique VALUES('2024-01-01', 0, 1, 1.0); ROLLBACK;
BEGIN; INSERT INTO multi_unique VALUES('2024-01-01', 1, 0, 1.0); ROLLBACK;
\set ON_ERROR_STOP 1

DROP TABLE multi_unique;

