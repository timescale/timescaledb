-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\ir include/insert_two_partitions.sql

SELECT * FROM test.show_columnsp('_timescaledb_internal.%_hyper%');
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%');
SELECT * FROM _timescaledb_catalog.chunk;

SELECT * FROM "two_Partitions" ORDER BY "timeCustom", device_id, series_0, series_1;
SELECT * FROM ONLY "two_Partitions";

CREATE TABLE error_test(time timestamp, temp float8, device text NOT NULL);
SELECT create_hypertable('error_test', 'time', 'device', 2);

\set QUIET off
INSERT INTO error_test VALUES ('Mon Mar 20 09:18:20.1 2017', 21.3, 'dev1');
\set ON_ERROR_STOP 0
-- generate insert error
INSERT INTO error_test VALUES ('Mon Mar 20 09:18:22.3 2017', 21.1, NULL);
\set ON_ERROR_STOP 1
INSERT INTO error_test VALUES ('Mon Mar 20 09:18:25.7 2017', 22.4, 'dev2');
\set QUIET on
SELECT * FROM error_test;

--test character(9) partition keys since there were issues with padding causing partitioning errors
CREATE TABLE tick_character (
    symbol      character(9) NOT NULL,
    mid       REAL NOT NULL,
    spread      REAL NOT NULL,
    time        TIMESTAMPTZ       NOT NULL
);

SELECT create_hypertable ('tick_character', 'time', 'symbol', 2);
INSERT INTO tick_character ( symbol, mid, spread, time ) VALUES ( 'GBPJPY', 142.639000, 5.80, 'Mon Mar 20 09:18:22.3 2017') RETURNING time, symbol, mid;
SELECT * FROM tick_character;

CREATE TABLE  date_col_test(time date, temp float8, device text NOT NULL);
SELECT create_hypertable('date_col_test', 'time', 'device', 1000, chunk_time_interval => INTERVAL '1 Day');
INSERT INTO date_col_test
VALUES ('2001-02-01', 98, 'dev1'),
('2001-03-02', 98, 'dev1');

SELECT * FROM date_col_test WHERE time > '2001-01-01';

-- Out-of-order insertion regression test.
-- this used to trip an assert in subspace_store.c checking that
-- max_open_chunks_per_insert was obeyed
set timescaledb.max_open_chunks_per_insert=1;

CREATE TABLE chunk_assert_fail(i bigint, j bigint);
SELECT create_hypertable('chunk_assert_fail', 'i', 'j', 1000, chunk_time_interval=>1);
insert into chunk_assert_fail values (1, 1), (1, 2), (2,1);
select * from chunk_assert_fail;

CREATE TABLE one_space_test(time timestamp, temp float8, device text NOT NULL);
SELECT create_hypertable('one_space_test', 'time', 'device', 1);
INSERT INTO one_space_test VALUES
('2001-01-01 01:01:01', 1.0, 'device'),
('2002-01-01 01:02:01', 1.0, 'device');
SELECT * FROM one_space_test;

--CTE & EXPLAIN ANALYZE TESTS
WITH insert_cte as (
	INSERT INTO one_space_test VALUES
		('2001-01-01 01:02:01', 1.0, 'device')
	RETURNING *)
SELECT * FROM insert_cte;

EXPLAIN (analyze, costs off, timing off) --can't turn summary off in 9.6 so instead grep it away at end.
WITH insert_cte as (
	INSERT INTO one_space_test VALUES
		('2001-01-01 01:03:01', 1.0, 'device')
	)
SELECT 1 \g | grep -v "Planning" | grep -v "Execution"

-- INSERTs can exclude chunks based on constraints
EXPLAIN (costs off) INSERT INTO chunk_assert_fail SELECT i, j FROM chunk_assert_fail;
EXPLAIN (costs off) INSERT INTO chunk_assert_fail SELECT i, j FROM chunk_assert_fail WHERE i < 1;
EXPLAIN (costs off) INSERT INTO chunk_assert_fail SELECT i, j FROM chunk_assert_fail WHERE i = 1;
EXPLAIN (costs off) INSERT INTO chunk_assert_fail SELECT i, j FROM chunk_assert_fail WHERE i > 1;
INSERT INTO chunk_assert_fail SELECT i, j FROM chunk_assert_fail WHERE i > 1;

EXPLAIN (costs off) INSERT INTO one_space_test SELECT * FROM one_space_test WHERE time < 'infinity' LIMIT 1;
EXPLAIN (costs off) INSERT INTO one_space_test SELECT * FROM one_space_test WHERE time >= 'infinity' LIMIT 1;
EXPLAIN (costs off) INSERT INTO one_space_test SELECT * FROM one_space_test WHERE time <= '-infinity' LIMIT 1;
EXPLAIN (costs off) INSERT INTO one_space_test SELECT * FROM one_space_test WHERE time > '-infinity' LIMIT 1;
INSERT INTO one_space_test SELECT * FROM one_space_test WHERE time < 'infinity' LIMIT 1;
INSERT INTO one_space_test SELECT * FROM one_space_test WHERE time >= 'infinity' LIMIT 1;
INSERT INTO one_space_test SELECT * FROM one_space_test WHERE time <= '-infinity' LIMIT 1;
INSERT INTO one_space_test SELECT * FROM one_space_test WHERE time > '-infinity' LIMIT 1;

CREATE TABLE timestamp_inf(time TIMESTAMP);
SELECT create_hypertable('timestamp_inf', 'time');

INSERT INTO timestamp_inf VALUES ('2018/01/02'), ('2019/01/02');

EXPLAIN (costs off) INSERT INTO timestamp_inf SELECT * FROM timestamp_inf
    WHERE time < 'infinity' LIMIT 1;
EXPLAIN (costs off) INSERT INTO timestamp_inf SELECT * FROM timestamp_inf
    WHERE time >= 'infinity' LIMIT 1;
EXPLAIN (costs off) INSERT INTO timestamp_inf SELECT * FROM timestamp_inf
    WHERE time <= '-infinity' LIMIT 1;
EXPLAIN (costs off) INSERT INTO timestamp_inf SELECT * FROM timestamp_inf
    WHERE time > '-infinity' LIMIT 1;

CREATE TABLE date_inf(time DATE);
SELECT create_hypertable('date_inf', 'time');

INSERT INTO date_inf VALUES ('2018/01/02'), ('2019/01/02');

EXPLAIN (costs off) INSERT INTO date_inf SELECT * FROM date_inf
    WHERE time < 'infinity' LIMIT 1;
EXPLAIN (costs off) INSERT INTO date_inf SELECT * FROM date_inf
    WHERE time >= 'infinity' LIMIT 1;
EXPLAIN (costs off) INSERT INTO date_inf SELECT * FROM date_inf
    WHERE time <= '-infinity' LIMIT 1;
EXPLAIN (costs off) INSERT INTO date_inf SELECT * FROM date_inf
    WHERE time > '-infinity' LIMIT 1;

-- test INSERT with cached plans / plpgsql functions
-- https://github.com/timescale/timescaledb/issues/1809
CREATE TABLE status_table(a int, b int, last_ts timestamptz, UNIQUE(a,b));
CREATE TABLE metrics(time timestamptz NOT NULL, value float);
CREATE TABLE metrics2(time timestamptz NOT NULL, value float);
SELECT (create_hypertable(t,'time')).table_name FROM (VALUES ('metrics'),('metrics2')) v(t);

INSERT INTO metrics VALUES ('2000-01-01',random()), ('2000-02-01',random()), ('2000-03-01',random());

CREATE OR REPLACE FUNCTION insert_test() RETURNS VOID LANGUAGE plpgsql AS
$$
  DECLARE
    r RECORD;
  BEGIN
    FOR r IN
      SELECT * FROM metrics
    LOOP
      WITH foo AS (
        INSERT INTO metrics2 SELECT * FROM metrics RETURNING *
      )
      INSERT INTO status_table (a,b, last_ts)
        VALUES (1,1, now())
        ON CONFLICT (a,b) DO UPDATE SET last_ts=(SELECT max(time) FROM metrics);
    END LOOP;
  END;
$$;

SELECT insert_test(), insert_test(), insert_test();

-- test Postgres crashes on INSERT ... SELECT ... WHERE NOT EXISTS with empty table
-- https://github.com/timescale/timescaledb/issues/1883
CREATE TABLE readings (
	toe TIMESTAMPTZ NOT NULL,
	sensor_id INT NOT NULL,
	value INT NOT NULL
);
SELECT create_hypertable(
	'readings',
	'toe',
	chunk_time_interval => interval '1 day',
	if_not_exists => TRUE,
	migrate_data => TRUE
);
EXPLAIN (costs off)
INSERT INTO readings
SELECT '2020-05-09 10:34:35.296288+00', 1, 0
WHERE NOT EXISTS (
	SELECT 1
	FROM readings
	WHERE sensor_id = 1
		AND toe = '2020-05-09 10:34:35.296288+00'
);
INSERT INTO readings
SELECT '2020-05-09 10:34:35.296288+00', 1, 0
WHERE NOT EXISTS (
	SELECT 1
	FROM readings
	WHERE sensor_id = 1
		AND toe = '2020-05-09 10:34:35.296288+00'
);
DROP TABLE readings;
CREATE TABLE sample_table (
       sequence INTEGER NOT NULL,
       time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
       value NUMERIC NOT NULL,
       UNIQUE (sequence, time)
);
SELECT * FROM create_hypertable('sample_table', 'time',
       chunk_time_interval => INTERVAL '1 day');
INSERT INTO sample_table (sequence,time,value) VALUES
       (7, generate_series(TIMESTAMP '2019-08-01', TIMESTAMP '2019-08-10', INTERVAL '10 minutes'), ROUND(RANDOM()*10)::int);
\set ON_ERROR_STOP 0
INSERT INTO sample_table (sequence,time,value) VALUES
       (7, generate_series(TIMESTAMP '2019-07-21', TIMESTAMP '2019-08-01', INTERVAL '10 minutes'), ROUND(RANDOM()*10)::int);
\set ON_ERROR_STOP 1
INSERT INTO sample_table (sequence,time,value) VALUES
       (7,generate_series(TIMESTAMP '2019-01-01', TIMESTAMP '2019-07-01', '10 minutes'), ROUND(RANDOM()*10)::int);

DROP TABLE sample_table;

-- test on conflict clause on columns with default value
-- issue #3037
CREATE TABLE i3037(time timestamptz PRIMARY KEY);
SELECT create_hypertable('i3037','time');
ALTER TABLE i3037 ADD COLUMN value float DEFAULT 0;

INSERT INTO i3037 VALUES ('2000-01-01');
INSERT INTO i3037 VALUES ('2000-01-01') ON CONFLICT(time) DO UPDATE SET value = EXCLUDED.value;

