\ir include/create_single_db.sql


CREATE TABLE "btest"(time timestamp, time_alt timestamp, gp INTEGER, temp float, strid TEXT DEFAULT 'testing');
SELECT create_hypertable('"btest"', 'time');
INSERT INTO "btest" VALUES('2017-01-20T09:00:01', '2017-01-20T10:00:00', 1, 22.5);
INSERT INTO "btest" VALUES('2017-01-20T09:00:21', '2017-01-20T09:00:59', 1, 21.2);
INSERT INTO "btest" VALUES('2017-01-20T09:00:47', '2017-01-20T09:00:58', 1, 25.1);
INSERT INTO "btest" VALUES('2017-01-20T09:00:02', '2017-01-20T09:00:57', 2, 35.5);
INSERT INTO "btest" VALUES('2017-01-20T09:00:21', '2017-01-20T09:00:56', 2, 30.2);
--TOASTED;
INSERT INTO "btest" VALUES('2017-01-20T09:00:43', '2017-01-20T09:00:55', 2, 20.1, repeat('xyz', 1000000) );

SELECT time, gp, temp FROM btest ORDER BY time;

SELECT last(temp, time) FROM "btest";
SELECT first(temp, time) FROM "btest";
SELECT last(temp, time_alt) FROM "btest";
SELECT first(temp, time_alt) FROM "btest";


SELECT gp, last(temp, time) FROM "btest" GROUP BY gp;
SELECT gp, first(temp, time) FROM "btest" GROUP BY gp;
SELECT gp, first("btest", time) FROM "btest" GROUP BY gp;

--check toasted col
SELECT gp, left(last(strid, time), 10) FROM "btest" GROUP BY gp;


CREATE TABLE test (i int, j double precision);
--has to be big enough to force at least 2 workers below.
INSERT INTO test SELECT x, x+0.1 FROM generate_series(1,1000000) AS x;

SET force_parallel_mode = 'on';
SET max_parallel_workers_per_gather = 4;

EXPLAIN SELECT first(i, j) FROM "test";
SELECT first(i, j) FROM "test";
EXPLAIN SELECT last(i, j) FROM "test";
SELECT last(i, j) FROM "test";



