-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- table 1
CREATE TABLE "hitest1"(key real, val varchar(40));

-- insertions
INSERT INTO "hitest1" VALUES(0, 'hi');
INSERT INTO "hitest1" VALUES(1, 'sup');
INSERT INTO "hitest1" VALUES(2, 'hello');
INSERT INTO "hitest1" VALUES(3, 'yo');
INSERT INTO "hitest1" VALUES(4, 'howdy');
INSERT INTO "hitest1" VALUES(5, 'hola');
INSERT INTO "hitest1" VALUES(6, 'ya');
INSERT INTO "hitest1" VALUES(1, 'sup');
INSERT INTO "hitest1" VALUES(2, 'hello');
INSERT INTO "hitest1" VALUES(1, 'sup');

-- table 2
CREATE TABLE "hitest2"(name varchar(30), score integer, qualify boolean);

-- insertions
INSERT INTO "hitest2" VALUES('Tom', 6, TRUE);
INSERT INTO "hitest2" VALUES('Mary', 4, FALSE);
INSERT INTO "hitest2" VALUES('Jaq', 3, FALSE);
INSERT INTO "hitest2" VALUES('Jane', 10, TRUE);

-- standard 2 bucket
SELECT histogram(key, 0, 9, 2) FROM hitest1;
-- standard multi-bucket
SELECT histogram(key, 0, 9, 5) FROM hitest1;
-- standard 3 bucket
SELECT val, histogram(key, 0, 7, 3) FROM hitest1 GROUP BY val ORDER BY val;
-- standard element beneath lb
SELECT histogram(key, 1, 7, 3) FROM hitest1;
-- standard element above ub
SELECT histogram(key, 0, 3, 3) FROM hitest1;
-- standard element beneath and above lb and ub, respectively
SELECT histogram(key, 1, 3, 2) FROM hitest1;

-- standard 1 bucket
SELECT histogram(key, 1, 3, 1) FROM hitest1;

-- standard 2 bucket
SELECT qualify, histogram(score, 0, 10, 2) FROM hitest2 GROUP BY qualify ORDER BY qualify;
-- standard multi-bucket
SELECT qualify, histogram(score, 0, 10, 5) FROM hitest2 GROUP BY qualify ORDER BY qualify;

-- check number of buckets is constant
\set ON_ERROR_STOP 0
select histogram(i,10,90,case when i=1 then 1 else 1000000 end) FROM generate_series(1,100) i;
\set ON_ERROR_STOP 1

CREATE TABLE weather (
       time TIMESTAMPTZ NOT NULL,
       city TEXT,
       temperature FLOAT,
       PRIMARY KEY(time, city)
);

-- There is a bug in width_bucket() causing a NaN as a result, so we
-- check that it is not causing a crash in histogram().
SELECT * FROM create_hypertable('weather', 'time', 'city', 3);
INSERT INTO weather VALUES
       ('2023-02-10 09:16:51.133584+00','city1',10.4),
       ('2023-02-10 11:16:51.611618+00','city1',10.3),
       ('2023-02-10 06:58:59.999999+00','city1',10.3),
       ('2023-02-10 01:58:59.999999+00','city1',10.3),
       ('2023-02-09 01:58:59.999999+00','city1',10.3),
       ('2023-02-10 08:58:59.999999+00','city1',10.3),
       ('2023-03-23 06:12:02.73765+00 ','city1', 9.7),
       ('2023-03-23 06:12:06.990998+00','city1',11.7);

-- This will currently generate an error.
\set ON_ERROR_STOP 0
SELECT histogram(temperature, -1.79769e+308, 1.79769e+308,10) FROM weather GROUP BY city;
\set ON_ERROR_STOP 1
