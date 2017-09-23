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


SELECT gp, last(temp, time) FROM "btest" GROUP BY gp ORDER BY gp;
SELECT gp, first(temp, time) FROM "btest" GROUP BY gp ORDER BY gp;

--check whole row
SELECT gp, first("btest", time) FROM "btest" GROUP BY gp ORDER BY gp;

--check toasted col
SELECT gp, left(last(strid, time), 10) FROM "btest" GROUP BY gp ORDER BY gp;
SELECT gp, last(temp, strid) FROM "btest" GROUP BY gp ORDER BY gp;

--check null value as last element
INSERT INTO "btest" VALUES('2018-01-20T09:00:43', '2017-01-20T09:00:55', 2, NULL);
SELECT last(temp, time) FROM "btest";
--check non-null element "overrides" NULL because it comes after.
INSERT INTO "btest" VALUES('2019-01-20T09:00:43', '2017-01-20T09:00:55', 2, 30.5);
SELECT last(temp, time) FROM "btest";

--check null cmp elements
INSERT INTO "btest" VALUES('2018-01-20T09:00:43', NULL, 2, 32.3);
SELECT last(temp, time_alt) FROM "btest";
--no overriding a cmp NULL
INSERT INTO "btest" VALUES('2020-01-20T09:00:43', '2020-01-20T09:00:43', 2, 35.3);
SELECT last(temp, time_alt) FROM "btest";
--cmp nulls make the group NULL but don't interfere with other groups
SELECT gp, last(temp, time_alt) FROM "btest" GROUP BY gp ORDER BY gp;


--Previously, some bugs were found with NULLS and numeric types, so test that
CREATE TABLE btest_numeric
(
    time timestamp,
    quantity numeric
);

SELECT create_hypertable('btest_numeric', 'time');

-- Insert rows, with rows that contain NULL values
INSERT INTO btest_numeric VALUES
    ('2019-01-20T09:00:43', NULL);
SELECT last(quantity, time) FROM btest_numeric;

--check non-null element "overrides" NULL because it comes after.
INSERT INTO btest_numeric VALUES('2020-01-20T09:00:43', 30.5);
SELECT last(quantity, time) FROM btest_numeric;
