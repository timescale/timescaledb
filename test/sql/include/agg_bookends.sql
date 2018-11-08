-- Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
--
-- This file is licensed under the Apache License,
-- see LICENSE-APACHE at the top level directory.

CREATE TABLE "btest"(time timestamp, time_alt timestamp, gp INTEGER, temp float, strid TEXT DEFAULT 'testing');
SELECT 1 AS hypertable_created FROM (SELECT create_hypertable('"btest"', 'time')) t;
INSERT INTO "btest" VALUES('2017-01-20T09:00:01', '2017-01-20T10:00:00', 1, 22.5);
INSERT INTO "btest" VALUES('2017-01-20T09:00:21', '2017-01-20T09:00:59', 1, 21.2);
INSERT INTO "btest" VALUES('2017-01-20T09:00:47', '2017-01-20T09:00:58', 1, 25.1);
INSERT INTO "btest" VALUES('2017-01-20T09:00:02', '2017-01-20T09:00:57', 2, 35.5);
INSERT INTO "btest" VALUES('2017-01-20T09:00:21', '2017-01-20T09:00:56', 2, 30.2);
--TOASTED;
INSERT INTO "btest" VALUES('2017-01-20T09:00:43', '2017-01-20T09:01:55', 2, 20.1, repeat('xyz', 1000000) );

:PREFIX SELECT time, gp, temp FROM btest ORDER BY time;

:PREFIX SELECT last(temp, time) FROM "btest";
:PREFIX SELECT first(temp, time) FROM "btest";
:PREFIX SELECT last(temp, time_alt) FROM "btest";
:PREFIX SELECT first(temp, time_alt) FROM "btest";


:PREFIX SELECT gp, last(temp, time) FROM "btest" GROUP BY gp ORDER BY gp;
:PREFIX SELECT gp, first(temp, time) FROM "btest" GROUP BY gp ORDER BY gp;

--check whole row
:PREFIX SELECT gp, first("btest", time) FROM "btest" GROUP BY gp ORDER BY gp;

--check toasted col
:PREFIX SELECT gp, left(last(strid, time), 10) FROM "btest" GROUP BY gp ORDER BY gp;
:PREFIX SELECT gp, last(temp, strid) FROM "btest" GROUP BY gp ORDER BY gp;

--check null value as last element
INSERT INTO "btest" VALUES('2018-01-20T09:00:43', '2017-01-20T09:00:55', 2, NULL);
:PREFIX SELECT last(temp, time) FROM "btest";
--check non-null element "overrides" NULL because it comes after.
INSERT INTO "btest" VALUES('2019-01-20T09:00:43', '2018-01-20T09:00:55', 2, 30.5);
:PREFIX SELECT last(temp, time) FROM "btest";

--check null cmp element is skipped
INSERT INTO "btest" VALUES('2018-01-20T09:00:43', NULL, 2, 32.3);
:PREFIX SELECT last(temp, time_alt) FROM "btest";
-- fist returns NULL value 
:PREFIX SELECT first(temp, time_alt) FROM "btest";
-- test first return non NULL value
INSERT INTO "btest" VALUES('2016-01-20T09:00:00', '2016-01-20T09:00:00', 2, 36.5);
:PREFIX SELECT first(temp, time_alt) FROM "btest";
--check non null cmp element insert after null cmp  
INSERT INTO "btest" VALUES('2020-01-20T09:00:43', '2020-01-20T09:00:43', 2, 35.3);
:PREFIX SELECT last(temp, time_alt) FROM "btest";
:PREFIX SELECT first(temp, time_alt) FROM "btest";
--cmp nulls should be ignored and not present in groups
:PREFIX SELECT gp, last(temp, time_alt) FROM "btest" GROUP BY gp ORDER BY gp;


--Previously, some bugs were found with NULLS and numeric types, so test that
CREATE TABLE btest_numeric
(
    time timestamp,
    quantity numeric
);

SELECT 1 AS hypertable_created FROM (SELECT create_hypertable('btest_numeric', 'time')) t;

-- Insert rows, with rows that contain NULL values
INSERT INTO btest_numeric VALUES
    ('2019-01-20T09:00:43', NULL);
:PREFIX SELECT last(quantity, time) FROM btest_numeric;

--check non-null element "overrides" NULL because it comes after.
INSERT INTO btest_numeric VALUES('2020-01-20T09:00:43', 30.5);
:PREFIX SELECT last(quantity, time) FROM btest_numeric;

-- do index scan for last
:PREFIX SELECT last(temp, time) FROM "btest"; 

-- do index scan for first
:PREFIX SELECT first(temp, time) FROM "btest";

-- can't do index scan when ordering on non-index column
:PREFIX SELECT first(temp, time_alt) FROM "btest"; 

-- do index scan for subquery
:PREFIX SELECT * FROM (SELECT last(temp, time) FROM "btest") last;

-- can't do index scan when using group by
:PREFIX SELECT last(temp, time) FROM "btest" GROUP BY gp ORDER BY gp;

-- do index scan when agg function is used in CTE subquery
:PREFIX WITH last_temp AS (SELECT last(temp, time) FROM "btest") SELECT * from last_temp;

-- do index scan when using both FIRST and LAST aggregate functions
:PREFIX SELECT first(temp, time), last(temp, time) FROM "btest";

-- verify results when using both FIRST and LAST
:PREFIX SELECT first(temp, time), last(temp, time) FROM "btest";

-- do index scan when using WHERE
:PREFIX SELECT last(temp, time) FROM "btest" WHERE time <= '2017-01-20T09:00:02';

-- can't do index scan for MAX and LAST combined (MinMax optimization fails when having different aggregate functions)
:PREFIX SELECT max(time), last(temp, time) FROM "btest";

-- can't do index scan when using FIRST/LAST in ORDER BY
:PREFIX SELECT last(temp, time) FROM "btest" ORDER BY last(temp, time);

-- SELECT last(temp, time) FROM "btest" WHERE temp < 30;

-- do index scan
:PREFIX SELECT last(temp, time) FROM "btest" WHERE temp < 30;

-- SELECT first(temp, time) FROM "btest" WHERE time >= '2017-01-20 09:00:47';

-- do index scan
:PREFIX SELECT first(temp, time) FROM "btest" WHERE time >= '2017-01-20 09:00:47';

-- SELECT gp, last(temp, time) OVER (PARTITION BY gp) AS last FROM "btest";

-- can't do index scan when using WINDOW function
:PREFIX SELECT gp, last(temp, time) OVER (PARTITION BY gp) AS last FROM "btest";

-- test constants
:PREFIX SELECT first(100, 100) FROM "btest";

-- create an index so we can test optimization
CREATE INDEX btest_time_alt_idx ON btest(time_alt);
SET enable_seqscan=OFF;
:PREFIX SELECT last(temp, time_alt) FROM "btest";

--test nested FIRST/LAST - should optimize
:PREFIX SELECT abs(last(temp, time)) FROM "btest";

-- test nested FIRST/LAST in ORDER BY - no optimization possible
:PREFIX SELECT abs(last(temp, time)) FROM "btest" ORDER BY abs(last(temp,time));


