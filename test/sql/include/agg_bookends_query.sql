-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- canary for results diff
-- this should be only output of results diff

SELECT setting, current_setting(setting) AS value from (VALUES ('timescaledb.enable_optimizations')) v(setting);


:PREFIX SELECT time, gp, temp FROM btest ORDER BY time;

:PREFIX SELECT last(temp, time) FROM btest;
:PREFIX SELECT first(temp, time) FROM btest;
:PREFIX SELECT last(temp, time_alt) FROM btest;
:PREFIX SELECT first(temp, time_alt) FROM btest;

:PREFIX SELECT gp, last(temp, time) FROM btest GROUP BY gp ORDER BY gp;
:PREFIX SELECT gp, first(temp, time) FROM btest GROUP BY gp ORDER BY gp;

--check whole row
:PREFIX SELECT gp, first(btest, time) FROM btest GROUP BY gp ORDER BY gp;

--check toasted col
:PREFIX SELECT gp, left(last(strid, time), 10) FROM btest GROUP BY gp ORDER BY gp;
:PREFIX SELECT gp, last(temp, strid) FROM btest GROUP BY gp ORDER BY gp;
:PREFIX SELECT gp, last(strid, temp) FROM btest GROUP BY gp ORDER BY gp;

BEGIN;

--check null value as last element
INSERT INTO btest VALUES('2018-01-20T09:00:43', '2017-01-20T09:00:55', 2, NULL);
:PREFIX SELECT last(temp, time) FROM btest;
--check non-null element "overrides" NULL because it comes after.
INSERT INTO btest VALUES('2019-01-20T09:00:43', '2018-01-20T09:00:55', 2, 30.5);
:PREFIX SELECT last(temp, time) FROM btest;

--check null cmp element is skipped
INSERT INTO btest VALUES('2018-01-20T09:00:43', NULL, 2, 32.3);
:PREFIX SELECT last(temp, time_alt) FROM btest;
-- fist returns NULL value
:PREFIX SELECT first(temp, time_alt) FROM btest;
-- test first return non NULL value
INSERT INTO btest VALUES('2016-01-20T09:00:00', '2016-01-20T09:00:00', 2, 36.5);
:PREFIX SELECT first(temp, time_alt) FROM btest;
--check non null cmp element insert after null cmp
INSERT INTO btest VALUES('2020-01-20T09:00:43', '2020-01-20T09:00:43', 2, 35.3);
:PREFIX SELECT last(temp, time_alt) FROM btest;
:PREFIX SELECT first(temp, time_alt) FROM btest;
--cmp nulls should be ignored and not present in groups
:PREFIX SELECT gp, last(temp, time_alt) FROM btest GROUP BY gp ORDER BY gp;

--Previously, some bugs were found with NULLS and numeric types, so test that
INSERT INTO btest_numeric VALUES ('2019-01-20T09:00:43', NULL);

:PREFIX SELECT last(quantity, time) FROM btest_numeric;

--check non-null element "overrides" NULL because it comes after.
INSERT INTO btest_numeric VALUES('2020-01-20T09:00:43', 30.5);
:PREFIX SELECT last(quantity, time) FROM btest_numeric;

-- do index scan for last
:PREFIX SELECT last(temp, time) FROM btest;

-- do index scan for first
:PREFIX SELECT first(temp, time) FROM btest;

-- can't do index scan when ordering on non-index column
:PREFIX SELECT first(temp, time_alt) FROM btest;

-- do index scan for subquery
:PREFIX SELECT * FROM (SELECT last(temp, time) FROM btest) last;

-- can't do index scan when using group by
:PREFIX SELECT last(temp, time) FROM btest GROUP BY gp ORDER BY gp;

-- do index scan when agg function is used in CTE subquery
:PREFIX WITH last_temp AS (SELECT last(temp, time) FROM btest) SELECT * from last_temp;

-- do index scan when using both FIRST and LAST aggregate functions
:PREFIX SELECT first(temp, time), last(temp, time) FROM btest;

-- verify results when using both FIRST and LAST
:PREFIX SELECT first(temp, time), last(temp, time) FROM btest;

-- do index scan when using WHERE
:PREFIX SELECT last(temp, time) FROM btest WHERE time <= '2017-01-20T09:00:02';

-- can't do index scan for MAX and LAST combined (MinMax optimization fails when having different aggregate functions)
:PREFIX SELECT max(time), last(temp, time) FROM btest;

-- can't do index scan when using FIRST/LAST in ORDER BY
:PREFIX SELECT last(temp, time) FROM btest ORDER BY last(temp, time);

-- do index scan
:PREFIX SELECT last(temp, time) FROM btest WHERE temp < 30;

-- SELECT first(temp, time) FROM btest WHERE time >= '2017-01-20 09:00:47';

-- do index scan
:PREFIX SELECT first(temp, time) FROM btest WHERE time >= '2017-01-20 09:00:47';

-- can't do index scan when using WINDOW function
:PREFIX SELECT gp, last(temp, time) OVER (PARTITION BY gp) AS last FROM btest;

-- test constants
:PREFIX SELECT first(100, 100) FROM btest;

-- create an index so we can test optimization
CREATE INDEX btest_time_alt_idx ON btest(time_alt);
:PREFIX SELECT last(temp, time_alt) FROM btest;

--test nested FIRST/LAST - should optimize
:PREFIX SELECT abs(last(temp, time)) FROM btest;

-- test nested FIRST/LAST in ORDER BY - no optimization possible
:PREFIX SELECT abs(last(temp, time)) FROM btest ORDER BY abs(last(temp,time));

ROLLBACK;

