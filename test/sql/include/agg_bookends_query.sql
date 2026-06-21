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

-- do index scan when using FIRST/LAST in HAVING
:PREFIX SELECT last(temp, time) FROM btest HAVING last(temp, time) IS NOT NULL;
:PREFIX SELECT first(temp, time) FROM btest HAVING first(temp, time) > 0;

-- HAVING references a FIRST/LAST aggregate not present in the target list
:PREFIX SELECT first(temp, time) FROM btest HAVING last(temp, time) IS NOT NULL;

-- DISTINCT in HAVING while the target list has the same aggregate without DISTINCT
:PREFIX SELECT first(temp, time) FROM btest HAVING first(DISTINCT temp, time) > 0;
:PREFIX SELECT last(temp, time) FROM btest HAVING last(DISTINCT temp, time) IS NOT NULL;

-- do index scan
:PREFIX SELECT last(temp, time) FROM btest WHERE temp < 30;

-- SELECT first(temp, time) FROM btest WHERE time >= '2017-01-20 09:00:47';

-- do index scan
:PREFIX SELECT first(temp, time) FROM btest WHERE time >= '2017-01-20 09:00:47';

-- can't do index scan when using WINDOW function
:PREFIX SELECT gp, last(temp, time) OVER (PARTITION BY gp) AS last FROM btest;

-- parent-only scan
:PREFIX SELECT first(temp, time) FROM ONLY btest;
:PREFIX SELECT last(temp, time) FROM ONLY btest;

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

-- two FIRST/LAST sharing the value column but ordering on different columns
BEGIN;
CREATE TABLE bookend_two_orderings(time timestamptz NOT NULL, time_alt timestamptz NOT NULL, val int NOT NULL);
SELECT schema_name, table_name, created FROM create_hypertable('bookend_two_orderings', 'time');
INSERT INTO bookend_two_orderings
SELECT '2025-01-01'::timestamptz + g * interval '1 minute',
       '2025-01-01'::timestamptz + (201 - g) * interval '1 minute',
       g
FROM generate_series(1, 200) g;
CREATE INDEX ON bookend_two_orderings(time);
CREATE INDEX ON bookend_two_orderings(time_alt);
:PREFIX SELECT first(val, time), first(val, time_alt) FROM bookend_two_orderings;
:PREFIX SELECT last(val, time), last(val, time_alt) FROM bookend_two_orderings;
-- DISTINCT sorts on the first/last results, so the optimization must be skipped
SET enable_hashagg = off;
:PREFIX SELECT DISTINCT first(val, time), first(val, time_alt) FROM bookend_two_orderings;
RESET enable_hashagg;
ROLLBACK;

-- Test with NULL numeric values
BEGIN;
TRUNCATE btest_numeric;

-- Empty table
:PREFIX SELECT first(btest_numeric, time) FROM btest_numeric;
:PREFIX SELECT last(btest_numeric, time) FROM btest_numeric;

-- Only NULL values
INSERT INTO btest_numeric VALUES('2018-01-20T09:00:43', NULL);
INSERT INTO btest_numeric VALUES('2018-01-20T09:00:43', NULL);
:PREFIX SELECT first(quantity, time) FROM btest_numeric;
:PREFIX SELECT last(quantity, time) FROM btest_numeric;
:PREFIX SELECT first(time, quantity) FROM btest_numeric;
:PREFIX SELECT last(time, quantity) FROM btest_numeric;

-- NULL values followed by non-NULL values
INSERT INTO btest_numeric VALUES('2019-01-20T09:00:43', 1);
INSERT INTO btest_numeric VALUES('2019-01-20T09:00:43', 2);
:PREFIX SELECT first(quantity, time) FROM btest_numeric;
:PREFIX SELECT last(quantity, time) FROM btest_numeric;
:PREFIX SELECT first(time, quantity) FROM btest_numeric;
:PREFIX SELECT last(time, quantity) FROM btest_numeric;

TRUNCATE btest_numeric;

-- non-NULL values followed by NULL values
INSERT INTO btest_numeric VALUES('2019-01-20T09:00:43', 1);
INSERT INTO btest_numeric VALUES('2019-01-20T09:00:43', 2);
INSERT INTO btest_numeric VALUES('2018-01-20T09:00:43', NULL);
INSERT INTO btest_numeric VALUES('2018-01-20T09:00:43', NULL);
:PREFIX SELECT first(quantity, time) FROM btest_numeric;
:PREFIX SELECT last(quantity, time) FROM btest_numeric;
:PREFIX SELECT first(time, quantity) FROM btest_numeric;
:PREFIX SELECT last(time, quantity) FROM btest_numeric;

ROLLBACK;
