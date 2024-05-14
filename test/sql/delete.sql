-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\o /dev/null
\ir include/insert_two_partitions.sql
\o

SELECT * FROM "two_Partitions" ORDER BY "timeCustom", device_id, series_0, series_1;

DELETE FROM "two_Partitions" WHERE series_0 = 1.5;
DELETE FROM "two_Partitions" WHERE series_0 = 100;
SELECT * FROM "two_Partitions" ORDER BY "timeCustom", device_id, series_0, series_1;

-- Make sure DELETE isn't optimized if it includes Append plans
-- Need to turn of nestloop to make append appear the same on PG96 and PG10
set enable_nestloop = 'off';

CREATE OR REPLACE FUNCTION series_val()
RETURNS integer LANGUAGE PLPGSQL STABLE AS
$BODY$
BEGIN
    RETURN 5;
END;
$BODY$;

-- ConstraintAwareAppend applied for SELECT
EXPLAIN (costs off)
SELECT FROM "two_Partitions"
WHERE series_1 IN (SELECT series_1 FROM "two_Partitions" WHERE series_1 > series_val());

-- ConstraintAwareAppend NOT applied for DELETE
EXPLAIN (costs off)
DELETE FROM "two_Partitions"
WHERE series_1 IN (SELECT series_1 FROM "two_Partitions" WHERE series_1 > series_val());


SELECT * FROM "two_Partitions" ORDER BY "timeCustom", device_id, series_0, series_1;
BEGIN;
DELETE FROM "two_Partitions"
WHERE series_1 IN (SELECT series_1 FROM "two_Partitions" WHERE series_1 > series_val());
SELECT * FROM "two_Partitions" ORDER BY "timeCustom", device_id, series_0, series_1;
ROLLBACK;

BEGIN;
DELETE FROM "two_Partitions"
WHERE series_1 IN (SELECT series_1 FROM "two_Partitions" WHERE series_1 > series_val()) RETURNING "timeCustom";
SELECT * FROM "two_Partitions" ORDER BY "timeCustom", device_id, series_0, series_1;
ROLLBACK;
