\o /dev/null
\ir include/insert_single.sql
\o

-- Make sure UPDATE isn't optimized if it includes Append plans
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
SELECT FROM "one_Partition"
WHERE series_1 IN (SELECT series_1 FROM "one_Partition" WHERE series_1 > series_val());

-- ConstraintAwareAppend NOT applied for UPDATE
EXPLAIN (costs off)
UPDATE "one_Partition"
SET series_1 = 8
WHERE series_1 IN (SELECT series_1 FROM "one_Partition" WHERE series_1 > series_val());

SELECT * FROM "one_Partition" ORDER BY "timeCustom", device_id;
UPDATE "one_Partition"
SET series_1 = 8
WHERE series_1 IN (SELECT series_1 FROM "one_Partition" WHERE series_1 > series_val());
SELECT * FROM "one_Partition"  ORDER BY "timeCustom", device_id;

UPDATE "one_Partition" SET series_1 = 47;
UPDATE "one_Partition" SET series_bool = true;
SELECT * FROM "one_Partition" ORDER BY "timeCustom", device_id;
