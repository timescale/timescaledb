\o /dev/null
\ir include/insert_single.sql
\o

SELECT * FROM "one_Partition";

UPDATE "one_Partition" SET series_1 = 47;
UPDATE "one_Partition" SET series_bool = true;
SELECT * FROM "one_Partition";

