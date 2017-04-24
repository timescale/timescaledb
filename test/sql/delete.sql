\o /dev/null
\ir include/insert_two_partitions.sql
\o

SELECT * FROM "two_Partitions" ORDER BY "timeCustom", device_id;

DELETE FROM "two_Partitions" WHERE series_0 = 1.5;
DELETE FROM "two_Partitions" WHERE series_0 = 100;
SELECT * FROM "two_Partitions" ORDER BY "timeCustom", device_id;

