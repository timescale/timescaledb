\ir include/insert_single.sql

\d+ "one_Partition".*
SELECT * FROM "one_Partition" ORDER BY "timeCustom", device_id;

--test that we can insert data into a 1-dimensional table (only time partitioning)
CREATE TABLE "1dim"(time timestamp, temp float);
SELECT create_hypertable('"1dim"', 'time');
INSERT INTO "1dim" VALUES('2017-01-20T09:00:01', 22.5);
INSERT INTO "1dim" VALUES('2017-01-20T09:00:21', 21.2);
INSERT INTO "1dim" VALUES('2017-01-20T09:00:47', 25.1);
SELECT * FROM "1dim";

CREATE TABLE regular_table (time timestamp, temp float);
INSERT INTO regular_table SELECT * FROM "1dim";
SELECT * FROM regular_table;

TRUNCATE TABLE regular_table;
INSERT INTO regular_table VALUES('2017-01-20T09:00:59', 29.2);
INSERT INTO "1dim" SELECT * FROM regular_table;
SELECT * FROM "1dim";

