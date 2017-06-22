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
SELECT "1dim" FROM "1dim";

-- Create a three-dimensional table
CREATE TABLE "3dim" (time timestamp, temp float, device text, location text);
SELECT create_hypertable('"3dim"', 'time', 'device', 2);
SELECT add_dimension('"3dim"', 'location', 2);
INSERT INTO "3dim" VALUES('2017-01-20T09:00:01', 22.5, 'blue', 'nyc');
INSERT INTO "3dim" VALUES('2017-01-20T09:00:21', 21.2, 'brown', 'sthlm');
INSERT INTO "3dim" VALUES('2017-01-20T09:00:47', 25.1, 'yellow', 'la');

--show the constraints on the three-dimensional chunk
\d+ _timescaledb_internal._hyper_3_6_chunk

--queries should work in three dimensions
SELECT * FROM "3dim";

