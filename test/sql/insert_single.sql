\ir include/insert_single.sql

\d+ "one_Partition".*
SELECT * FROM "one_Partition" ORDER BY "timeCustom", device_id;

--test that we can insert data into a 1-dimensional table (only time partitioning)
CREATE TABLE "1dim"(time timestamp PRIMARY KEY, temp float);
SELECT create_hypertable('"1dim"', 'time');
INSERT INTO "1dim" VALUES('2017-01-20T09:00:01', 22.5) RETURNING *;
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

-- test that explain works
EXPLAIN
INSERT INTO "3dim" VALUES('2017-01-21T09:00:01', 32.9, 'green', 'nyc'),
                         ('2017-01-21T09:00:47', 27.3, 'purple', 'la') RETURNING *;

EXPLAIN
WITH "3dim_insert" AS (
     INSERT INTO "3dim" VALUES('2017-01-21T09:01:44', 19.3, 'black', 'la') RETURNING time, temp
), regular_insert AS (
   INSERT INTO regular_table VALUES('2017-01-21T10:00:51', 14.3) RETURNING time, temp
) INSERT INTO "1dim" (SELECT time, temp FROM "3dim_insert" UNION SELECT time, temp FROM regular_insert);

-- test prepared statement INSERT
PREPARE "1dim_plan" (timestamp, float) AS
INSERT INTO "1dim" VALUES($1, $2) ON CONFLICT (time) DO NOTHING;
EXECUTE "1dim_plan" ('2017-04-17 23:35', 31.4);
EXECUTE "1dim_plan" ('2017-04-17 23:35', 32.6);

-- test prepared statement with generic plan (forced when no parameters)
PREPARE "1dim_plan_generic" AS
INSERT INTO "1dim" VALUES('2017-05-18 17:24', 18.3);
EXECUTE "1dim_plan_generic";

SELECT * FROM "1dim" ORDER BY time;
SELECT * FROM "3dim" ORDER BY (time, device);

-- Test that large intervals and no interval fail for INTEGER
\set ON_ERROR_STOP 0
CREATE TABLE "inttime_err"(time INTEGER PRIMARY KEY, temp float);
SELECT create_hypertable('"inttime_err"', 'time', chunk_time_interval=>2147483648);
SELECT create_hypertable('"inttime_err"', 'time');
\set ON_ERROR_STOP 1
SELECT create_hypertable('"inttime_err"', 'time', chunk_time_interval=>2147483647);

-- Test that large intervals and no interval fail for SMALLINT
\set ON_ERROR_STOP 0
CREATE TABLE "smallinttime_err"(time SMALLINT PRIMARY KEY, temp float);
SELECT create_hypertable('"smallinttime_err"', 'time', chunk_time_interval=>65536);
SELECT create_hypertable('"smallinttime_err"', 'time');
\set ON_ERROR_STOP 1
SELECT create_hypertable('"smallinttime_err"', 'time', chunk_time_interval=>65535);
