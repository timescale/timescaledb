CREATE TABLE upsert_test(time timestamp PRIMARY KEY, temp float, color text);
SELECT create_hypertable('upsert_test', 'time');
INSERT INTO upsert_test VALUES ('2017-01-20T09:00:01', 22.5, 'yellow') RETURNING *;
INSERT INTO upsert_test VALUES ('2017-01-20T09:00:01', 23.8, 'yellow') ON CONFLICT (time)
DO UPDATE SET temp = 23.8 RETURNING *;
INSERT INTO upsert_test VALUES ('2017-01-20T09:00:01', 78.4, 'yellow') ON CONFLICT DO NOTHING;
SELECT * FROM upsert_test;

-- Referencing constraints by name does not yet work on Hypertables. Check for proper error message.
\set ON_ERROR_STOP 0
INSERT INTO upsert_test VALUES ('2017-01-20T09:00:01', 12.3, 'yellow') ON CONFLICT ON CONSTRAINT upsert_test_pkey
DO UPDATE SET temp = 12.3 RETURNING time, temp, color;

-- Test that update generates error on conflicts
INSERT INTO upsert_test VALUES ('2017-01-21T09:00:01', 22.5, 'yellow') RETURNING *;
UPDATE upsert_test SET time = '2017-01-20T09:00:01';
\set ON_ERROR_STOP 1

-- Test with UNIQUE index on multiple columns instead of PRIMARY KEY constraint
CREATE TABLE upsert_test_unique(time timestamp, temp float, color text);
SELECT create_hypertable('upsert_test_unique', 'time');
CREATE UNIQUE INDEX time_color_idx ON upsert_test_unique (time, color);
INSERT INTO upsert_test_unique VALUES ('2017-01-20T09:00:01', 22.5, 'yellow') RETURNING *;
INSERT INTO upsert_test_unique VALUES ('2017-01-20T09:00:01', 21.2, 'brown');
SELECT * FROM upsert_test_unique ORDER BY time, color DESC;
INSERT INTO upsert_test_unique VALUES ('2017-01-20T09:00:01', 31.8, 'yellow') ON CONFLICT (time, color)
DO UPDATE SET temp = 31.8;
INSERT INTO upsert_test_unique VALUES ('2017-01-20T09:00:01', 54.3, 'yellow') ON CONFLICT DO NOTHING;
SELECT * FROM upsert_test_unique ORDER BY time, color DESC;

-- Test with multiple UNIQUE indexes
CREATE TABLE upsert_test_multi_unique(time timestamp, temp float, color text);
SELECT create_hypertable('upsert_test_multi_unique', 'time');
CREATE UNIQUE INDEX multi_time_temp_idx ON upsert_test_multi_unique (time, temp);
CREATE UNIQUE INDEX multi_time_color_idx ON upsert_test_multi_unique (time, color);
INSERT INTO upsert_test_multi_unique VALUES ('2017-01-20T09:00:01', 25.9, 'yellow');
INSERT INTO upsert_test_multi_unique VALUES ('2017-01-21T09:00:01', 25.9, 'yellow');
INSERT INTO upsert_test_multi_unique VALUES ('2017-01-20T09:00:01', 23.5, 'brown');
INSERT INTO upsert_test_multi_unique VALUES ('2017-01-20T09:00:01', 25.9, 'purple') ON CONFLICT DO NOTHING;
SELECT * FROM upsert_test_multi_unique ORDER BY time, color DESC;
INSERT INTO upsert_test_multi_unique VALUES ('2017-01-20T09:00:01', 25.9, 'blue') ON CONFLICT (time, temp)
DO UPDATE SET color = 'blue';
SELECT * FROM upsert_test_multi_unique ORDER BY time, color DESC;
INSERT INTO upsert_test_multi_unique VALUES ('2017-01-21T09:00:01', 45.7, 'yellow') ON CONFLICT (time, color)
DO UPDATE SET temp = 45.7;
SELECT * FROM upsert_test_multi_unique ORDER BY time, color DESC;
\set ON_ERROR_STOP 0
INSERT INTO upsert_test_multi_unique VALUES ('2017-01-20T09:00:01', 23.5, 'purple') ON CONFLICT (time, color)
DO UPDATE set temp = 23.5;
\set ON_ERROR_STOP 1
