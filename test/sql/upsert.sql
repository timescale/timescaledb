-- Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
--
-- This file is licensed under the Apache License,
-- see LICENSE-APACHE at the top level directory.

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
INSERT INTO upsert_test_multi_unique VALUES ('2017-01-20T09:00:01', 23.5, 'orange') ON CONFLICT (time, temp)
DO UPDATE SET color = excluded.color;
SELECT * FROM upsert_test_multi_unique ORDER BY time, color DESC;
INSERT INTO upsert_test_multi_unique VALUES ('2017-01-21T09:00:01', 45.7, 'yellow') ON CONFLICT (time, color)
DO UPDATE SET temp = 45.7;
SELECT * FROM upsert_test_multi_unique ORDER BY time, color DESC;
\set ON_ERROR_STOP 0
INSERT INTO upsert_test_multi_unique VALUES ('2017-01-20T09:00:01', 23.5, 'purple') ON CONFLICT (time, color)
DO UPDATE set temp = 23.5;
\set ON_ERROR_STOP 1

CREATE TABLE upsert_test_space(time timestamp, device_id_1 char(20), to_drop int, temp float, color text);
--drop two columns; create one.
ALTER TABLE upsert_test_space DROP to_drop;
ALTER TABLE upsert_test_space DROP device_id_1, ADD device_id char(20);
CREATE UNIQUE INDEX time_space_idx ON upsert_test_space (time, device_id);
SELECT create_hypertable('upsert_test_space', 'time', 'device_id', 2, partitioning_func=>'_timescaledb_internal.get_partition_for_key'::regproc);
INSERT INTO upsert_test_space (time, device_id, temp, color) VALUES ('2017-01-20T09:00:01', 'dev1', 25.9, 'yellow') RETURNING *;
INSERT INTO upsert_test_space (time, device_id, temp, color) VALUES ('2017-01-20T09:00:01', 'dev2', 25.9, 'yellow');
INSERT INTO upsert_test_space (time, device_id, temp, color) VALUES ('2017-01-20T09:00:01', 'dev1', 23.5, 'orange') ON CONFLICT (time, device_id)
DO UPDATE SET color = excluded.color;

INSERT INTO upsert_test_space (time, device_id, temp, color) VALUES ('2017-01-20T09:00:01', 'dev2', 23.5, 'orange3') ON CONFLICT (time, device_id)
DO UPDATE SET color = excluded.color||' (originally '|| upsert_test_space.color ||')' RETURNING *;

INSERT INTO upsert_test_space (time, device_id, temp, color) VALUES ('2017-01-20T09:00:01', 'dev3', 23.5, 'orange3.1') ON CONFLICT (time, device_id)
DO UPDATE SET color = excluded.color||' (originally '|| upsert_test_space.color ||')' RETURNING *;

INSERT INTO upsert_test_space (time, device_id, temp, color) VALUES ('2017-01-20T09:00:01', 'dev2', 23.5, 'orange4') ON CONFLICT (time, device_id)
DO NOTHING RETURNING *;
INSERT INTO upsert_test_space (time, device_id, temp, color) VALUES ('2017-01-20T09:00:01', 'dev4', 23.5, 'orange5') ON CONFLICT (time, device_id)
DO NOTHING RETURNING *;

INSERT INTO upsert_test_space (time, device_id, temp, color) VALUES ('2017-01-20T09:00:01', 'dev5', 23.5, 'orange5') ON CONFLICT (time, device_id)
DO NOTHING RETURNING *;

--restore a column with the same name as a previously deleted one;
ALTER TABLE upsert_test_space ADD device_id_1 char(20);
INSERT INTO upsert_test_space (time, device_id, temp, color, device_id_1) VALUES ('2017-01-20T09:00:01', 'dev4', 23.5, 'orange5.1', 'dev-id-1') ON CONFLICT (time, device_id)
DO UPDATE SET color = excluded.color||' (originally '|| upsert_test_space.color ||')' RETURNING *;

INSERT INTO upsert_test_space (time, device_id, temp, color) VALUES ('2017-01-20T09:00:01', 'dev5', 23.5, 'orange6') ON CONFLICT (time, device_id)
DO UPDATE SET color = excluded.color WHERE upsert_test_space.temp < 20 RETURNING *;
INSERT INTO upsert_test_space (time, device_id, temp, color) VALUES ('2017-01-20T09:00:01', 'dev5', 23.5, 'orange7') ON CONFLICT (time, device_id)
DO UPDATE SET color = excluded.color WHERE excluded.temp < 20 RETURNING *;
INSERT INTO upsert_test_space (time, device_id, temp, color) VALUES ('2017-01-20T09:00:01', 'dev5', 3.5, 'orange7') ON CONFLICT (time, device_id)
DO UPDATE SET color = excluded.color, temp=excluded.temp WHERE excluded.temp < 20 RETURNING *;
INSERT INTO upsert_test_space (time, device_id, temp, color) VALUES ('2017-01-20T09:00:01', 'dev5', 43.5, 'orange8') ON CONFLICT (time, device_id)
DO UPDATE SET color = excluded.color WHERE upsert_test_space.temp < 20 RETURNING *;
INSERT INTO upsert_test_space (time, device_id, temp, color, device_id_1) VALUES ('2017-01-20T09:00:01', 'dev5', 43.5, 'orange8', 'device-id-1-new') ON CONFLICT (time, device_id)
DO UPDATE SET device_id_1 = excluded.device_id_1 RETURNING *;
INSERT INTO upsert_test_space (time, device_id, temp, color, device_id_1) VALUES ('2017-01-20T09:00:01', 'dev5', 43.5, 'orange8', 'device-id-1-new') ON CONFLICT (time, device_id)
DO UPDATE SET device_id_1 = 'device-id-1-new-2', color = 'orange9'  RETURNING *;

SELECT * FROM upsert_test_space;

ALTER TABLE upsert_test_space DROP device_id_1, ADD device_id_2 char(20);
INSERT INTO upsert_test_space (time, device_id, temp, color, device_id_2) VALUES ('2017-01-20T09:00:01', 'dev5', 43.5, 'orange8', 'device-id-2')
ON CONFLICT (time, device_id)
DO UPDATE SET device_id_2 = 'device-id-2-new', color = 'orange10' RETURNING *;


WITH CTE AS (
    INSERT INTO upsert_test_multi_unique
    VALUES ('2017-01-20T09:00:01', 25.9, 'purple')
    ON CONFLICT DO NOTHING
    RETURNING *
) SELECT 1;

WITH CTE AS (
    INSERT INTO upsert_test_multi_unique
    VALUES ('2017-01-20T09:00:01', 25.9, 'purple'),
    ('2017-01-20T09:00:01', 29.9, 'purple1')
    ON CONFLICT DO NOTHING
    RETURNING *
) SELECT * FROM CTE;

WITH CTE AS (
    INSERT INTO upsert_test_multi_unique
    VALUES ('2017-01-20T09:00:01', 25.9, 'blue')
    ON CONFLICT (time, temp) DO UPDATE SET color = 'blue'
    RETURNING *
)
SELECT * FROM CTE;
