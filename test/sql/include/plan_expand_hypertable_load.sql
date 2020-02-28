-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

--single time dimension
CREATE TABLE hyper ("time_broken" bigint NOT NULL, "value" integer);

ALTER TABLE hyper
DROP COLUMN time_broken,
ADD COLUMN time BIGINT;

SELECT create_hypertable('hyper', 'time',  chunk_time_interval => 10);

INSERT INTO hyper SELECT g, g FROM generate_series(0,1000) g;

--insert a point with INT_MAX_64
INSERT INTO hyper (time, value) SELECT 9223372036854775807::bigint, 0;

--time and space
CREATE TABLE hyper_w_space ("time_broken" bigint NOT NULL, "device_id" text, "value" integer);

ALTER TABLE hyper_w_space
DROP COLUMN time_broken,
ADD COLUMN time BIGINT;

SELECT create_hypertable('hyper_w_space', 'time', 'device_id', 4, chunk_time_interval => 10);

INSERT INTO hyper_w_space (time, device_id, value) SELECT g, 'dev' || g, g FROM generate_series(0,30) g;

CREATE VIEW hyper_w_space_view AS (SELECT * FROM hyper_w_space);


--with timestamp and space
CREATE TABLE tag (id serial PRIMARY KEY, name text);
CREATE TABLE hyper_ts ("time_broken" timestamptz NOT NULL, "device_id" text, tag_id INT REFERENCES tag(id), "value" integer);

ALTER TABLE hyper_ts
DROP COLUMN time_broken,
ADD COLUMN time TIMESTAMPTZ;

SELECT create_hypertable('hyper_ts', 'time', 'device_id', 2, chunk_time_interval => '10 seconds'::interval);

INSERT INTO tag(name) SELECT 'tag'||g FROM generate_series(0,10) g;
INSERT INTO hyper_ts (time, device_id, tag_id, value) SELECT to_timestamp(g), 'dev' || g, (random() /10)+1, g FROM generate_series(0,30) g;

--one in the future
INSERT INTO hyper_ts (time, device_id, tag_id, value)  VALUES ('2100-01-01 02:03:04 PST', 'dev101', 1, 0);

--time partitioning function
CREATE OR REPLACE FUNCTION unix_to_timestamp(unixtime float8)
    RETURNS TIMESTAMPTZ LANGUAGE SQL IMMUTABLE PARALLEL SAFE STRICT AS
$BODY$
    SELECT to_timestamp(unixtime);
$BODY$;
CREATE TABLE hyper_timefunc ("time" float8 NOT NULL, "device_id" text, "value" integer);

SELECT create_hypertable('hyper_timefunc', 'time', 'device_id', 4, chunk_time_interval => 10, time_partitioning_func => 'unix_to_timestamp');

INSERT INTO hyper_timefunc (time, device_id, value) SELECT g, 'dev' || g, g FROM generate_series(0,30) g;

CREATE TABLE metrics_timestamp(time timestamp);
SELECT create_hypertable('metrics_timestamp','time');
INSERT INTO metrics_timestamp SELECT generate_series('2000-01-01'::timestamp,'2000-02-01'::timestamp,'1d'::interval);

CREATE TABLE metrics_timestamptz(time timestamptz, device_id int);
SELECT create_hypertable('metrics_timestamptz','time');
INSERT INTO metrics_timestamptz SELECT generate_series('2000-01-01'::timestamptz,'2000-02-01'::timestamptz,'1d'::interval), 1;
INSERT INTO metrics_timestamptz SELECT generate_series('2000-01-01'::timestamptz,'2000-02-01'::timestamptz,'1d'::interval), 2;
INSERT INTO metrics_timestamptz SELECT generate_series('2000-01-01'::timestamptz,'2000-02-01'::timestamptz,'1d'::interval), 3;

--create a second table to test joins with
CREATE TABLE metrics_timestamptz_2 (LIKE metrics_timestamptz);
SELECT create_hypertable('metrics_timestamptz_2','time');
INSERT INTO metrics_timestamptz_2
SELECT * FROM metrics_timestamptz;
INSERT INTO metrics_timestamptz_2 VALUES ('2000-12-01'::timestamptz, 3);

CREATE TABLE metrics_date(time date);
SELECT create_hypertable('metrics_date','time');
INSERT INTO metrics_date SELECT generate_series('2000-01-01'::date,'2000-02-01'::date,'1d'::interval);

ANALYZE hyper;
ANALYZE hyper_w_space;
ANALYZE tag;
ANALYZE hyper_ts;
ANALYZE hyper_timefunc;

-- create normal table for JOIN tests
CREATE TABLE regular_timestamptz(time timestamptz);
INSERT INTO regular_timestamptz SELECT generate_series('2000-01-01'::timestamptz,'2000-02-01'::timestamptz,'1d'::interval);
