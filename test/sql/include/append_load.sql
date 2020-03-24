-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- create a now() function for repeatable testing that always returns
-- the same timestamp. It needs to be marked STABLE
CREATE OR REPLACE FUNCTION now_s()
RETURNS timestamptz LANGUAGE PLPGSQL STABLE AS
$BODY$
BEGIN
    RAISE NOTICE 'Stable function now_s() called!';
    RETURN '2017-08-22T10:00:00'::timestamptz;
END;
$BODY$;

CREATE OR REPLACE FUNCTION now_i()
RETURNS timestamptz LANGUAGE PLPGSQL IMMUTABLE AS
$BODY$
BEGIN
    RAISE NOTICE 'Immutable function now_i() called!';
    RETURN '2017-08-22T10:00:00'::timestamptz;
END;
$BODY$;

CREATE OR REPLACE FUNCTION now_v()
RETURNS timestamptz LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    RAISE NOTICE 'Volatile function now_v() called!';
    RETURN '2017-08-22T10:00:00'::timestamptz;
END;
$BODY$;

CREATE TABLE append_test(time timestamptz, temp float, colorid integer);
SELECT create_hypertable('append_test', 'time', chunk_time_interval => 2628000000000);

-- create three chunks
INSERT INTO append_test VALUES ('2017-03-22T09:18:22', 23.5, 1),
                               ('2017-03-22T09:18:23', 21.5, 1),
                               ('2017-05-22T09:18:22', 36.2, 2),
                               ('2017-05-22T09:18:23', 15.2, 2),
                               ('2017-08-22T09:18:22', 34.1, 3);

-- Create another hypertable to join with
CREATE TABLE join_test(time timestamptz, temp float, colorid integer);
SELECT create_hypertable('join_test', 'time', chunk_time_interval => 2628000000000);

INSERT INTO join_test VALUES ('2017-01-22T09:18:22', 15.2, 1),
                             ('2017-02-22T09:18:22', 24.5, 2),
                             ('2017-08-22T09:18:22', 23.1, 3);

-- create hypertable with DATE time dimension
CREATE TABLE metrics_date(time DATE NOT NULL);
SELECT create_hypertable('metrics_date','time');
INSERT INTO metrics_date SELECT generate_series('2000-01-01'::date, '2000-02-01'::date, '5m'::interval);
ANALYZE metrics_date;

-- create hypertable with TIMESTAMP time dimension
CREATE TABLE metrics_timestamp(time TIMESTAMP NOT NULL);
SELECT create_hypertable('metrics_timestamp','time');
INSERT INTO metrics_timestamp SELECT generate_series('2000-01-01'::date, '2000-02-01'::date, '5m'::interval);
ANALYZE metrics_timestamp;

-- create hypertable with TIMESTAMPTZ time dimension
CREATE TABLE metrics_timestamptz(time TIMESTAMPTZ NOT NULL, device_id INT NOT NULL);
CREATE INDEX ON metrics_timestamptz(device_id,time);
SELECT create_hypertable('metrics_timestamptz','time');
INSERT INTO metrics_timestamptz SELECT generate_series('2000-01-01'::date, '2000-02-01'::date, '5m'::interval), 1;
INSERT INTO metrics_timestamptz SELECT generate_series('2000-01-01'::date, '2000-02-01'::date, '5m'::interval), 2;
INSERT INTO metrics_timestamptz SELECT generate_series('2000-01-01'::date, '2000-02-01'::date, '5m'::interval), 3;
ANALYZE metrics_timestamptz;

-- create space partitioned hypertable
CREATE TABLE metrics_space(time timestamptz NOT NULL, device_id int NOT NULL, v1 float, v2 float, v3 text);
SELECT create_hypertable('metrics_space','time','device_id',3);

INSERT INTO metrics_space
SELECT time, device_id, device_id + 0.25, device_id + 0.75, device_id
FROM generate_series('2000-01-01'::timestamptz, '2000-01-14'::timestamptz, '5m'::interval) g1(time),
  generate_series(1,10,1) g2(device_id)
ORDER BY time, device_id;

ANALYZE metrics_space;

