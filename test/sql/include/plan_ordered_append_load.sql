-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- create a now() function for repeatable testing that always returns
-- the same timestamp. It needs to be marked STABLE
CREATE OR REPLACE FUNCTION now_s()
RETURNS timestamptz LANGUAGE PLPGSQL STABLE AS
$BODY$
BEGIN
    RETURN '2000-01-08T0:00:00+0'::timestamptz;
END;
$BODY$;

CREATE TABLE devices(device_id INT PRIMARY KEY, name TEXT);
INSERT INTO devices VALUES
(1,'Device 1'),
(2,'Device 2'),
(3,'Device 3');

-- create a table where we create chunks in order
CREATE TABLE ordered_append(time timestamptz NOT NULL, device_id INT, value float);
SELECT create_hypertable('ordered_append','time');
CREATE index on ordered_append(time DESC,device_id);
CREATE index on ordered_append(device_id,time DESC);

INSERT INTO ordered_append SELECT generate_series('2000-01-01'::timestamptz,'2000-01-18'::timestamptz,'1m'::interval), 1, 0.5;
INSERT INTO ordered_append SELECT generate_series('2000-01-01'::timestamptz,'2000-01-18'::timestamptz,'1m'::interval), 2, 1.5;
INSERT INTO ordered_append SELECT generate_series('2000-01-01'::timestamptz,'2000-01-18'::timestamptz,'1m'::interval), 3, 2.5;

-- create a second table where we create chunks in reverse order
CREATE TABLE ordered_append_reverse(time timestamptz NOT NULL, device_id INT, value float);
SELECT create_hypertable('ordered_append_reverse','time');

INSERT INTO ordered_append_reverse SELECT generate_series('2000-01-18'::timestamptz,'2000-01-01'::timestamptz,'-1m'::interval), 1, 0.5;

-- table where dimension column is last column
CREATE TABLE IF NOT EXISTS dimension_last(
    id INT8 NOT NULL,
    device_id INT NOT NULL,
    name TEXT NOT NULL,
    time timestamptz NOT NULL
);

SELECT create_hypertable('dimension_last', 'time', chunk_time_interval => interval '1day', if_not_exists => True);

-- table with only dimension column
CREATE TABLE IF NOT EXISTS dimension_only(
    time timestamptz NOT NULL
);

SELECT create_hypertable('dimension_only', 'time', chunk_time_interval => interval '1day', if_not_exists => True);

INSERT INTO dimension_last SELECT 1,1,'Device 1',generate_series('2000-01-01 0:00:00+0'::timestamptz,'2000-01-04 23:59:00+0'::timestamptz,'1m'::interval);

INSERT INTO dimension_only VALUES
('2000-01-01'),
('2000-01-03'),
('2000-01-05'),
('2000-01-07');

ANALYZE devices;
ANALYZE ordered_append;
ANALYZE ordered_append_reverse;
ANALYZE dimension_last;
ANALYZE dimension_only;

