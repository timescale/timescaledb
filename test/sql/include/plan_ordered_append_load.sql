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

INSERT INTO ordered_append VALUES('2000-01-01',1,1.0);
INSERT INTO ordered_append VALUES('2000-01-08',1,2.0);
INSERT INTO ordered_append VALUES('2000-01-15',1,3.0);

-- create a second table where we create chunks in reverse order
CREATE TABLE ordered_append_reverse(time timestamptz NOT NULL, device_id INT, value float);
SELECT create_hypertable('ordered_append_reverse','time');

INSERT INTO ordered_append_reverse VALUES('2000-01-15',1,1.0);
INSERT INTO ordered_append_reverse VALUES('2000-01-08',1,2.0);
INSERT INTO ordered_append_reverse VALUES('2000-01-01',1,3.0);

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

INSERT INTO dimension_last VALUES
(1,1,'Device 1','2000-01-01'),
(2,1,'Device 1','2000-01-02'),
(3,1,'Device 1','2000-01-03'),
(4,1,'Device 1','2000-01-04');

INSERT INTO dimension_only VALUES
('2000-01-01'),
('2000-01-03'),
('2000-01-05'),
('2000-01-07');

