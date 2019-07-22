-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE TABLE btest(time timestamp NOT NULL, time_alt timestamp, gp INTEGER, temp float, strid TEXT DEFAULT 'testing');
SELECT schema_name, table_name, created FROM create_hypertable('btest', 'time');
INSERT INTO btest VALUES('2017-01-20T09:00:01', '2017-01-20T10:00:00', 1, 22.5);
INSERT INTO btest VALUES('2017-01-20T09:00:21', '2017-01-20T09:00:59', 1, 21.2);
INSERT INTO btest VALUES('2017-01-20T09:00:47', '2017-01-20T09:00:58', 1, 25.1);
INSERT INTO btest VALUES('2017-01-20T09:00:02', '2017-01-20T09:00:57', 2, 35.5);
INSERT INTO btest VALUES('2017-01-20T09:00:21', '2017-01-20T09:00:56', 2, 30.2);
--TOASTED;
INSERT INTO btest VALUES('2017-01-20T09:00:43', '2017-01-20T09:01:55', 2, 20.1, repeat('xyz', 1000000) );

CREATE TABLE btest_numeric (time timestamp NOT NULL, quantity numeric);
SELECT schema_name, table_name, created FROM create_hypertable('btest_numeric', 'time');

