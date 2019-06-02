-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE TABLE PUBLIC.hyper_1 (
  time TIMESTAMP NOT NULL,
  series_0 DOUBLE PRECISION NULL,
  series_1 DOUBLE PRECISION NULL,
  series_2 DOUBLE PRECISION NULL
);

CREATE INDEX "time_plain" ON PUBLIC.hyper_1 (time DESC, series_0);
SELECT * FROM create_hypertable('"public"."hyper_1"'::regclass, 'time'::name, number_partitions => 1, create_default_indexes=>false);

INSERT INTO hyper_1 SELECT to_timestamp(ser), ser, ser+10000, sqrt(ser::numeric) FROM generate_series(0,10000) ser;
INSERT INTO hyper_1 SELECT to_timestamp(ser), ser, ser+10000, sqrt(ser::numeric) FROM generate_series(10001,20000) ser;

CREATE TABLE PUBLIC.hyper_1_tz (
  time TIMESTAMPTZ NOT NULL,
  series_0 DOUBLE PRECISION NULL,
  series_1 DOUBLE PRECISION NULL,
  series_2 DOUBLE PRECISION NULL
);

CREATE INDEX "time_plain_tz" ON PUBLIC.hyper_1_tz (time DESC, series_0);
SELECT * FROM create_hypertable('"public"."hyper_1_tz"'::regclass, 'time'::name, number_partitions => 1, create_default_indexes=>false);
INSERT INTO hyper_1_tz SELECT to_timestamp(ser), ser, ser+10000, sqrt(ser::numeric) FROM generate_series(0,10000) ser;
INSERT INTO hyper_1_tz SELECT to_timestamp(ser), ser, ser+10000, sqrt(ser::numeric) FROM generate_series(10001,20000) ser;

CREATE TABLE PUBLIC.hyper_1_int (
  time int NOT NULL,
  series_0 DOUBLE PRECISION NULL,
  series_1 DOUBLE PRECISION NULL,
  series_2 DOUBLE PRECISION NULL
);

CREATE INDEX "time_plain_int" ON PUBLIC.hyper_1_int (time DESC, series_0);
SELECT * FROM create_hypertable('"public"."hyper_1_int"'::regclass, 'time'::name, number_partitions => 1, chunk_time_interval=>10000, create_default_indexes=>FALSE);
INSERT INTO hyper_1_int SELECT ser, ser, ser+10000, sqrt(ser::numeric) FROM generate_series(0,10000) ser;
INSERT INTO hyper_1_int SELECT ser, ser, ser+10000, sqrt(ser::numeric) FROM generate_series(10001,20000) ser;

CREATE TABLE PUBLIC.hyper_1_date (
  time date NOT NULL,
  series_0 DOUBLE PRECISION NULL,
  series_1 DOUBLE PRECISION NULL,
  series_2 DOUBLE PRECISION NULL
);

CREATE INDEX "time_plain_date" ON PUBLIC.hyper_1_date (time DESC, series_0);
SELECT * FROM create_hypertable('"public"."hyper_1_date"'::regclass, 'time'::name, number_partitions => 1, chunk_time_interval=>86400000000, create_default_indexes=>FALSE);
INSERT INTO hyper_1_date SELECT to_timestamp(ser)::date, ser, ser+10000, sqrt(ser::numeric) FROM generate_series(0,10000) ser;
INSERT INTO hyper_1_date SELECT to_timestamp(ser)::date, ser, ser+10000, sqrt(ser::numeric) FROM generate_series(10001,20000) ser;
--below needed to create enough unique dates to trigger an index scan
INSERT INTO hyper_1_date SELECT to_timestamp(ser*100)::date, ser, ser+10000, sqrt(ser::numeric) FROM generate_series(10001,20000) ser;

CREATE TABLE PUBLIC.plain_table (
  time TIMESTAMPTZ NOT NULL,
  series_0 DOUBLE PRECISION NULL,
  series_1 DOUBLE PRECISION NULL,
  series_2 DOUBLE PRECISION NULL
);

CREATE INDEX "time_plain_plain_table" ON PUBLIC.plain_table (time DESC, series_0);
INSERT INTO plain_table SELECT to_timestamp(ser), ser, ser+10000, sqrt(ser::numeric) FROM generate_series(0,10000) ser;
INSERT INTO plain_table SELECT to_timestamp(ser), ser, ser+10000, sqrt(ser::numeric) FROM generate_series(10001,20000) ser;

-- Table with a time partitioning function
CREATE TABLE PUBLIC.hyper_timefunc (
  time float8 NOT NULL,
  series_0 DOUBLE PRECISION NULL,
  series_1 DOUBLE PRECISION NULL,
  series_2 DOUBLE PRECISION NULL
);

CREATE OR REPLACE FUNCTION unix_to_timestamp(unixtime float8)
    RETURNS TIMESTAMPTZ LANGUAGE SQL IMMUTABLE AS
$BODY$
    SELECT to_timestamp(unixtime);
$BODY$;

CREATE INDEX "time_plain_timefunc" ON PUBLIC.hyper_timefunc (to_timestamp(time) DESC, series_0);
SELECT * FROM create_hypertable('"public"."hyper_timefunc"'::regclass, 'time'::name, number_partitions => 1, create_default_indexes=>false, time_partitioning_func => 'unix_to_timestamp');

INSERT INTO hyper_timefunc SELECT ser, ser, ser+10000, sqrt(ser::numeric) FROM generate_series(0,10000) ser;
INSERT INTO hyper_timefunc SELECT ser, ser, ser+10000, sqrt(ser::numeric) FROM generate_series(10001,20000) ser;

ANALYZE plain_table;
ANALYZE hyper_timefunc;
ANALYZE hyper_1;
ANALYZE hyper_1_tz;
ANALYZE hyper_1_int;
ANALYZE hyper_1_date;
