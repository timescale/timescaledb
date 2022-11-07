-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Utility function for grouping/slotting time with a given interval.
CREATE OR REPLACE FUNCTION date_group(
    field           timestamp,
    group_interval  interval
)
    RETURNS timestamp LANGUAGE SQL STABLE AS
$BODY$
    SELECT to_timestamp((EXTRACT(EPOCH from $1)::int /
        EXTRACT(EPOCH from group_interval)::int) *
        EXTRACT(EPOCH from group_interval)::int)::timestamp;
$BODY$;

CREATE TABLE PUBLIC."testNs" (
  "timeCustom" TIMESTAMP NOT NULL,
  device_id TEXT NOT NULL,
  series_0 DOUBLE PRECISION NULL,
  series_1 DOUBLE PRECISION NULL,
  series_2 DOUBLE PRECISION NULL,
  series_bool BOOLEAN NULL
);
CREATE INDEX ON PUBLIC."testNs" (device_id, "timeCustom" DESC NULLS LAST) WHERE device_id IS NOT NULL;

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE SCHEMA "testNs" AUTHORIZATION :ROLE_DEFAULT_PERM_USER;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
SELECT * FROM create_hypertable('"public"."testNs"', 'timeCustom', 'device_id', 2, associated_schema_name=>'testNs' );

\c :TEST_DBNAME
INSERT INTO PUBLIC."testNs"("timeCustom", device_id, series_0, series_1) VALUES
('2009-11-12T01:00:00+00:00', 'dev1', 1.5, 1),
('2009-11-12T01:00:00+00:00', 'dev1', 1.5, 2),
('2009-11-10T23:00:02+00:00', 'dev1', 2.5, 3);

INSERT INTO PUBLIC."testNs"("timeCustom", device_id, series_0, series_1) VALUES
('2009-11-10T23:00:00+00:00', 'dev2', 1.5, 1),
('2009-11-10T23:00:00+00:00', 'dev2', 1.5, 2);

SELECT * FROM PUBLIC."testNs";

SET client_min_messages = WARNING;

\echo 'The next 2 queries will differ in output between UTC and EST since the mod is on the 100th hour UTC'
SET timezone = 'UTC';
SELECT date_group("timeCustom", '100 days') AS time, sum(series_0)
FROM PUBLIC."testNs" GROUP BY time ORDER BY time ASC;

SET timezone = 'EST';
SELECT date_group("timeCustom", '100 days') AS time, sum(series_0)
FROM PUBLIC."testNs" GROUP BY time ORDER BY time ASC;

\echo 'The rest of the queries will be the same in output between UTC and EST'
SET timezone = 'UTC';
SELECT date_group("timeCustom", '1 day') AS time, sum(series_0)
FROM PUBLIC."testNs" GROUP BY time ORDER BY time ASC;

SET timezone = 'EST';
SELECT date_group("timeCustom", '1 day') AS time, sum(series_0)
FROM PUBLIC."testNs" GROUP BY time ORDER BY time ASC;

SET timezone = 'UTC';

SELECT *
FROM PUBLIC."testNs"
WHERE "timeCustom" >= TIMESTAMP '2009-11-10T23:00:00'
AND "timeCustom" < TIMESTAMP '2009-11-12T01:00:00' ORDER BY "timeCustom" DESC, device_id, series_1;

SET timezone = 'EST';
SELECT *
FROM PUBLIC."testNs"
WHERE "timeCustom" >= TIMESTAMP '2009-11-10T23:00:00'
AND "timeCustom" < TIMESTAMP '2009-11-12T01:00:00' ORDER BY "timeCustom" DESC, device_id, series_1;

SET timezone = 'UTC';
SELECT date_group("timeCustom", '1 day') AS time, sum(series_0)
FROM PUBLIC."testNs" GROUP BY time ORDER BY time ASC LIMIT 2;

SET timezone = 'EST';
SELECT date_group("timeCustom", '1 day') AS time, sum(series_0)
FROM PUBLIC."testNs" GROUP BY time ORDER BY time ASC LIMIT 2;

------------------------------------
-- Test time conversion functions --
------------------------------------
\set ON_ERROR_STOP 0

SET timezone = 'UTC';

-- Conversion to timestamp using Postgres built-in function taking
-- double. Gives inaccurate result on Postgres <= 9.6.2. Accurate on
-- Postgres >= 9.6.3.
SELECT to_timestamp(1486480176.236538);

-- extension-specific version taking microsecond UNIX timestamp
SELECT _timescaledb_internal.to_timestamp(1486480176236538);

-- Should be the inverse of the statement above.
SELECT _timescaledb_internal.to_unix_microseconds('2017-02-07 15:09:36.236538+00');

-- For timestamps, BIGINT MAX represents +Infinity and BIGINT MIN
-- -Infinity. We keep this notion for UNIX epoch time:
SELECT _timescaledb_internal.to_unix_microseconds('+infinity');
SELECT _timescaledb_internal.to_timestamp(9223372036854775807);
SELECT _timescaledb_internal.to_unix_microseconds('-infinity');
SELECT _timescaledb_internal.to_timestamp(-9223372036854775808);

-- In UNIX microseconds, the largest bigint value below infinity
-- (BIGINT MAX) is smaller than internal date upper bound and should
-- therefore be OK. Further, converting to the internal postgres epoch
-- cannot overflow a 64-bit INTEGER since the postgres epoch is at a
-- later date compared to the UNIX epoch, and is therefore represented
-- by a smaller number
SELECT _timescaledb_internal.to_timestamp(9223372036854775806);

-- Julian day zero is -210866803200000000 microseconds from UNIX epoch
SELECT _timescaledb_internal.to_timestamp(-210866803200000000);

\set VERBOSITY default
-- Going beyond Julian day zero should give out-of-range error
SELECT _timescaledb_internal.to_timestamp(-210866803200000001);

-- Lower bound on date (should return the Julian day zero UNIX timestamp above)
SELECT _timescaledb_internal.to_unix_microseconds('4714-11-24 00:00:00+00 BC');

-- Going beyond lower bound on date should return out-of-range
SELECT _timescaledb_internal.to_unix_microseconds('4714-11-23 23:59:59.999999+00 BC');

-- The upper bound for Postgres TIMESTAMPTZ
SELECT timestamp '294276-12-31 23:59:59.999999+00';

-- Going beyond the upper bound, should fail
SELECT timestamp '294276-12-31 23:59:59.999999+00' + interval '1 us';

-- Cannot represent the upper bound timestamp with a UNIX microsecond timestamp
-- since the Postgres epoch is at a later date than the UNIX epoch.
SELECT _timescaledb_internal.to_unix_microseconds('294276-12-31 23:59:59.999999+00');

-- Subtracting the difference between the two epochs (10957 days) should bring
-- us within range.
SELECT timestamp '294276-12-31 23:59:59.999999+00' - interval '10957 days';

SELECT _timescaledb_internal.to_unix_microseconds('294247-01-01 23:59:59.999999');

-- Adding one microsecond should take us out-of-range again
SELECT timestamp '294247-01-01 23:59:59.999999' + interval '1 us';
SELECT _timescaledb_internal.to_unix_microseconds(timestamp '294247-01-01 23:59:59.999999' + interval '1 us');

--no time_bucketing of dates not by integer # of days

SELECT time_bucket('1 hour', DATE '2012-01-01');
SELECT time_bucket('25 hour', DATE '2012-01-01');

\set ON_ERROR_STOP 1

SELECT time_bucket(INTERVAL '1 day', TIMESTAMP '2011-01-02 01:01:01');

SELECT time, time_bucket(INTERVAL '2 day ', time)
FROM unnest(ARRAY[
    TIMESTAMP '2011-01-01 01:01:01',
    TIMESTAMP '2011-01-02 01:01:01',
    TIMESTAMP '2011-01-03 01:01:01',
    TIMESTAMP '2011-01-04 01:01:01'
    ]) AS time;


SELECT int_def, time_bucket(int_def,TIMESTAMP '2011-01-02 01:01:01.111')
FROM unnest(ARRAY[
    INTERVAL '1 millisecond',
    INTERVAL '1 second',
    INTERVAL '1 minute',
    INTERVAL '1 hour',
    INTERVAL '1 day',
    INTERVAL '2 millisecond',
    INTERVAL '2 second',
    INTERVAL '2 minute',
    INTERVAL '2 hour',
    INTERVAL '2 day'
    ]) AS int_def;

\set ON_ERROR_STOP 0
SELECT time_bucket(INTERVAL '1 year 1d',TIMESTAMP '2011-01-02 01:01:01.111');
SELECT time_bucket(INTERVAL '1 month 1 minute',TIMESTAMP '2011-01-02 01:01:01.111');
\set ON_ERROR_STOP 1

SELECT time, time_bucket(INTERVAL '5 minute', time)
FROM unnest(ARRAY[
    TIMESTAMP '1970-01-01 00:59:59.999999',
    TIMESTAMP '1970-01-01 01:01:00',
    TIMESTAMP '1970-01-01 01:04:59.999999',
    TIMESTAMP '1970-01-01 01:05:00'
    ]) AS time;


SELECT time, time_bucket(INTERVAL '5 minute', time)
FROM unnest(ARRAY[
    TIMESTAMP '2011-01-02 01:04:59.999999',
    TIMESTAMP '2011-01-02 01:05:00',
    TIMESTAMP '2011-01-02 01:09:59.999999',
    TIMESTAMP '2011-01-02 01:10:00'
    ]) AS time;

--offset with interval
SELECT time, time_bucket(INTERVAL '5 minute', time ,  INTERVAL '2 minutes')
FROM unnest(ARRAY[
    TIMESTAMP '2011-01-02 01:01:59.999999',
    TIMESTAMP '2011-01-02 01:02:00',
    TIMESTAMP '2011-01-02 01:06:59.999999',
    TIMESTAMP '2011-01-02 01:07:00'
    ]) AS time;

SELECT time, time_bucket(INTERVAL '5 minute', time , - INTERVAL '2 minutes')
FROM unnest(ARRAY[
    TIMESTAMP '2011-01-02 01:02:59.999999',
    TIMESTAMP '2011-01-02 01:03:00',
    TIMESTAMP '2011-01-02 01:07:59.999999',
    TIMESTAMP '2011-01-02 01:08:00'
    ]) AS time;

--offset with infinity
-- timestamp
SELECT time, time_bucket(INTERVAL '1 week', time, INTERVAL '1 day')
FROM unnest(ARRAY[
    timestamp '-Infinity',
    timestamp 'Infinity'
    ]) AS time;

-- timestamptz
SELECT time, time_bucket(INTERVAL '1 week', time, INTERVAL '1 day')
FROM unnest(ARRAY[
    timestamp with time zone '-Infinity',
    timestamp with time zone 'Infinity'
    ]) AS time;

-- Date
SELECT date, time_bucket(INTERVAL '1 week', date, INTERVAL '1 day')
FROM unnest(ARRAY[
    date '-Infinity',
    date 'Infinity'
    ]) AS date;

--example to align with an origin
SELECT time, time_bucket(INTERVAL '5 minute', time - (TIMESTAMP '2011-01-02 00:02:00' - TIMESTAMP 'epoch')) +  (TIMESTAMP '2011-01-02 00:02:00'-TIMESTAMP 'epoch')
FROM unnest(ARRAY[
    TIMESTAMP '2011-01-02 01:01:59.999999',
    TIMESTAMP '2011-01-02 01:02:00',
    TIMESTAMP '2011-01-02 01:06:59.999999',
    TIMESTAMP '2011-01-02 01:07:00'
    ]) AS time;


--rounding version
SELECT time, time_bucket(INTERVAL '5 minute', time , - INTERVAL '2.5 minutes') + INTERVAL '2 minutes 30 seconds'
FROM unnest(ARRAY[
    TIMESTAMP '2011-01-02 01:05:01',
    TIMESTAMP '2011-01-02 01:07:29',
    TIMESTAMP '2011-01-02 01:02:30',
    TIMESTAMP '2011-01-02 01:07:30',
    TIMESTAMP '2011-01-02 01:02:29'
    ]) AS time;

--time_bucket with timezone should mimick date_trunc
SET timezone TO 'UTC';
SELECT time, time_bucket(INTERVAL '1 hour', time), date_trunc('hour', time)
FROM unnest(ARRAY[
    TIMESTAMP WITH TIME ZONE '2011-01-02 01:01:01',
    TIMESTAMP WITH TIME ZONE '2011-01-02 01:01:01+01',
    TIMESTAMP WITH TIME ZONE '2011-01-02 01:01:01+02'
    ]) AS time;

SELECT time, time_bucket(INTERVAL '1 day', time), date_trunc('day', time)
FROM unnest(ARRAY[
    TIMESTAMP WITH TIME ZONE '2011-01-02 01:01:01',
    TIMESTAMP WITH TIME ZONE '2011-01-02 01:01:01+01',
    TIMESTAMP WITH TIME ZONE '2011-01-02 01:01:01+02'
    ]) AS time;

--what happens with a local tz
SET timezone TO 'America/New_York';
SELECT time, time_bucket(INTERVAL '1 hour', time), date_trunc('hour', time)
FROM unnest(ARRAY[
    TIMESTAMP WITH TIME ZONE '2011-01-02 01:01:01',
    TIMESTAMP WITH TIME ZONE '2011-01-02 01:01:01+01',
    TIMESTAMP WITH TIME ZONE '2011-01-02 01:01:01+02'
    ]) AS time;

--Note the timestamp tz input is aligned with UTC day /not/ local day. different than date_trunc.
SELECT time, time_bucket(INTERVAL '1 day', time), date_trunc('day', time)
FROM unnest(ARRAY[
    TIMESTAMP WITH TIME ZONE '2011-01-02 01:01:01',
    TIMESTAMP WITH TIME ZONE '2011-01-03 01:01:01+01',
    TIMESTAMP WITH TIME ZONE '2011-01-04 01:01:01+02'
    ]) AS time;

--can force local bucketing with simple cast.
SELECT time, time_bucket(INTERVAL '1 day', time::timestamp), date_trunc('day', time)
FROM unnest(ARRAY[
    TIMESTAMP WITH TIME ZONE '2011-01-02 01:01:01',
    TIMESTAMP WITH TIME ZONE '2011-01-03 01:01:01+01',
    TIMESTAMP WITH TIME ZONE '2011-01-04 01:01:01+02'
    ]) AS time;

--can also use interval to correct
SELECT time, time_bucket(INTERVAL '1 day', time, -INTERVAL '19 hours'), date_trunc('day', time)
FROM unnest(ARRAY[
    TIMESTAMP WITH TIME ZONE '2011-01-02 01:01:01',
    TIMESTAMP WITH TIME ZONE '2011-01-03 01:01:01+01',
    TIMESTAMP WITH TIME ZONE '2011-01-04 01:01:01+02'
    ]) AS time;

--dst: same local hour bucketed as two different hours.
SELECT time, time_bucket(INTERVAL '1 hour', time), date_trunc('hour', time)
FROM unnest(ARRAY[
    TIMESTAMP WITH TIME ZONE '2017-11-05 12:05:00+07',
    TIMESTAMP WITH TIME ZONE '2017-11-05 13:05:00+07'
    ]) AS time;

--local alignment changes when bucketing by UTC across dst boundary
SELECT time, time_bucket(INTERVAL '2 hour', time)
FROM unnest(ARRAY[
    TIMESTAMP WITH TIME ZONE '2017-11-05 10:05:00+07',
    TIMESTAMP WITH TIME ZONE '2017-11-05 12:05:00+07',
    TIMESTAMP WITH TIME ZONE '2017-11-05 13:05:00+07',
    TIMESTAMP WITH TIME ZONE '2017-11-05 15:05:00+07'
    ]) AS time;

--local alignment is preserved when bucketing by local time across DST boundary.
SELECT time, time_bucket(INTERVAL '2 hour', time::timestamp)
FROM unnest(ARRAY[
    TIMESTAMP WITH TIME ZONE '2017-11-05 10:05:00+07',
    TIMESTAMP WITH TIME ZONE '2017-11-05 12:05:00+07',
    TIMESTAMP WITH TIME ZONE '2017-11-05 13:05:00+07',
    TIMESTAMP WITH TIME ZONE '2017-11-05 15:05:00+07'
    ]) AS time;


SELECT time,
    time_bucket(10::smallint, time) AS time_bucket_smallint,
    time_bucket(10::int, time) AS time_bucket_int,
    time_bucket(10::bigint, time) AS time_bucket_bigint
FROM unnest(ARRAY[
     '-11',
     '-10',
      '-9',
      '-1',
       '0',
       '1',
      '99',
     '100',
     '109',
     '110'
    ]::smallint[]) AS time;

SELECT time,
    time_bucket(10::smallint, time, 2::smallint) AS time_bucket_smallint,
    time_bucket(10::int, time, 2::int) AS time_bucket_int,
    time_bucket(10::bigint, time, 2::bigint) AS time_bucket_bigint
FROM unnest(ARRAY[
      '-9',
      '-8',
      '-7',
       '1',
       '2',
       '3',
     '101',
     '102',
     '111',
     '112'
    ]::smallint[]) AS time;

SELECT time,
    time_bucket(10::smallint, time, -2::smallint) AS time_bucket_smallint,
    time_bucket(10::int, time, -2::int) AS time_bucket_int,
    time_bucket(10::bigint, time, -2::bigint) AS time_bucket_bigint
FROM unnest(ARRAY[
    '-13',
    '-12',
    '-11',
     '-3',
     '-2',
     '-1',
     '97',
     '98',
    '107',
    '108'
    ]::smallint[]) AS time;


\set ON_ERROR_STOP 0
SELECT time_bucket(10::smallint, '-32768'::smallint);
SELECT time_bucket(10::smallint, '-32761'::smallint);
select time_bucket(10::smallint, '-32768'::smallint, 1000::smallint);
select time_bucket(10::smallint, '-32768'::smallint, '32767'::smallint);
select time_bucket(10::smallint, '32767'::smallint, '-32768'::smallint);
\set ON_ERROR_STOP 1

SELECT time, time_bucket(10::smallint, time)
FROM unnest(ARRAY[
    '-32760',
    '-32759',
    '32767'
    ]::smallint[]) AS time;

\set ON_ERROR_STOP 0
SELECT time_bucket(10::int, '-2147483648'::int);
SELECT time_bucket(10::int, '-2147483641'::int);
SELECT time_bucket(1000::int, '-2147483000'::int, 1::int);
SELECT time_bucket(1000::int, '-2147483648'::int, '2147483647'::int);
SELECT time_bucket(1000::int, '2147483647'::int, '-2147483648'::int);
\set ON_ERROR_STOP 1

SELECT time, time_bucket(10::int, time)
FROM unnest(ARRAY[
    '-2147483640',
    '-2147483639',
    '2147483647'
    ]::int[]) AS time;

\set ON_ERROR_STOP 0
SELECT time_bucket(10::bigint, '-9223372036854775808'::bigint);
SELECT time_bucket(10::bigint, '-9223372036854775801'::bigint);
SELECT time_bucket(1000::bigint, '-9223372036854775000'::bigint, 1::bigint);
SELECT time_bucket(1000::bigint, '-9223372036854775808'::bigint, '9223372036854775807'::bigint);
SELECT time_bucket(1000::bigint, '9223372036854775807'::bigint, '-9223372036854775808'::bigint);
\set ON_ERROR_STOP 1

SELECT time, time_bucket(10::bigint, time)
FROM unnest(ARRAY[
    '-9223372036854775800',
    '-9223372036854775799',
    '9223372036854775807'
    ]::bigint[]) AS time;

SELECT time, time_bucket(INTERVAL '1 day', time::date)
FROM unnest(ARRAY[
    date '2017-11-05',
    date '2017-11-06'
    ]) AS time;

SELECT time, time_bucket(INTERVAL '4 day', time::date)
FROM unnest(ARRAY[
    date '2017-11-04',
    date '2017-11-05',
    date '2017-11-08',
    date '2017-11-09'
    ]) AS time;

SELECT time, time_bucket(INTERVAL '4 day', time::date, INTERVAL '2 day')
FROM unnest(ARRAY[
    date '2017-11-06',
    date '2017-11-07',
    date '2017-11-10',
    date '2017-11-11'
    ]) AS time;

-- 2019-09-24 is a Monday, and we want to ensure that time_bucket returns the week starting with a Monday as date_trunc does,
-- Rather than a Saturday which is the date of the PostgreSQL epoch
SELECT time, time_bucket(INTERVAL '1 week', time::date)
FROM unnest(ARRAY[
    date '2018-09-16',
    date '2018-09-17',
    date '2018-09-23',
    date '2018-09-24'
    ]) AS time;

SELECT time, time_bucket(INTERVAL '1 week', time)
FROM unnest(ARRAY[
    timestamp without time zone '2018-09-16',
    timestamp without time zone '2018-09-17',
    timestamp without time zone '2018-09-23',
    timestamp without time zone '2018-09-24'
    ]) AS time;

SELECT time, time_bucket(INTERVAL '1 week', time)
FROM unnest(ARRAY[
    timestamp with time zone '2018-09-16',
    timestamp with time zone '2018-09-17',
    timestamp with time zone '2018-09-23',
    timestamp with time zone '2018-09-24'
    ]) AS time;

SELECT time, time_bucket(INTERVAL '1 week', time)
FROM unnest(ARRAY[
    timestamp with time zone '-Infinity',
    timestamp with time zone 'Infinity'
    ]) AS time;

SELECT time, time_bucket(INTERVAL '1 week', time)
FROM unnest(ARRAY[
    timestamp without time zone '-Infinity',
    timestamp without time zone 'Infinity'
    ]) AS time;


SELECT time, time_bucket(INTERVAL '1 week', time), date_trunc('week', time) = time_bucket(INTERVAL '1 week', time)
FROM unnest(ARRAY[
    timestamp without time zone '4714-11-24 01:01:01.0 BC',
    timestamp without time zone '294276-12-31 23:59:59.9999'
    ]) AS time;

--1000 years later weeks still align.
SELECT time, time_bucket(INTERVAL '1 week', time), date_trunc('week', time) = time_bucket(INTERVAL '1 week', time)
FROM unnest(ARRAY[
    timestamp without time zone '3018-09-14',
    timestamp without time zone '3018-09-20',
    timestamp without time zone '3018-09-21',
    timestamp without time zone '3018-09-22'
    ]) AS time;

--weeks align for timestamptz as well if cast to local time, (but not if done at UTC).
SELECT time, date_trunc('week', time) = time_bucket(INTERVAL '1 week', time),  date_trunc('week', time) = time_bucket(INTERVAL '1 week', time::timestamp)
FROM unnest(ARRAY[
    timestamp with time zone '3018-09-14',
    timestamp with time zone '3018-09-20',
    timestamp with time zone '3018-09-21',
    timestamp with time zone '3018-09-22'
    ]) AS time;

--check functions with origin
--note that the default origin is at 0 UTC, using origin parameter it is easy to provide a EDT origin point
\x
SELECT time, time_bucket(INTERVAL '1 week', time) no_epoch,
             time_bucket(INTERVAL '1 week', time::timestamp) no_epoch_local,
             time_bucket(INTERVAL '1 week', time) = time_bucket(INTERVAL '1 week', time, timestamptz '2000-01-03 00:00:00+0') always_true,
             time_bucket(INTERVAL '1 week', time, timestamptz '2000-01-01 00:00:00+0') pg_epoch,
             time_bucket(INTERVAL '1 week', time, timestamptz 'epoch') unix_epoch,
             time_bucket(INTERVAL '1 week', time, timestamptz '3018-09-13') custom_1,
             time_bucket(INTERVAL '1 week', time, timestamptz '3018-09-14') custom_2
FROM unnest(ARRAY[
    timestamp with time zone '2000-01-01 00:00:00+0'- interval '1 second',
    timestamp with time zone '2000-01-01 00:00:00+0',
    timestamp with time zone '2000-01-03 00:00:00+0'- interval '1 second',
    timestamp with time zone '2000-01-03 00:00:00+0',
    timestamp with time zone '2000-01-01',
    timestamp with time zone '2000-01-02',
    timestamp with time zone '2000-01-03',
    timestamp with time zone '3018-09-12',
    timestamp with time zone '3018-09-13',
    timestamp with time zone '3018-09-14',
    timestamp with time zone '3018-09-15'
    ]) AS time;

SELECT time, time_bucket(INTERVAL '1 week', time) no_epoch,
             time_bucket(INTERVAL '1 week', time) = time_bucket(INTERVAL '1 week', time, timestamp '2000-01-03 00:00:00') always_true,
             time_bucket(INTERVAL '1 week', time, timestamp '2000-01-01 00:00:00+0') pg_epoch,
             time_bucket(INTERVAL '1 week', time, timestamp 'epoch') unix_epoch,
             time_bucket(INTERVAL '1 week', time, timestamp '3018-09-13') custom_1,
             time_bucket(INTERVAL '1 week', time, timestamp '3018-09-14') custom_2
FROM unnest(ARRAY[
    timestamp without time zone '2000-01-01 00:00:00'- interval '1 second',
    timestamp without time zone '2000-01-01 00:00:00',
    timestamp without time zone '2000-01-03 00:00:00'- interval '1 second',
    timestamp without time zone '2000-01-03 00:00:00',
    timestamp without time zone '2000-01-01',
    timestamp without time zone '2000-01-02',
    timestamp without time zone '2000-01-03',
    timestamp without time zone '3018-09-12',
    timestamp without time zone '3018-09-13',
    timestamp without time zone '3018-09-14',
    timestamp without time zone '3018-09-15'
    ]) AS time;

SELECT time, time_bucket(INTERVAL '1 week', time) no_epoch,
             time_bucket(INTERVAL '1 week', time) = time_bucket(INTERVAL '1 week', time, date '2000-01-03') always_true,
             time_bucket(INTERVAL '1 week', time, date '2000-01-01') pg_epoch,
             time_bucket(INTERVAL '1 week', time, (timestamp 'epoch')::date) unix_epoch,
             time_bucket(INTERVAL '1 week', time, date '3018-09-13') custom_1,
             time_bucket(INTERVAL '1 week', time, date '3018-09-14') custom_2
FROM unnest(ARRAY[
    date '1999-12-31',
    date '2000-01-01',
    date '2000-01-02',
    date '2000-01-03',
    date '3018-09-12',
    date '3018-09-13',
    date '3018-09-14',
    date '3018-09-15'
    ]) AS time;
\x

--really old origin works if date around that time
SELECT time, time_bucket(INTERVAL '1 week', time, timestamp without time zone '4710-11-24 01:01:01.0 BC')
FROM unnest(ARRAY[
    timestamp without time zone '4710-11-24 01:01:01.0 BC',
    timestamp without time zone '4710-11-25 01:01:01.0 BC',
    timestamp without time zone '2001-01-01',
    timestamp without time zone '3001-01-01'
    ]) AS time;

SELECT time, time_bucket(INTERVAL '1 week', time, timestamp without time zone '294270-12-30 23:59:59.9999')
FROM unnest(ARRAY[
    timestamp without time zone '294270-12-29 23:59:59.9999',
    timestamp without time zone '294270-12-30 23:59:59.9999',
    timestamp without time zone '294270-12-31 23:59:59.9999',
    timestamp without time zone '2001-01-01',
    timestamp without time zone '3001-01-01'
    ]) AS time;

\set ON_ERROR_STOP 0
--really old origin + very new data + long period errors
SELECT time, time_bucket(INTERVAL '100000 day', time, timestamp without time zone '4710-11-24 01:01:01.0 BC')
FROM unnest(ARRAY[
    timestamp without time zone '294270-12-31 23:59:59.9999'
    ]) AS time;
SELECT time, time_bucket(INTERVAL '100000 day', time, timestamp with time zone '4710-11-25 01:01:01.0 BC')
FROM unnest(ARRAY[
    timestamp with time zone '294270-12-30 23:59:59.9999'
    ]) AS time;

--really high origin + old data + long period errors out
SELECT time, time_bucket(INTERVAL '10000000 day', time, timestamp without time zone '294270-12-31 23:59:59.9999')
FROM unnest(ARRAY[
    timestamp without time zone '4710-11-24 01:01:01.0 BC'
    ]) AS time;
SELECT time, time_bucket(INTERVAL '10000000 day', time, timestamp with time zone '294270-12-31 23:59:59.9999')
FROM unnest(ARRAY[
    timestamp with time zone '4710-11-24 01:01:01.0 BC'
    ]) AS time;
\set ON_ERROR_STOP 1

-------------------------------------------
--- Test time_bucket with month periods ---
-------------------------------------------

SET datestyle TO ISO;

SELECT
  time::date,
  time_bucket('1 month', time::date) AS "1m",
  time_bucket('2 month', time::date) AS "2m",
  time_bucket('3 month', time::date) AS "3m",
  time_bucket('1 month', time::date, '2000-02-01'::date) AS "1m origin",
  time_bucket('2 month', time::date, '2000-02-01'::date) AS "2m origin",
  time_bucket('3 month', time::date, '2000-02-01'::date) AS "3m origin"
FROM generate_series('1990-01-03'::date,'1990-06-03'::date,'1month'::interval) time;

SELECT
  time,
  time_bucket('1 month', time) AS "1m",
  time_bucket('2 month', time) AS "2m",
  time_bucket('3 month', time) AS "3m",
  time_bucket('1 month', time, '2000-02-01'::timestamp) AS "1m origin",
  time_bucket('2 month', time, '2000-02-01'::timestamp) AS "2m origin",
  time_bucket('3 month', time, '2000-02-01'::timestamp) AS "3m origin"
FROM generate_series('1990-01-03'::timestamp,'1990-06-03'::timestamp,'1month'::interval) time;

SELECT
  time,
  time_bucket('1 month', time) AS "1m",
  time_bucket('2 month', time) AS "2m",
  time_bucket('3 month', time) AS "3m",
  time_bucket('1 month', time, '2000-02-01'::timestamptz) AS "1m origin",
  time_bucket('2 month', time, '2000-02-01'::timestamptz) AS "2m origin",
  time_bucket('3 month', time, '2000-02-01'::timestamptz) AS "3m origin"
FROM generate_series('1990-01-03'::timestamptz,'1990-06-03'::timestamptz,'1month'::interval) time;

---------------------------------------
--- Test time_bucket with timezones ---
---------------------------------------

-- test NULL args
SELECT
time_bucket(NULL::interval,now(),'Europe/Berlin'),
time_bucket('1day',NULL::timestamptz,'Europe/Berlin'),
time_bucket('1day',now(),NULL::text),
time_bucket('1day','2020-02-03','Europe/Berlin',NULL),
time_bucket('1day','2020-02-03','Europe/Berlin','2020-04-01',NULL),
time_bucket('1day','2020-02-03','Europe/Berlin',NULL,NULL),
time_bucket('1day','2020-02-03','Europe/Berlin',"offset":=NULL::interval),
time_bucket('1day','2020-02-03','Europe/Berlin',origin:=NULL::timestamptz);

SET datestyle TO ISO;
SELECT
  time_bucket('1day', ts) AS "UTC",
  time_bucket('1day', ts, 'Europe/Berlin') AS "Berlin",
  time_bucket('1day', ts, 'Europe/London') AS "London",
  time_bucket('1day', ts, 'America/New_York') AS "New York",
  time_bucket('1day', ts, 'PST') AS "PST",
  time_bucket('1day', ts, current_setting('timezone')) AS "current"

FROM generate_series('1999-12-31 17:00'::timestamptz,'2000-01-02 3:00'::timestamptz, '1hour'::interval) ts;

SELECT
  time_bucket('1month', ts) AS "UTC",
  time_bucket('1month', ts, 'Europe/Berlin') AS "Berlin",
  time_bucket('1month', ts, 'America/New_York') AS "New York",
  time_bucket('1month', ts, current_setting('timezone')) AS "current",
  time_bucket('2month', ts, current_setting('timezone')) AS "2m",
  time_bucket('2month', ts, current_setting('timezone'), '2000-02-01'::timestamp) AS "2m origin",
  time_bucket('2month', ts, current_setting('timezone'), "offset":='14 day'::interval) AS "2m offset",
  time_bucket('2month', ts, current_setting('timezone'), '2000-02-01'::timestamp, '7 day'::interval) AS "2m offset + origin"

FROM generate_series('1999-12-01'::timestamptz,'2000-09-01'::timestamptz, '9 day'::interval) ts;

RESET datestyle;

------------------------------------------------------------
--- Test timescaledb_experimental.time_bucket_ng function --
------------------------------------------------------------

-- not supported functionality
\set ON_ERROR_STOP 0
SELECT timescaledb_experimental.time_bucket_ng('1 hour', '2001-02-03' :: date) AS result;
SELECT timescaledb_experimental.time_bucket_ng('0 days', '2001-02-03' :: date) AS result;
SELECT timescaledb_experimental.time_bucket_ng('1 month', '2001-02-03' :: date, origin => '2000-01-02') AS result;
SELECT timescaledb_experimental.time_bucket_ng('1 month', '2000-01-02' :: date, origin => '2001-01-01') AS result;
SELECT timescaledb_experimental.time_bucket_ng('1 day', '2000-01-02' :: date, origin => '2001-01-01') AS result;
SELECT timescaledb_experimental.time_bucket_ng('1 month 3 hours', '2021-11-22' :: timestamp) AS result;
-- timestamp is less than the default 'origin' value
SELECT timescaledb_experimental.time_bucket_ng('1 day', '1999-01-01 12:34:56 MSK' :: timestamptz, timezone => 'MSK');
-- 'origin' in Europe/Moscow timezone is not the first day of the month at given time zone (UTC in this case)
select timescaledb_experimental.time_bucket_ng('1 month', '2021-07-12 12:34:56 Europe/Moscow' :: timestamptz, origin => '2021-06-01 00:00:00 Europe/Moscow' :: timestamptz, timezone => 'UTC');
\set ON_ERROR_STOP 1

-- wrappers
SELECT timescaledb_experimental.time_bucket_ng('1 year', '2021-11-22' :: timestamp) AS result;
SELECT timescaledb_experimental.time_bucket_ng('1 year', '2021-11-22' :: timestamptz) AS result;
SELECT timescaledb_experimental.time_bucket_ng('1 year', '2021-11-22' :: timestamp, origin => '2021-06-01') AS result;
SELECT timescaledb_experimental.time_bucket_ng('1 year', '2021-11-22' :: timestamptz, origin => '2021-06-01') AS result;

-- null argument
SELECT timescaledb_experimental.time_bucket_ng('1 year', null :: date) AS result;
SELECT timescaledb_experimental.time_bucket_ng('1 year', null :: timestamp) AS result;
SELECT timescaledb_experimental.time_bucket_ng('1 year', null :: timestamptz) AS result;
SELECT timescaledb_experimental.time_bucket_ng('1 year', null :: timestamptz, timezone => 'Europe/Moscow') AS result;
SELECT timescaledb_experimental.time_bucket_ng('1 year', null :: date, origin => '2021-06-01') AS result;
SELECT timescaledb_experimental.time_bucket_ng('1 year', null :: timestamp, origin => '2021-06-01') AS result;
SELECT timescaledb_experimental.time_bucket_ng('1 year', null :: timestamptz, origin => '2021-06-01') AS result;
SELECT timescaledb_experimental.time_bucket_ng('1 year', null :: timestamptz, origin => '2021-06-01', timezone => 'Europe/Moscow') AS result;

-- null interval
SELECT timescaledb_experimental.time_bucket_ng(null, '2021-07-12' :: date) AS result;
SELECT timescaledb_experimental.time_bucket_ng(null, '2021-07-12 12:34:56' :: timestamp) AS result;
SELECT timescaledb_experimental.time_bucket_ng(null, '2021-07-12 12:34:56' :: timestamptz) AS result;
SELECT timescaledb_experimental.time_bucket_ng(null, '2021-07-12 12:34:56' :: timestamptz, 'Europe/Moscow') AS result;
SELECT timescaledb_experimental.time_bucket_ng(null, '2021-07-12' :: date, origin => '2021-06-01') AS result;
SELECT timescaledb_experimental.time_bucket_ng(null, '2021-07-12 12:34:56' :: timestamp, origin => '2021-06-01') AS result;
SELECT timescaledb_experimental.time_bucket_ng(null, '2021-07-12 12:34:56' :: timestamptz, origin => '2021-06-01') AS result;
SELECT timescaledb_experimental.time_bucket_ng(null, '2021-07-12 12:34:56' :: timestamptz, origin => '2021-06-01', timezone => 'Europe/Moscow') AS result;

-- null origin
SELECT timescaledb_experimental.time_bucket_ng('1 year', '2021-07-12' :: date, origin => null) AS result;
SELECT timescaledb_experimental.time_bucket_ng('1 year', '2021-07-12 12:34:56' :: timestamp, origin => null) AS result;
SELECT timescaledb_experimental.time_bucket_ng('1 year', '2021-07-12 12:34:56' :: timestamptz, origin => null) AS result;
SELECT timescaledb_experimental.time_bucket_ng('1 year', '2021-07-12 12:34:56' :: timestamptz, origin => null, timezone => 'Europe/Moscow') AS result;

-- infinity argument
SELECT timescaledb_experimental.time_bucket_ng('1 year', 'infinity' :: date) AS result;
SELECT timescaledb_experimental.time_bucket_ng('1 year', 'infinity' :: timestamp) AS result;
SELECT timescaledb_experimental.time_bucket_ng('1 year', 'infinity' :: timestamptz) AS result;
SELECT timescaledb_experimental.time_bucket_ng('1 year', 'infinity' :: timestamptz, timezone => 'Europe/Moscow') AS result;
SELECT timescaledb_experimental.time_bucket_ng('1 year', 'infinity' :: date, origin => '2021-06-01') AS result;
SELECT timescaledb_experimental.time_bucket_ng('1 year', 'infinity' :: timestamp, origin => '2021-06-01') AS result;
SELECT timescaledb_experimental.time_bucket_ng('1 year', 'infinity' :: timestamptz, origin => '2021-06-01') AS result;
SELECT timescaledb_experimental.time_bucket_ng('1 year', 'infinity' :: timestamptz, origin => '2021-06-01', timezone => 'Europe/Moscow') AS result;
-- test for specific code path: hours/minutes/seconds interval and timestamp argument
SELECT timescaledb_experimental.time_bucket_ng('12 hours', 'infinity' :: timestamp) AS result;
SELECT timescaledb_experimental.time_bucket_ng('12 hours', 'infinity' :: timestamp, origin => '2021-06-01') AS result;

-- infinite origin
SELECT timescaledb_experimental.time_bucket_ng('1 year', '2021-07-12' :: date, origin => 'infinity') AS result;
SELECT timescaledb_experimental.time_bucket_ng('1 year', '2021-07-12 12:34:56' :: timestamp, origin => 'infinity') AS result;
SELECT timescaledb_experimental.time_bucket_ng('1 year', '2021-07-12 12:34:56' :: timestamptz, origin => 'infinity') AS result;
SELECT timescaledb_experimental.time_bucket_ng('1 year', '2021-07-12 12:34:56' :: timestamptz, origin => 'infinity', timezone => 'Europe/Moscow') AS result;
-- test for specific code path: hours/minutes/seconds interval and timestamp argument
SELECT timescaledb_experimental.time_bucket_ng('12 hours', '2021-07-12 12:34:56' :: timestamp, origin => 'infinity') AS result;

-- test for invalid timezone argument
SELECT timescaledb_experimental.time_bucket_ng('1 year', '2021-07-12 12:34:56' :: timestamptz, timezone => null) AS result;
\set ON_ERROR_STOP 0
SELECT timescaledb_experimental.time_bucket_ng('1 year', '2021-07-12 12:34:56' :: timestamptz, timezone => 'Europe/Ololondon') AS result;
\set ON_ERROR_STOP 1

-- Make sure time_bucket_ng() supports seconds, minutes, and hours.
-- We happen to know that the internal implementation is the same
-- as for time_bucket(), thus there is no reason to execute all the tests
-- we already have for time_bucket(). These two functions will most likely
-- be merged eventually anyway.
SELECT timescaledb_experimental.time_bucket_ng('30 seconds', '2021-07-12 12:34:56' :: timestamp) AS result;
SELECT timescaledb_experimental.time_bucket_ng('15 minutes', '2021-07-12 12:34:56' :: timestamp) AS result;
SELECT timescaledb_experimental.time_bucket_ng('6 hours', '2021-07-12 12:34:56' :: timestamp) AS result;

-- Same as above, but with provided 'origin' argument.
SELECT timescaledb_experimental.time_bucket_ng('30 seconds', '2021-07-12 12:34:56' :: timestamp, origin => '2021-07-12 12:10:00') AS result;
SELECT timescaledb_experimental.time_bucket_ng('15 minutes', '2021-07-12 12:34:56' :: timestamp, origin => '2021-07-12 12:10:00') AS result;
SELECT timescaledb_experimental.time_bucket_ng('6 hours', '2021-07-12 12:34:56' :: timestamp, origin => '2021-07-12 12:10:00') AS result;

-- N days / weeks buckets
SELECT  to_char(d, 'YYYY-MM-DD') AS d,
        to_char(timescaledb_experimental.time_bucket_ng('1 day', d),  'YYYY-MM-DD') AS d1,
        to_char(timescaledb_experimental.time_bucket_ng('2 days', d),  'YYYY-MM-DD') AS d2,
        to_char(timescaledb_experimental.time_bucket_ng('3 days', d),  'YYYY-MM-DD') AS d3,
        to_char(timescaledb_experimental.time_bucket_ng('1 week', d),  'YYYY-MM-DD') AS w1,
        to_char(timescaledb_experimental.time_bucket_ng('1 week 2 days', d),  'YYYY-MM-DD') AS w1d2
FROM generate_series('2020-01-01' :: date, '2020-01-12', '1 day') AS ts,
     unnest(array[ts :: date]) AS d;

-- N days / weeks buckets with given 'origin'
SELECT  to_char(d, 'YYYY-MM-DD') AS d,
        to_char(timescaledb_experimental.time_bucket_ng('1 day', d, origin => '2020-01-01'), 'YYYY-MM-DD') AS d1,
        to_char(timescaledb_experimental.time_bucket_ng('2 days', d, origin => '2020-01-01'), 'YYYY-MM-DD') AS d2,
        to_char(timescaledb_experimental.time_bucket_ng('3 days', d, origin => '2020-01-01'), 'YYYY-MM-DD') AS d3,
        to_char(timescaledb_experimental.time_bucket_ng('1 week', d, origin => '2020-01-01'), 'YYYY-MM-DD') AS w1,
        to_char(timescaledb_experimental.time_bucket_ng('1 week 2 days', d, origin => '2020-01-01'), 'YYYY-MM-DD') AS w1d2
FROM generate_series('2020-01-01' :: date, '2020-01-12', '1 day') AS ts,
     unnest(array[ts :: date]) AS d;

-- N month buckets
SELECT  to_char(d, 'YYYY-MM-DD') AS d,
        to_char(timescaledb_experimental.time_bucket_ng('1 month', d), 'YYYY-MM-DD') AS m1,
        to_char(timescaledb_experimental.time_bucket_ng('2 month', d), 'YYYY-MM-DD') AS m2,
        to_char(timescaledb_experimental.time_bucket_ng('3 month', d), 'YYYY-MM-DD') AS m3,
        to_char(timescaledb_experimental.time_bucket_ng('4 month', d), 'YYYY-MM-DD') AS m4,
        to_char(timescaledb_experimental.time_bucket_ng('5 month', d), 'YYYY-MM-DD') AS m5
FROM generate_series('2020-01-01' :: date, '2020-12-01', '1 month') AS ts,
     unnest(array[ts :: date]) AS d;

-- N month buckets with given 'origin'
SELECT  to_char(d, 'YYYY-MM-DD') AS d,
        to_char(timescaledb_experimental.time_bucket_ng('1 month', d, origin => '2019-05-01'), 'YYYY-MM-DD') AS m1,
        to_char(timescaledb_experimental.time_bucket_ng('2 month', d, origin => '2019-05-01'), 'YYYY-MM-DD') AS m2,
        to_char(timescaledb_experimental.time_bucket_ng('3 month', d, origin => '2019-05-01'), 'YYYY-MM-DD') AS m3,
        to_char(timescaledb_experimental.time_bucket_ng('4 month', d, origin => '2019-05-01'), 'YYYY-MM-DD') AS m4,
        to_char(timescaledb_experimental.time_bucket_ng('5 month', d, origin => '2019-05-01'), 'YYYY-MM-DD') AS m5
FROM generate_series('2020-01-01' :: date, '2020-12-01', '1 month') AS ts,
     unnest(array[ts :: date]) AS d;

-- N years / N years, M month buckets
SELECT  to_char(d, 'YYYY-MM-DD') AS d,
        to_char(timescaledb_experimental.time_bucket_ng('1 year', d), 'YYYY-MM-DD') AS y1,
        to_char(timescaledb_experimental.time_bucket_ng('1 year 6 month', d), 'YYYY-MM-DD') AS y1m6,
        to_char(timescaledb_experimental.time_bucket_ng('2 years', d), 'YYYY-MM-DD') AS y2,
        to_char(timescaledb_experimental.time_bucket_ng('2 years 6 month', d), 'YYYY-MM-DD') AS y2m6,
        to_char(timescaledb_experimental.time_bucket_ng('3 years', d), 'YYYY-MM-DD') AS y3
FROM generate_series('2015-01-01' :: date, '2020-12-01', '6 month') AS ts,
     unnest(array[ts :: date]) AS d;

-- N years / N years, M month buckets with given 'origin'
SELECT  to_char(d, 'YYYY-MM-DD') AS d,
        to_char(timescaledb_experimental.time_bucket_ng('1 year', d, origin => '2000-06-01'), 'YYYY-MM-DD') AS y1,
        to_char(timescaledb_experimental.time_bucket_ng('1 year 6 month', d, origin => '2000-06-01'), 'YYYY-MM-DD') AS y1m6,
        to_char(timescaledb_experimental.time_bucket_ng('2 years', d, origin => '2000-06-01'), 'YYYY-MM-DD') AS y2,
        to_char(timescaledb_experimental.time_bucket_ng('2 years 6 month', d, origin => '2000-06-01'), 'YYYY-MM-DD') AS y2m6,
        to_char(timescaledb_experimental.time_bucket_ng('3 years', d, origin => '2000-06-01'), 'YYYY-MM-DD') AS y3
FROM generate_series('2015-01-01' :: date, '2020-12-01', '6 month') AS ts,
     unnest(array[ts :: date]) AS d;

-- Test timezones support with different bucket sizes
BEGIN;
-- Timestamptz type is displayed in the session timezone.
-- To get consistent results during the test we temporary set the session
-- timezone to the known one.
SET TIME ZONE '+00';

-- Moscow is UTC+3 in the year 2021. Let's say you are dealing with '1 day' bucket.
-- In order to calculate the beginning of the bucket you have to take LOCAL
-- Moscow time and throw away the time. You will get the midnight. The new day
-- starts 3 hours EARLIER in Moscow than in UTC+0 time zone, thus resulting
-- timestamp will be 3 hours LESS than for UTC+0.
SELECT bs, tz, to_char(ts_out, 'YYYY-MM-DD HH24:MI:SS TZ') as res
FROM unnest(array['Europe/Moscow', 'UTC']) as tz,
     unnest(array['12 hours', '1 day', '1 month', '4 months', '1 year']) as bs,
     unnest(array['2021-07-12 12:34:56 Europe/Moscow' :: timestamptz]) as ts_in,
     unnest(array[timescaledb_experimental.time_bucket_ng(bs :: interval, ts_in, timezone => tz)]) as ts_out
ORDER BY tz, bs :: interval;

-- Same as above, but with 'origin'
SELECT bs, tz, to_char(ts_out, 'YYYY-MM-DD HH24:MI:SS TZ') as res
FROM unnest(array['Europe/Moscow']) as tz,
     unnest(array['12 hours', '1 day', '1 month', '4 months', '1 year']) as bs,
     unnest(array['2021-07-12 12:34:56 Europe/Moscow' :: timestamptz]) as ts_in,
     unnest(array['2021-06-01 00:00:00 Europe/Moscow' :: timestamptz]) as origin_in,
     unnest(array[timescaledb_experimental.time_bucket_ng(bs :: interval, ts_in, origin => origin_in, timezone => tz)]) as ts_out
ORDER BY tz, bs :: interval;

-- Overwritten origin allows to work with dates earlier than the default origin
SELECT to_char(timescaledb_experimental.time_bucket_ng('1 day', '1999-01-01 12:34:56 MSK' :: timestamptz, origin => '1900-01-01 00:00:00 MSK', timezone => 'MSK'), 'YYYY-MM-DD HH24:MI:SS TZ');

-- Restore previously used time zone.
ROLLBACK;

-------------------------------------
--- Test time input functions --
-------------------------------------
\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE OR REPLACE FUNCTION test.interval_to_internal(coltype REGTYPE, value ANYELEMENT = NULL::BIGINT) RETURNS BIGINT
AS :MODULE_PATHNAME, 'ts_dimension_interval_to_internal_test' LANGUAGE C VOLATILE;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

SELECT test.interval_to_internal('TIMESTAMP'::regtype, INTERVAL '1 day');
SELECT test.interval_to_internal('TIMESTAMP'::regtype, 86400000000);
---should give warning
SELECT test.interval_to_internal('TIMESTAMP'::regtype, 86400);

SELECT test.interval_to_internal('TIMESTAMP'::regtype);
SELECT test.interval_to_internal('BIGINT'::regtype, 2147483649::bigint);

\set VERBOSITY terse
\set ON_ERROR_STOP 0
SELECT test.interval_to_internal('INT'::regtype);
SELECT test.interval_to_internal('INT'::regtype, 2147483649::bigint);
SELECT test.interval_to_internal('SMALLINT'::regtype, 32768::bigint);
SELECT test.interval_to_internal('TEXT'::regtype, 32768::bigint);
SELECT test.interval_to_internal('INT'::regtype, INTERVAL '1 day');
\set ON_ERROR_STOP 1
