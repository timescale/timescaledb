-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE OR REPLACE FUNCTION test.min_pg_timestamptz() RETURNS TIMESTAMPTZ
AS :MODULE_PATHNAME, 'ts_timestamptz_pg_min' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION test.end_pg_timestamptz() RETURNS TIMESTAMPTZ
AS :MODULE_PATHNAME, 'ts_timestamptz_pg_end' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION test.min_ts_timestamptz() RETURNS TIMESTAMPTZ
AS :MODULE_PATHNAME, 'ts_timestamptz_min' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION test.end_ts_timestamptz() RETURNS TIMESTAMPTZ
AS :MODULE_PATHNAME, 'ts_timestamptz_end' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION test.internal_min_ts_timestamptz() RETURNS BIGINT
AS :MODULE_PATHNAME, 'ts_timestamptz_internal_min' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION test.internal_end_ts_timestamptz() RETURNS BIGINT
AS :MODULE_PATHNAME, 'ts_timestamptz_internal_end' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION test.min_pg_timestamp() RETURNS TIMESTAMP
AS :MODULE_PATHNAME, 'ts_timestamp_pg_min' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION test.end_pg_timestamp() RETURNS TIMESTAMP
AS :MODULE_PATHNAME, 'ts_timestamp_pg_end' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION test.min_ts_timestamp() RETURNS TIMESTAMP
AS :MODULE_PATHNAME, 'ts_timestamp_min' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION test.end_ts_timestamp() RETURNS TIMESTAMP
AS :MODULE_PATHNAME, 'ts_timestamp_end' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION test.internal_min_ts_timestamp() RETURNS BIGINT
AS :MODULE_PATHNAME, 'ts_timestamp_internal_min' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION test.internal_end_ts_timestamp() RETURNS BIGINT
AS :MODULE_PATHNAME, 'ts_timestamp_internal_end' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION test.min_pg_date() RETURNS DATE
AS :MODULE_PATHNAME, 'ts_date_pg_min' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION test.end_pg_date() RETURNS DATE
AS :MODULE_PATHNAME, 'ts_date_pg_end' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION test.min_ts_date() RETURNS DATE
AS :MODULE_PATHNAME, 'ts_date_min' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION test.end_ts_date() RETURNS DATE
AS :MODULE_PATHNAME, 'ts_date_end' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION test.internal_min_ts_date() RETURNS BIGINT
AS :MODULE_PATHNAME, 'ts_date_internal_min' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION test.internal_end_ts_date() RETURNS BIGINT
AS :MODULE_PATHNAME, 'ts_date_internal_end' LANGUAGE C VOLATILE;

SET ROLE :ROLE_DEFAULT_PERM_USER;

--show PG and TimescaleDB-specific time boundaries. Note that we
--internally convert to UNIX epoch, which restricts the range of
--supported timestamps.
SET datestyle TO 'ISO,YMD';
\x

--To display end timestamps (which aren't valid timestamps), we need
--to be at UTC to avoid timezone conversion which would fail with
--out-of-range error. Being at UTC also makes it easier to compare
--TIMESTAMP and TIMESTAMP WITH TIME ZONE.
SET timezone TO 'UTC';

-- MIN values (PostgreSQL).
SELECT test.min_pg_timestamptz(),
       test.min_pg_timestamp(),
       test.min_pg_date();

-- MIN values (TimescaleDB).
SELECT test.min_ts_timestamptz(),
       test.min_ts_timestamp(),
       test.min_ts_date(),
       _timescaledb_functions.to_timestamp(test.internal_min_ts_timestamptz()) AS min_ts_internal_timestamptz,
       _timescaledb_functions.to_timestamp_without_timezone(test.internal_min_ts_timestamp()) AS min_ts_internal_timestamp,
       _timescaledb_functions.to_date(test.internal_min_ts_date()) AS min_ts_internal_date;

-- END values (PostgreSQL). Note that and values aren't valid
-- timestamps or dates (since, e.g., END_TIMESTAMP is exclusive). It
-- is possible to display them at UTC since no conversion is made.
SELECT test.end_pg_timestamptz(),
       test.end_pg_timestamp(),
       test.end_pg_date();

-- END values (TimescaleDB). Note that we convert DATE to TIMESTAMP
-- internally, and that limits the end to that of timestamp.
SELECT test.end_ts_timestamptz(),
       test.end_ts_timestamp(),
       test.end_ts_date(),
       _timescaledb_functions.to_timestamp(test.internal_end_ts_timestamptz()) AS end_ts_internal_timestamptz,
       _timescaledb_functions.to_timestamp_without_timezone(test.internal_end_ts_timestamp()) AS end_ts_internal_timestamp,
       _timescaledb_functions.to_date(test.internal_end_ts_date()) AS end_ts_internal_date;

--Test insert of time values close to or at limits
--Suitable constraints should be created on chunks
CREATE TABLE smallint_table(time smallint);
SELECT table_name FROM create_hypertable('smallint_table', 'time', chunk_time_interval=>10);
INSERT INTO smallint_table VALUES (-32768), (32767);

SELECT pg_get_constraintdef(c.oid)
FROM show_chunks('smallint_table') chunk, pg_constraint c
WHERE c.conrelid = chunk
AND c.contype = 'c' ORDER BY chunk;

CREATE TABLE int_table(time int);
SELECT table_name FROM create_hypertable('int_table', 'time', chunk_time_interval=>10);
INSERT INTO int_table VALUES (-2147483648), (2147483647);

SELECT pg_get_constraintdef(c.oid)
FROM show_chunks('int_table') chunk, pg_constraint c
WHERE c.conrelid = chunk
AND c.contype = 'c' ORDER BY chunk;

CREATE TABLE bigint_table(time bigint);
SELECT table_name FROM create_hypertable('bigint_table', 'time', chunk_time_interval=>10);
INSERT INTO bigint_table VALUES (-9223372036854775808), (9223372036854775807);

SELECT pg_get_constraintdef(c.oid)
FROM show_chunks('bigint_table') chunk, pg_constraint c
WHERE c.conrelid = chunk
AND c.contype = 'c' ORDER BY chunk;

CREATE TABLE date_table(time date);
SELECT table_name FROM create_hypertable('date_table', 'time');
INSERT INTO date_table VALUES (test.min_ts_date()), (test.end_ts_date() - INTERVAL '1 day');
-- Test out-of-range dates
\set ON_ERROR_STOP 0
INSERT INTO date_table VALUES (test.min_ts_date() - INTERVAL '1 day');
INSERT INTO date_table VALUES (test.end_ts_date());
\set ON_ERROR_STOP 1

SELECT pg_get_constraintdef(c.oid)
FROM show_chunks('date_table') chunk, pg_constraint c
WHERE c.conrelid = chunk
AND c.contype = 'c' ORDER BY chunk;

CREATE TABLE timestamp_table(time timestamp);
SELECT table_name FROM create_hypertable('timestamp_table', 'time');

INSERT INTO timestamp_table VALUES (test.min_ts_timestamp());
INSERT INTO timestamp_table VALUES (test.end_ts_timestamp() - INTERVAL '1 microsecond');

-- Test out-of-range timestamps
\set ON_ERROR_STOP 0
INSERT INTO timestamp_table VALUES (test.min_ts_timestamp() - INTERVAL '1 microsecond');
INSERT INTO timestamp_table VALUES (test.end_ts_timestamp());
\set ON_ERROR_STOP 1

SELECT pg_get_constraintdef(c.oid)
FROM show_chunks('timestamp_table') chunk, pg_constraint c
WHERE c.conrelid = chunk
AND c.contype = 'c' ORDER BY chunk;

CREATE TABLE timestamptz_table(time timestamp);
SELECT table_name FROM create_hypertable('timestamptz_table', 'time');

-- Need to be at UTC for this to work for timestamp with time zone
INSERT INTO timestamptz_table VALUES (test.min_ts_timestamptz());
INSERT INTO timestamptz_table VALUES (test.end_ts_timestamptz() - INTERVAL '1 microsecond');

-- Test out-of-range timestamps
\set ON_ERROR_STOP 0
INSERT INTO timestamptz_table VALUES (test.min_ts_timestamptz() - INTERVAL '1 microsecond');
INSERT INTO timestamptz_table VALUES (test.end_ts_timestamptz());
\set ON_ERROR_STOP 1

SELECT pg_get_constraintdef(c.oid)
FROM show_chunks('timestamptz_table') chunk, pg_constraint c
WHERE c.conrelid = chunk
AND c.contype = 'c' ORDER BY chunk;

RESET datestyle;
RESET timezone;
