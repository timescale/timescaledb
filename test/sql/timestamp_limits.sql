-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE OR REPLACE FUNCTION test.min_pg_timestamptz() RETURNS TIMESTAMPTZ
AS :MODULE_PATHNAME, 'ts_timestamptz_pg_min' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION test.end_pg_timestamptz() RETURNS TIMESTAMPTZ
AS :MODULE_PATHNAME, 'ts_timestamptz_pg_end' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION test.min_ts_timestamptz() RETURNS TIMESTAMPTZ
AS :MODULE_PATHNAME, 'ts_timestamptz_min' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION test.end_ts_timestamptz() RETURNS TIMESTAMPTZ
AS :MODULE_PATHNAME, 'ts_timestamptz_end' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION test.min_pg_date() RETURNS DATE
AS :MODULE_PATHNAME, 'ts_date_pg_min' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION test.end_pg_date() RETURNS DATE
AS :MODULE_PATHNAME, 'ts_date_pg_end' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION test.min_ts_date() RETURNS DATE
AS :MODULE_PATHNAME, 'ts_date_min' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION test.end_ts_date() RETURNS DATE
AS :MODULE_PATHNAME, 'ts_date_end' LANGUAGE C VOLATILE;
SET ROLE :ROLE_DEFAULT_PERM_USER;

--show PG and TimescaleDB-specific time boundaries. Note that we
--internally convert to UNIX epoch, which restricts the range of
--supported timestamps.
SET datestyle TO 'ISO,YMD';
SELECT test.min_pg_timestamptz(), test.end_pg_timestamptz(), test.min_ts_timestamptz(), test.end_ts_timestamptz();

--TimescaleDB-specific dates should be consistent with timestamps
--since we internally first convert dates to timestamps and then to
--UNIX epoch.
SELECT test.min_pg_date(), test.end_pg_date(), test.min_ts_date(), test.end_ts_date();
RESET datestyle;
