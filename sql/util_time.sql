-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- This file contains utilities for time conversion.
CREATE OR REPLACE FUNCTION _timescaledb_internal.to_unix_microseconds(ts TIMESTAMPTZ) RETURNS BIGINT
    AS '@MODULE_PATHNAME@', 'ts_pg_timestamp_to_unix_microseconds' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.to_timestamp(unixtime_us BIGINT) RETURNS TIMESTAMPTZ
    AS '@MODULE_PATHNAME@', 'ts_pg_unix_microseconds_to_timestamp' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.to_timestamp_without_timezone(unixtime_us BIGINT)
  RETURNS TIMESTAMP
  AS '@MODULE_PATHNAME@', 'ts_pg_unix_microseconds_to_timestamp'
  LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.to_date(unixtime_us BIGINT)
  RETURNS DATE
  AS '@MODULE_PATHNAME@', 'ts_pg_unix_microseconds_to_date'
  LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.to_interval(unixtime_us BIGINT) RETURNS INTERVAL
    AS '@MODULE_PATHNAME@', 'ts_pg_unix_microseconds_to_interval' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- Time can be represented in a hypertable as an int* (bigint/integer/smallint) or as a timestamp type (
-- with or without timezones). In metatables and other internal systems all time values are stored as bigint.
-- Converting from int* columns to internal representation is a cast to bigint.
-- Converting from timestamps to internal representation is conversion to epoch (in microseconds).

-- Gets the sql code for representing the literal for the given time value (in the internal representation) as the column_type.
CREATE OR REPLACE FUNCTION _timescaledb_internal.time_literal_sql(
    time_value      BIGINT,
    column_type     REGTYPE
)
    RETURNS text LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    ret text;
BEGIN
    IF time_value IS NULL THEN
        RETURN format('%L', NULL);
    END IF;
    CASE column_type
      WHEN 'BIGINT'::regtype, 'INTEGER'::regtype, 'SMALLINT'::regtype THEN
        RETURN format('%L', time_value); -- scale determined by user.
      WHEN 'TIMESTAMP'::regtype THEN
        --the time_value for timestamps w/o tz does not depend on local timezones. So perform at UTC.
        RETURN format('TIMESTAMP %1$L', timezone('UTC',_timescaledb_internal.to_timestamp(time_value))); -- microseconds
      WHEN 'TIMESTAMPTZ'::regtype THEN
        -- assume time_value is in microsec
        RETURN format('TIMESTAMPTZ %1$L', _timescaledb_internal.to_timestamp(time_value)); -- microseconds
      WHEN 'DATE'::regtype THEN
        RETURN format('%L', timezone('UTC',_timescaledb_internal.to_timestamp(time_value))::date);
      ELSE
         EXECUTE 'SELECT format(''%L'', $1::' || column_type::text || ')' into ret using time_value;
         RETURN ret;
    END CASE;
END
$BODY$ SET search_path TO pg_catalog;

CREATE OR REPLACE FUNCTION _timescaledb_internal.interval_to_usec(
       chunk_interval INTERVAL
)
RETURNS BIGINT LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS
$BODY$
    SELECT (int_sec * 1000000)::bigint from extract(epoch from chunk_interval) as int_sec;
$BODY$ SET search_path TO pg_catalog;

CREATE OR REPLACE FUNCTION _timescaledb_internal.time_to_internal(time_val ANYELEMENT)
RETURNS BIGINT AS '@MODULE_PATHNAME@', 'ts_time_to_internal' LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_internal.cagg_watermark(hypertable_id INTEGER)
RETURNS INT8 AS '@MODULE_PATHNAME@', 'ts_continuous_agg_watermark' LANGUAGE C STABLE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_internal.subtract_integer_from_now( hypertable_relid REGCLASS, lag INT8 )
RETURNS INT8 AS '@MODULE_PATHNAME@', 'ts_subtract_integer_from_now' LANGUAGE C STABLE STRICT;
