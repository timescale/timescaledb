-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- This file contains utilities for time conversion.

-- Return the minimum for the type. For time types, it will be the
-- Unix timestamp in microseconds.
CREATE OR REPLACE FUNCTION _timescaledb_functions.get_internal_time_min(REGTYPE) RETURNS BIGINT
AS '@MODULE_PATHNAME@', 'ts_get_internal_time_min' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- Return the minimum for the type. For time types, it will be the
-- Unix timestamp in microseconds.
CREATE OR REPLACE FUNCTION _timescaledb_functions.get_internal_time_max(REGTYPE) RETURNS BIGINT
AS '@MODULE_PATHNAME@', 'ts_get_internal_time_max' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.to_unix_microseconds(ts TIMESTAMPTZ) RETURNS BIGINT
    AS '@MODULE_PATHNAME@', 'ts_pg_timestamp_to_unix_microseconds' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.to_timestamp(unixtime_us BIGINT) RETURNS TIMESTAMPTZ
    AS '@MODULE_PATHNAME@', 'ts_pg_unix_microseconds_to_timestamp' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.to_timestamp_without_timezone(unixtime_us BIGINT)
  RETURNS TIMESTAMP
  AS '@MODULE_PATHNAME@', 'ts_pg_unix_microseconds_to_timestamp'
  LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.to_date(unixtime_us BIGINT)
  RETURNS DATE
  AS '@MODULE_PATHNAME@', 'ts_pg_unix_microseconds_to_date'
  LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.to_interval(unixtime_us BIGINT) RETURNS INTERVAL
    AS '@MODULE_PATHNAME@', 'ts_pg_unix_microseconds_to_interval' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- Time can be represented in a hypertable as an int* (bigint/integer/smallint) or as a timestamp type (
-- with or without timezones). In metatables and other internal systems all time values are stored as bigint.
-- Converting from int* columns to internal representation is a cast to bigint.
-- Converting from timestamps to internal representation is conversion to epoch (in microseconds).

CREATE OR REPLACE FUNCTION _timescaledb_functions.interval_to_usec(
       chunk_interval INTERVAL
)
RETURNS BIGINT LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS
$BODY$
    SELECT (int_sec * 1000000)::bigint from extract(epoch from chunk_interval) as int_sec;
$BODY$ SET search_path TO pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION _timescaledb_functions.time_to_internal(time_val ANYELEMENT)
RETURNS BIGINT AS '@MODULE_PATHNAME@', 'ts_time_to_internal' LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_functions.cagg_watermark(hypertable_id INTEGER)
RETURNS INT8 AS '@MODULE_PATHNAME@', 'ts_continuous_agg_watermark' LANGUAGE C STABLE STRICT PARALLEL RESTRICTED;

CREATE OR REPLACE FUNCTION _timescaledb_functions.cagg_watermark_materialized(hypertable_id INTEGER)
RETURNS INT8 AS '@MODULE_PATHNAME@', 'ts_continuous_agg_watermark_materialized' LANGUAGE C STABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.subtract_integer_from_now( hypertable_relid REGCLASS, lag INT8 )
RETURNS INT8 AS '@MODULE_PATHNAME@', 'ts_subtract_integer_from_now' LANGUAGE C STABLE STRICT;

-- Convert integer UNIX timestamps in microsecond to a timestamp range.
CREATE OR REPLACE FUNCTION _timescaledb_functions.make_multirange_from_internal_time(
    base tstzrange, low_usec bigint, high_usec bigint
) RETURNS TSTZMULTIRANGE AS
$body$
  select multirange(tstzrange(_timescaledb_functions.to_timestamp(low_usec),
			      _timescaledb_functions.to_timestamp(high_usec)));
$body$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE
SET search_path TO pg_catalog, pg_temp;

-- Convert integer UNIX timestamps in microsecond to a timestamp range.
CREATE OR REPLACE FUNCTION _timescaledb_functions.make_multirange_from_internal_time(
    base TSRANGE, low_usec bigint, high_usec bigint
) RETURNS TSMULTIRANGE AS
$body$
  select multirange(tsrange(_timescaledb_functions.to_timestamp_without_timezone(low_usec),
			    _timescaledb_functions.to_timestamp_without_timezone(high_usec)));
$body$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE
SET search_path TO pg_catalog, pg_temp;

-- Helper function to construct a range given an existing type from
-- UNIX timestamps in microsecond precision.
CREATE OR REPLACE FUNCTION _timescaledb_functions.make_range_from_internal_time(
    base anyrange, low_usec anyelement, high_usec anyelement
) RETURNS anyrange
AS '@MODULE_PATHNAME@', 'ts_make_range_from_internal_time'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
