-- This file contains utilities for time conversion.
CREATE OR REPLACE FUNCTION _timescaledb_internal.to_microseconds(ts TIMESTAMPTZ) RETURNS BIGINT
    AS '@MODULE_PATHNAME@', 'pg_timestamp_to_microseconds' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.to_unix_microseconds(ts TIMESTAMPTZ) RETURNS BIGINT
    AS '@MODULE_PATHNAME@', 'pg_timestamp_to_unix_microseconds' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.to_timestamp(unixtime_us BIGINT) RETURNS TIMESTAMPTZ
    AS '@MODULE_PATHNAME@', 'pg_unix_microseconds_to_timestamp' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.to_timestamp_pg(postgres_us BIGINT) RETURNS TIMESTAMPTZ
    AS '@MODULE_PATHNAME@', 'pg_microseconds_to_timestamp' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;



-- Time can be represented in a hypertable as an int* (bigint/integer/smallint) or as a timestamp type (
-- with or without timezones). In or metatables and other internal systems all time values are stored as bigint.
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
    END CASE;
END
$BODY$;

-- Convert a interval to microseconds.
CREATE OR REPLACE FUNCTION _timescaledb_internal.interval_to_usec(
       chunk_interval INTERVAL
)
RETURNS BIGINT LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS
$BODY$
    SELECT (int_sec * 1000000)::bigint from extract(epoch from chunk_interval) as int_sec;
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.time_interval_specification_to_internal(
    time_type                 REGTYPE,
    specification             anyelement,
    default_value             INTERVAL,
    field_name                TEXT,
    ignore_interval_too_small BOOLEAN = FALSE
)
RETURNS BIGINT LANGUAGE PLPGSQL AS
$BODY$
BEGIN
    IF time_type IN ('TIMESTAMP', 'TIMESTAMPTZ', 'DATE') THEN
        IF specification IS NULL THEN
            RETURN _timescaledb_internal.interval_to_usec(default_value);
        ELSIF pg_typeof(specification) IN ('INT'::regtype, 'SMALLINT'::regtype, 'BIGINT'::regtype) THEN
            IF NOT ignore_interval_too_small AND specification::BIGINT < _timescaledb_internal.interval_to_usec('1 second') THEN
                RAISE WARNING 'You specified a % of less than a second, make sure that this is what you intended', field_name
                USING HINT = 'specification is specified in microseconds';
            END IF;
            RETURN specification::BIGINT;
        ELSIF pg_typeof(specification) = 'INTERVAL'::regtype THEN
            RETURN _timescaledb_internal.interval_to_usec(specification);
        ELSE
            RAISE EXCEPTION '% needs to be an INTERVAL or integer type for TIMESTAMP, TIMESTAMPTZ, or DATE time columns', field_name
            USING ERRCODE = 'IO102';
        END IF;
    ELSIF time_type IN ('SMALLINT', 'INTEGER', 'BIGINT') THEN
        IF specification IS NULL THEN
            RAISE EXCEPTION '% needs to be explicitly set for time columns of type SMALLINT, INTEGER, and BIGINT', field_name
            USING ERRCODE = 'IO102';
        ELSIF pg_typeof(specification) IN ('INT'::regtype, 'SMALLINT'::regtype, 'BIGINT'::regtype) THEN
            --bounds check
            IF time_type = 'INTEGER'::REGTYPE AND specification > 2147483647 THEN
                RAISE EXCEPTION '% is too large for type INTEGER (max: 2147483647)', field_name
                USING ERRCODE = 'IO102';
            ELSIF time_type = 'SMALLINT'::REGTYPE AND specification > 65535 THEN
                RAISE EXCEPTION '% is too large for type SMALLINT (max: 65535)', field_name
                USING ERRCODE = 'IO102';
            END IF;
            RETURN specification::BIGINT;
        ELSE
            RAISE EXCEPTION '% needs to be an integer type for SMALLINT, INTEGER, and BIGINT time columns', field_name
            USING ERRCODE = 'IO102';
        END IF;
    ELSE
        RAISE EXCEPTION 'unknown time column type: %', time_type
        USING ERRCODE = 'IO102';
    END IF;
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.time_interval_specification_to_internal_with_default_time(
    time_type                 REGTYPE,
    specification             anyelement,
    field_name                TEXT,
    ignore_interval_too_small BOOLEAN = FALSE
)
RETURNS BIGINT LANGUAGE PLPGSQL AS
$BODY$
BEGIN
    RETURN _timescaledb_internal.time_interval_specification_to_internal(
        time_type, specification, INTERVAL '1 month', field_name, ignore_interval_too_small
    );
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.time_to_internal(time_element anyelement, time_type REGTYPE) RETURNS BIGINT
	AS '@MODULE_PATHNAME@', 'time_to_internal' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;
