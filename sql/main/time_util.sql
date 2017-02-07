-- This file contains utilities for time conversion.
-- Time can be represented in a hypertable as an int* (bigint/integer/smallint) or as a timestamp type (
-- with or without timezones). In or metatables and other internal systems all time values are stored as bigint.
-- Converting from int* columns to internal representation is a cast to bigint.
-- Converting from timestamps to internal representation is conversion to epoch (in microseconds).

-- Gets the sql code for extracting the internal (bigint) time value from the identifier with the given column_type.
CREATE OR REPLACE FUNCTION _iobeamdb_internal.extract_time_sql(
    identifier      text,
    column_type     REGTYPE
)
    RETURNS text LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
BEGIN
    CASE column_type
      WHEN 'BIGINT'::regtype, 'INTEGER'::regtype, 'SMALLINT'::regtype THEN
        RETURN format('%s::bigint', identifier); --scale determined by user.
      WHEN 'TIMESTAMP'::regtype, 'TIMESTAMPTZ'::regtype THEN
        RETURN format('((EXTRACT(epoch FROM %s::timestamptz)*1e6)::bigint)', identifier); --microseconds since UTC epoch
    END CASE;
END
$BODY$;

CREATE OR REPLACE FUNCTION _iobeamdb_internal.time_value_to_timestamp(
    time_value BIGINT
)
    RETURNS TIMESTAMPTZ LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    seconds                 BIGINT;
    microseconds            BIGINT;
    microseconds_interval   INTERVAL;
    timestamp_value         TIMESTAMPTZ;
BEGIN
    seconds := (time_value / 1e6)::bigint;
    microseconds := time_value - (seconds * 1e6);
    microseconds_interval := make_interval(secs => microseconds / 1e6);
    SELECT to_timestamp(seconds) + microseconds_interval INTO timestamp_value;
    RETURN timestamp_value;
END
$BODY$;

-- Gets the sql code for representing the literal for the given time value (in the internal representation) as the column_type.
CREATE OR REPLACE FUNCTION _iobeamdb_internal.time_literal_sql(
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
        RETURN format('%L', time_value); --scale determined by user.
      WHEN 'TIMESTAMP'::regtype, 'TIMESTAMPTZ'::regtype THEN
        --assume time_value is in microsec
        RETURN format('%2$s %1$L', _iobeamdb_internal.time_value_to_timestamp(time_value), column_type); --microseconds
    END CASE;
END
$BODY$;
