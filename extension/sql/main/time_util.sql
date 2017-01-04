-- This file contains utilities for time conversion. 
-- Time can be represented in a hypertable as an int* (bigint/integer/smallint) or as a timestamp type (
-- with or without timezones). In or metatables and other internal systems all time values are stored as bigint.
-- Converting from int* columns to internal representation is a cast to bigint. 
-- Converting from timestamps to internal representation is conversion to epoch (in microseconds). 

-- Gets the sql code for extracting the internal (bigint) time value from the identifier with the given field_type. 
CREATE OR REPLACE FUNCTION  _sysinternal.extract_time_sql(
    identifier      text,
    field_type      REGTYPE
)
    RETURNS text LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
BEGIN
    CASE field_type 
      WHEN 'BIGINT'::regtype, 'INTEGER'::regtype, 'SMALLINT'::regtype THEN
        RETURN format('%s::bigint', identifier); --scale determined by user.
      WHEN 'TIMESTAMP'::regtype, 'TIMESTAMPTZ'::regtype THEN
        RETURN format('((EXTRACT(epoch FROM %s)*1e6)::bigint)', identifier); --microseconds
    END CASE;
END
$BODY$;

-- Gets the sql code for representing the literal for the given time value (in the internal representation) as the field_type.
CREATE OR REPLACE FUNCTION  _sysinternal.time_literal_sql(
    time_value      BIGINT,
    field_type      REGTYPE
)
    RETURNS text LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
BEGIN
    IF time_value IS NULL THEN
      RETURN format('%L', NULL);
    END IF;
    CASE field_type 
      WHEN 'BIGINT'::regtype, 'INTEGER'::regtype, 'SMALLINT'::regtype THEN
        RETURN format('%L', time_value); --scale determined by user.
      WHEN 'TIMESTAMP'::regtype, 'TIMESTAMPTZ'::regtype THEN
        --assume time_value is in microsec
        RETURN format('%2$s %1$L', to_timestamp(time_value::double precision / 1e6), field_type); --microseconds
    END CASE;
END
$BODY$;


