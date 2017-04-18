-- This file contains utilities for time conversion.

CREATE OR REPLACE FUNCTION _timescaledb_internal.to_microseconds(ts TIMESTAMPTZ) RETURNS BIGINT
	AS '$libdir/timescaledb', 'pg_timestamp_to_microseconds' LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_internal.to_unix_microseconds(ts TIMESTAMPTZ) RETURNS BIGINT
	AS '$libdir/timescaledb', 'pg_timestamp_to_unix_microseconds' LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_internal.to_timestamp(unixtime_us BIGINT) RETURNS TIMESTAMPTZ
	AS '$libdir/timescaledb', 'pg_unix_microseconds_to_timestamp' LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_internal.to_timestamp_pg(postgres_us BIGINT) RETURNS TIMESTAMPTZ
	AS '$libdir/timescaledb', 'pg_microseconds_to_timestamp' LANGUAGE C IMMUTABLE STRICT;

-- time_bucket returns the left edge of the bucket where ts falls into. 
-- Buckets span an interval of time equal to the bucket_width and are aligned with the epoch.
CREATE OR REPLACE FUNCTION public.time_bucket(bucket_width INTERVAL, ts TIMESTAMP) RETURNS TIMESTAMP
	AS '$libdir/timescaledb', 'timestamp_bucket' LANGUAGE C IMMUTABLE;

-- bucketing of timestamptz happens at UTC time
CREATE OR REPLACE FUNCTION public.time_bucket(bucket_width INTERVAL, ts TIMESTAMPTZ) RETURNS TIMESTAMPTZ
	AS '$libdir/timescaledb', 'timestamptz_bucket' LANGUAGE C IMMUTABLE;

-- If an interval is given as the third argument, the bucket alignment is offset by the interval.
CREATE OR REPLACE FUNCTION public.time_bucket(bucket_width INTERVAL, ts TIMESTAMP, "offset" INTERVAL)
    RETURNS TIMESTAMP LANGUAGE SQL IMMUTABLE AS
$BODY$
    SELECT public.time_bucket(bucket_width, ts-"offset")+"offset";
$BODY$;

CREATE OR REPLACE FUNCTION public.time_bucket(bucket_width INTERVAL, ts TIMESTAMPTZ, "offset" INTERVAL)
    RETURNS TIMESTAMPTZ LANGUAGE SQL IMMUTABLE AS
$BODY$
    SELECT public.time_bucket(bucket_width, ts-"offset")+"offset";
$BODY$;

CREATE OR REPLACE FUNCTION public.time_bucket(bucket_width BIGINT, ts BIGINT)
    RETURNS BIGINT LANGUAGE SQL IMMUTABLE AS
$BODY$
    SELECT (ts / bucket_width)*bucket_width;
$BODY$;

CREATE OR REPLACE FUNCTION public.time_bucket(bucket_width INT, ts INT)
    RETURNS INT LANGUAGE SQL IMMUTABLE AS
$BODY$
    SELECT (ts / bucket_width)*bucket_width;
$BODY$;

CREATE OR REPLACE FUNCTION public.time_bucket(bucket_width SMALLINT, ts SMALLINT)
    RETURNS SMALLINT LANGUAGE SQL IMMUTABLE AS
$BODY$
    SELECT (ts / bucket_width)*bucket_width;
$BODY$;

CREATE OR REPLACE FUNCTION public.time_bucket(bucket_width BIGINT, ts BIGINT, "offset" BIGINT)
    RETURNS BIGINT LANGUAGE SQL IMMUTABLE AS
$BODY$
    SELECT (((ts-"offset") / bucket_width)*bucket_width)+"offset";
$BODY$;

CREATE OR REPLACE FUNCTION public.time_bucket(bucket_width INT, ts INT, "offset" INT)
    RETURNS INT LANGUAGE SQL IMMUTABLE AS
$BODY$
    SELECT (((ts-"offset") / bucket_width)*bucket_width)+"offset";
$BODY$;

CREATE OR REPLACE FUNCTION public.time_bucket(bucket_width SMALLINT, ts SMALLINT, "offset" SMALLINT)
    RETURNS SMALLINT LANGUAGE SQL IMMUTABLE AS
$BODY$
    SELECT (((ts-"offset") / bucket_width)*bucket_width)+"offset";
$BODY$;



-- Time can be represented in a hypertable as an int* (bigint/integer/smallint) or as a timestamp type (
-- with or without timezones). In or metatables and other internal systems all time values are stored as bigint.
-- Converting from int* columns to internal representation is a cast to bigint.
-- Converting from timestamps to internal representation is conversion to epoch (in microseconds).

-- Gets the sql code for extracting the internal (bigint) time value from the identifier with the given column_type.
CREATE OR REPLACE FUNCTION _timescaledb_internal.extract_time_sql(
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
        RETURN format('(_timescaledb_internal.to_unix_microseconds(%s::timestamptz))', identifier); --microseconds since UTC epoch
    END CASE;
END
$BODY$;

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
        RETURN format('%L', time_value); --scale determined by user.
      WHEN 'TIMESTAMP'::regtype, 'TIMESTAMPTZ'::regtype THEN
        --assume time_value is in microsec
        RETURN format('%2$s %1$L', _timescaledb_internal.to_timestamp(time_value), column_type); --microseconds
    END CASE;
END
$BODY$;

--Convert a interval to microseconds.
CREATE OR REPLACE FUNCTION _timescaledb_internal.interval_to_usec(
       chunk_interval INTERVAL
)
RETURNS BIGINT LANGUAGE SQL IMMUTABLE AS
$BODY$
    SELECT (int_sec * 1000000)::bigint from extract(epoch from chunk_interval) as int_sec;
$BODY$;
