-- time_bucket returns the left edge of the bucket where ts falls into.
-- Buckets span an interval of time equal to the bucket_width and are aligned with the epoch.
CREATE OR REPLACE FUNCTION time_bucket(bucket_width INTERVAL, ts TIMESTAMP) RETURNS TIMESTAMP
	AS '@MODULE_PATHNAME@', 'timestamp_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE;

-- bucketing of timestamptz happens at UTC time
CREATE OR REPLACE FUNCTION time_bucket(bucket_width INTERVAL, ts TIMESTAMPTZ) RETURNS TIMESTAMPTZ
	AS '@MODULE_PATHNAME@', 'timestamptz_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE;

--bucketing on date should not do any timezone conversion
CREATE OR REPLACE FUNCTION time_bucket(bucket_width INTERVAL, ts DATE) RETURNS DATE
	AS '@MODULE_PATHNAME@', 'date_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE;

-- If an interval is given as the third argument, the bucket alignment is offset by the interval.
CREATE OR REPLACE FUNCTION time_bucket(bucket_width INTERVAL, ts TIMESTAMP, "offset" INTERVAL)
    RETURNS TIMESTAMP LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS
$BODY$
    SELECT time_bucket(bucket_width, ts-"offset")+"offset";
$BODY$;

CREATE OR REPLACE FUNCTION time_bucket(bucket_width INTERVAL, ts TIMESTAMPTZ, "offset" INTERVAL)
    RETURNS TIMESTAMPTZ LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS
$BODY$
    SELECT time_bucket(bucket_width, ts-"offset")+"offset";
$BODY$;

CREATE OR REPLACE FUNCTION time_bucket(bucket_width INTERVAL, ts DATE, "offset" INTERVAL)
    RETURNS DATE LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS
$BODY$
    SELECT (time_bucket(bucket_width, ts-"offset")+"offset")::date;
$BODY$;


CREATE OR REPLACE FUNCTION time_bucket(bucket_width BIGINT, ts BIGINT)
    RETURNS BIGINT LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS
$BODY$
    SELECT (ts / bucket_width)*bucket_width;
$BODY$;

CREATE OR REPLACE FUNCTION time_bucket(bucket_width INT, ts INT)
    RETURNS INT LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS
$BODY$
    SELECT (ts / bucket_width)*bucket_width;
$BODY$;

CREATE OR REPLACE FUNCTION time_bucket(bucket_width SMALLINT, ts SMALLINT)
    RETURNS SMALLINT LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS
$BODY$
    SELECT (ts / bucket_width)*bucket_width;
$BODY$;

CREATE OR REPLACE FUNCTION time_bucket(bucket_width BIGINT, ts BIGINT, "offset" BIGINT)
    RETURNS BIGINT LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS
$BODY$
    SELECT (((ts-"offset") / bucket_width)*bucket_width)+"offset";
$BODY$;

CREATE OR REPLACE FUNCTION time_bucket(bucket_width INT, ts INT, "offset" INT)
    RETURNS INT LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS
$BODY$
    SELECT (((ts-"offset") / bucket_width)*bucket_width)+"offset";
$BODY$;

CREATE OR REPLACE FUNCTION time_bucket(bucket_width SMALLINT, ts SMALLINT, "offset" SMALLINT)
    RETURNS SMALLINT LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS
$BODY$
    SELECT (((ts-"offset") / bucket_width)*bucket_width)+"offset";
$BODY$;
