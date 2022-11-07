-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- time_bucket returns the left edge of the bucket where ts falls into.
-- Buckets span an interval of time equal to the bucket_width and are aligned with the epoch.
CREATE OR REPLACE FUNCTION @extschema@.time_bucket(bucket_width INTERVAL, ts TIMESTAMP) RETURNS TIMESTAMP
	AS '@MODULE_PATHNAME@', 'ts_timestamp_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

-- bucketing of timestamptz happens at UTC time
CREATE OR REPLACE FUNCTION @extschema@.time_bucket(bucket_width INTERVAL, ts TIMESTAMPTZ) RETURNS TIMESTAMPTZ
	AS '@MODULE_PATHNAME@', 'ts_timestamptz_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

--bucketing on date should not do any timezone conversion
CREATE OR REPLACE FUNCTION @extschema@.time_bucket(bucket_width INTERVAL, ts DATE) RETURNS DATE
	AS '@MODULE_PATHNAME@', 'ts_date_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

--bucketing with origin
CREATE OR REPLACE FUNCTION @extschema@.time_bucket(bucket_width INTERVAL, ts TIMESTAMP, origin TIMESTAMP) RETURNS TIMESTAMP
	AS '@MODULE_PATHNAME@', 'ts_timestamp_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;
CREATE OR REPLACE FUNCTION @extschema@.time_bucket(bucket_width INTERVAL, ts TIMESTAMPTZ, origin TIMESTAMPTZ) RETURNS TIMESTAMPTZ
	AS '@MODULE_PATHNAME@', 'ts_timestamptz_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;
CREATE OR REPLACE FUNCTION @extschema@.time_bucket(bucket_width INTERVAL, ts DATE, origin DATE) RETURNS DATE
	AS '@MODULE_PATHNAME@', 'ts_date_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

--bucketing with offset
CREATE OR REPLACE FUNCTION @extschema@.time_bucket(bucket_width INTERVAL, ts TIMESTAMP, "offset" INTERVAL) RETURNS TIMESTAMP
	AS '@MODULE_PATHNAME@', 'ts_timestamp_offset_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;
CREATE OR REPLACE FUNCTION @extschema@.time_bucket(bucket_width INTERVAL, ts TIMESTAMPTZ, "offset" INTERVAL) RETURNS TIMESTAMPTZ
	AS '@MODULE_PATHNAME@', 'ts_timestamptz_offset_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;
CREATE OR REPLACE FUNCTION @extschema@.time_bucket(bucket_width INTERVAL, ts DATE, "offset" INTERVAL) RETURNS DATE
	AS '@MODULE_PATHNAME@', 'ts_date_offset_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

-- bucketing with timezone
CREATE OR REPLACE FUNCTION @extschema@.time_bucket(bucket_width INTERVAL, ts TIMESTAMPTZ, timezone TEXT, origin TIMESTAMPTZ DEFAULT NULL, "offset" INTERVAL DEFAULT NULL) RETURNS TIMESTAMPTZ
	AS '@MODULE_PATHNAME@', 'ts_timestamptz_timezone_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE;

-- bucketing of int
CREATE OR REPLACE FUNCTION @extschema@.time_bucket(bucket_width SMALLINT, ts SMALLINT) RETURNS SMALLINT
	AS '@MODULE_PATHNAME@', 'ts_int16_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;
CREATE OR REPLACE FUNCTION @extschema@.time_bucket(bucket_width INT, ts INT) RETURNS INT
	AS '@MODULE_PATHNAME@', 'ts_int32_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;
CREATE OR REPLACE FUNCTION @extschema@.time_bucket(bucket_width BIGINT, ts BIGINT) RETURNS BIGINT
	AS '@MODULE_PATHNAME@', 'ts_int64_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

-- bucketing of int with offset
CREATE OR REPLACE FUNCTION @extschema@.time_bucket(bucket_width SMALLINT, ts SMALLINT, "offset" SMALLINT) RETURNS SMALLINT
	AS '@MODULE_PATHNAME@', 'ts_int16_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;
CREATE OR REPLACE FUNCTION @extschema@.time_bucket(bucket_width INT, ts INT, "offset" INT) RETURNS INT
	AS '@MODULE_PATHNAME@', 'ts_int32_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;
CREATE OR REPLACE FUNCTION @extschema@.time_bucket(bucket_width BIGINT, ts BIGINT, "offset" BIGINT) RETURNS BIGINT
	AS '@MODULE_PATHNAME@', 'ts_int64_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;
