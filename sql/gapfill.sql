-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Original integer variants (4 args for backward compatibility)
CREATE OR REPLACE FUNCTION @extschema@.time_bucket_gapfill(bucket_width SMALLINT, ts SMALLINT, start SMALLINT=NULL, finish SMALLINT=NULL) RETURNS SMALLINT
	AS '@MODULE_PATHNAME@', 'ts_gapfill_int16_bucket' LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION @extschema@.time_bucket_gapfill(bucket_width INT, ts INT, start INT=NULL, finish INT=NULL) RETURNS INT
	AS '@MODULE_PATHNAME@', 'ts_gapfill_int32_bucket' LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION @extschema@.time_bucket_gapfill(bucket_width BIGINT, ts BIGINT, start BIGINT=NULL, finish BIGINT=NULL) RETURNS BIGINT
	AS '@MODULE_PATHNAME@', 'ts_gapfill_int64_bucket' LANGUAGE C VOLATILE PARALLEL SAFE;

-- Integer variants with offset (5 args)
CREATE OR REPLACE FUNCTION @extschema@.time_bucket_gapfill(bucket_width SMALLINT, ts SMALLINT, start SMALLINT, finish SMALLINT, "offset" SMALLINT) RETURNS SMALLINT
	AS '@MODULE_PATHNAME@', 'ts_gapfill_int16_bucket' LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION @extschema@.time_bucket_gapfill(bucket_width INT, ts INT, start INT, finish INT, "offset" INT) RETURNS INT
	AS '@MODULE_PATHNAME@', 'ts_gapfill_int32_bucket' LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION @extschema@.time_bucket_gapfill(bucket_width BIGINT, ts BIGINT, start BIGINT, finish BIGINT, "offset" BIGINT) RETURNS BIGINT
	AS '@MODULE_PATHNAME@', 'ts_gapfill_int64_bucket' LANGUAGE C VOLATILE PARALLEL SAFE;

-- Original timestamp variants (4 args for backward compatibility)
CREATE OR REPLACE FUNCTION @extschema@.time_bucket_gapfill(bucket_width INTERVAL, ts DATE, start DATE=NULL, finish DATE=NULL) RETURNS DATE
	AS '@MODULE_PATHNAME@', 'ts_gapfill_date_bucket' LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION @extschema@.time_bucket_gapfill(bucket_width INTERVAL, ts TIMESTAMP, start TIMESTAMP=NULL, finish TIMESTAMP=NULL) RETURNS TIMESTAMP
	AS '@MODULE_PATHNAME@', 'ts_gapfill_timestamp_bucket' LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION @extschema@.time_bucket_gapfill(bucket_width INTERVAL, ts TIMESTAMPTZ, start TIMESTAMPTZ=NULL, finish TIMESTAMPTZ=NULL) RETURNS TIMESTAMPTZ
	AS '@MODULE_PATHNAME@', 'ts_gapfill_timestamptz_bucket' LANGUAGE C VOLATILE PARALLEL SAFE;

-- Timestamp variants with origin and offset (6 args)
CREATE OR REPLACE FUNCTION @extschema@.time_bucket_gapfill(bucket_width INTERVAL, ts DATE, start DATE, finish DATE, origin DATE, "offset" INTERVAL) RETURNS DATE
	AS '@MODULE_PATHNAME@', 'ts_gapfill_date_bucket' LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION @extschema@.time_bucket_gapfill(bucket_width INTERVAL, ts TIMESTAMP, start TIMESTAMP, finish TIMESTAMP, origin TIMESTAMP, "offset" INTERVAL) RETURNS TIMESTAMP
	AS '@MODULE_PATHNAME@', 'ts_gapfill_timestamp_bucket' LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION @extschema@.time_bucket_gapfill(bucket_width INTERVAL, ts TIMESTAMPTZ, start TIMESTAMPTZ, finish TIMESTAMPTZ, origin TIMESTAMPTZ, "offset" INTERVAL) RETURNS TIMESTAMPTZ
	AS '@MODULE_PATHNAME@', 'ts_gapfill_timestamptz_bucket' LANGUAGE C VOLATILE PARALLEL SAFE;

-- Original timezone variant (5 args for backward compatibility)
CREATE OR REPLACE FUNCTION @extschema@.time_bucket_gapfill(bucket_width INTERVAL, ts TIMESTAMPTZ, timezone TEXT, start TIMESTAMPTZ=NULL, finish TIMESTAMPTZ=NULL) RETURNS TIMESTAMPTZ
	AS '@MODULE_PATHNAME@', 'ts_gapfill_timestamptz_timezone_bucket' LANGUAGE C VOLATILE PARALLEL SAFE;

-- Timezone variant with origin and offset (7 args)
CREATE OR REPLACE FUNCTION @extschema@.time_bucket_gapfill(bucket_width INTERVAL, ts TIMESTAMPTZ, timezone TEXT, start TIMESTAMPTZ, finish TIMESTAMPTZ, origin TIMESTAMPTZ, "offset" INTERVAL) RETURNS TIMESTAMPTZ
	AS '@MODULE_PATHNAME@', 'ts_gapfill_timestamptz_timezone_bucket' LANGUAGE C VOLATILE PARALLEL SAFE;

-- locf function
CREATE OR REPLACE FUNCTION @extschema@.locf(value ANYELEMENT, prev ANYELEMENT=NULL, treat_null_as_missing BOOL=false) RETURNS ANYELEMENT
	AS '@MODULE_PATHNAME@', 'ts_gapfill_marker' LANGUAGE C VOLATILE PARALLEL SAFE;

-- interpolate functions
CREATE OR REPLACE FUNCTION @extschema@.interpolate(value SMALLINT,prev RECORD=NULL,next RECORD=NULL) RETURNS SMALLINT
	AS '@MODULE_PATHNAME@', 'ts_gapfill_marker' LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION @extschema@.interpolate(value INT,prev RECORD=NULL,next RECORD=NULL) RETURNS INT
	AS '@MODULE_PATHNAME@', 'ts_gapfill_marker' LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION @extschema@.interpolate(value BIGINT,prev RECORD=NULL,next RECORD=NULL) RETURNS BIGINT
	AS '@MODULE_PATHNAME@', 'ts_gapfill_marker' LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION @extschema@.interpolate(value REAL,prev RECORD=NULL,next RECORD=NULL) RETURNS REAL
	AS '@MODULE_PATHNAME@', 'ts_gapfill_marker' LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION @extschema@.interpolate(value FLOAT,prev RECORD=NULL,next RECORD=NULL) RETURNS FLOAT
	AS '@MODULE_PATHNAME@', 'ts_gapfill_marker' LANGUAGE C VOLATILE PARALLEL SAFE;

