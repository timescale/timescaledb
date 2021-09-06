-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- time_bucket_ng() is an _experimental_ new version of time_bucket().
--
-- Unlike time_bucket(), time_bucket_ng() supports variable-sized buckets,
-- such as months and years, and also timezones. Note that the behavior
-- and the interface of this function are subjects to change. There could
-- be bugs, and the implementation doesn't claim to be complete. Use at
-- your own risk.
--
-- This function may return different results for the same arguments depending
-- on the version of local timezone database. Despite this fact, function is
-- marked as IMMUTABLE. This is consistent with the volatility [1] of the
-- functions provided by PostgreSQL. See discussion [2] for more details.
--
-- We don't forbid users to work with timestamptz's from the future, nor warn
-- about this corner case. This behavior is consistent with PostgreSQL
-- behavior [3].
--
-- [1]: https://www.postgresql.org/docs/current/xfunc-volatility.html
-- [2]: https://postgr.es/m/CAJ7c6TOMG8zSNEZtCn5SPe+cCk3Lfxb71ZaQwT2F4T7PJ_t=KA@mail.gmail.com
-- [3]: https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-TIMEZONES

-- DATE versions of time_bucket_ng().
CREATE OR REPLACE FUNCTION timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts DATE) RETURNS DATE
    AS '@MODULE_PATHNAME@', 'ts_time_bucket_ng_date' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts DATE, origin DATE) RETURNS DATE
    AS '@MODULE_PATHNAME@', 'ts_time_bucket_ng_date' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

-- TIMESTAMP versions of time_bucket_ng().
CREATE OR REPLACE FUNCTION timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts TIMESTAMP) RETURNS TIMESTAMP
    AS '@MODULE_PATHNAME@', 'ts_time_bucket_ng_timestamp' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts TIMESTAMP, origin TIMESTAMP) RETURNS TIMESTAMP
    AS '@MODULE_PATHNAME@', 'ts_time_bucket_ng_timestamp' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

-- TIMESTAMPTZ versions of time_bucket_ng().
CREATE OR REPLACE FUNCTION timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts TIMESTAMPTZ, timezone TEXT) RETURNS TIMESTAMPTZ
    AS '@MODULE_PATHNAME@', 'ts_time_bucket_ng_timezone' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;
CREATE OR REPLACE FUNCTION timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts TIMESTAMPTZ, origin TIMESTAMPTZ, timezone TEXT) RETURNS TIMESTAMPTZ
    AS '@MODULE_PATHNAME@', 'ts_time_bucket_ng_timezone_origin' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;


-- The following two versions of time_bucket_ng() are kept only for the backward
-- compatibility with time_bucket(). They convert 'ts' to UTC instead of treating
-- it in the given timezone, which is almost certainly not something you want.
-- Future versions may WARN you about this fact, and be completely removed
-- eventually.
--
-- These functions are STABLE because their implementation relies on the STABLE
-- function timestamptz_date(). The latest is STABLE because it accounts for the
-- session parameters.
CREATE OR REPLACE FUNCTION timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts TIMESTAMPTZ) RETURNS TIMESTAMPTZ
    AS '@MODULE_PATHNAME@', 'ts_time_bucket_ng_timestamptz' LANGUAGE C STABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts TIMESTAMPTZ, origin TIMESTAMPTZ) RETURNS TIMESTAMPTZ
    AS '@MODULE_PATHNAME@', 'ts_time_bucket_ng_timestamptz' LANGUAGE C STABLE PARALLEL SAFE STRICT;
