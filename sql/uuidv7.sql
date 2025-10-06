-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION @extschema@.generate_uuidv7() RETURNS UUID
AS '@MODULE_PATHNAME@', 'ts_uuid_generate_v7' LANGUAGE C VOLATILE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION @extschema@.to_uuidv7(
  ts TIMESTAMPTZ
) RETURNS UUID
AS '@MODULE_PATHNAME@', 'ts_uuid_v7_from_timestamptz' LANGUAGE C VOLATILE STRICT PARALLEL SAFE;

--
-- Produce a boundary UUIDv7 from a timestamp, with all otherwise
-- random bits in the resulting UUID set to zero. Useful for
-- time-range queries directly on a UUID column.
--
CREATE OR REPLACE FUNCTION @extschema@.to_uuidv7_boundary(
  ts TIMESTAMPTZ
) RETURNS UUID
AS '@MODULE_PATHNAME@', 'ts_uuid_v7_from_timestamptz_boundary' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

--
-- Get the v7 UUID timestamp with millisecond precision.
--
CREATE OR REPLACE FUNCTION @extschema@.uuid_timestamp(
  uuid UUID
) RETURNS TIMESTAMPTZ
AS '@MODULE_PATHNAME@', 'ts_timestamptz_from_uuid_v7' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

--
-- Get the v7 UUID timestamp with microsecond precision using the
-- (optional) rand_a bits.
--
CREATE OR REPLACE FUNCTION @extschema@.uuid_timestamp_micros(
  uuid UUID
) RETURNS TIMESTAMPTZ
AS '@MODULE_PATHNAME@', 'ts_timestamptz_from_uuid_v7_with_microseconds' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION @extschema@.uuid_version(
  uuid UUID
) RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_uuid_version' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
