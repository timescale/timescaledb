-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION _timescaledb_functions.generate_uuid_v7() RETURNS UUID
AS '@MODULE_PATHNAME@', 'ts_uuid_generate_v7' LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_functions.uuid_v7_from_timestamptz(
  ts TIMESTAMPTZ
) RETURNS UUID
AS '@MODULE_PATHNAME@', 'ts_uuid_v7_from_timestamptz' LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_functions.timestamptz_from_uuid_v7(
  uuid UUID
) RETURNS TIMESTAMPTZ
AS '@MODULE_PATHNAME@', 'ts_timestamptz_from_uuid_v7' LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_functions.uuid_version(
  uuid UUID
) RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_uuid_version' LANGUAGE C STRICT;

