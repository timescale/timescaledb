-- Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
--
-- This file is licensed under the Apache License, see LICENSE-APACHE
-- at the top level directory of the TimescaleDB distribution.

CREATE FUNCTION timescaledb_fdw_handler()
RETURNS fdw_handler
AS '@MODULE_PATHNAME@', 'ts_timescaledb_fdw_handler'
LANGUAGE C STRICT;

CREATE FUNCTION timescaledb_fdw_validator(text[], oid)
RETURNS void
AS '@MODULE_PATHNAME@', 'ts_timescaledb_fdw_validator'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER timescaledb_fdw
  HANDLER timescaledb_fdw_handler
  VALIDATOR timescaledb_fdw_validator;
  