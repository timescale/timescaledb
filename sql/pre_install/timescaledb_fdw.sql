-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE FOREIGN DATA WRAPPER timescaledb_fdw
  HANDLER timescaledb_fdw_handler
  VALIDATOR timescaledb_fdw_validator;
