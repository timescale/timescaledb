-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Postgres has special types such as timestamp, date, timestamptz, etc. to
-- represent logical time values. On top of these, TimescaleDB allows integers
-- to represent logical time values. Postgres INTERVAL types are suited only for
-- logical time types in postgres. The type defined below is an INTERVAL equivalent
-- for TimescaleDB and enables to represent time intervals for both postgres time
-- type valued and integer valued time columns.
CREATE TYPE _timescaledb_catalog.ts_interval AS (
    is_time_interval        BOOLEAN,
    time_interval		    INTERVAL,
    integer_interval        BIGINT
    );

-- PostgreSQL composite types do not support constraint checks. That is why any table having a ts_interval column must use the following
-- function for constraint validation.
-- This function needs to be defined before executing pre_install/tables.sql because it is used as
-- validation constraint for columns of type ts_interval.
CREATE OR REPLACE FUNCTION _timescaledb_internal.valid_ts_interval(invl _timescaledb_catalog.ts_interval)
RETURNS BOOLEAN AS '@MODULE_PATHNAME@', 'ts_valid_ts_interval' LANGUAGE C VOLATILE STRICT;
