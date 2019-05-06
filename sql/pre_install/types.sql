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

--
-- The general compressed_data type;
--

CREATE TYPE _timescaledb_internal.compressed_data;

--the textual input/output is simply base64 encoding of the binary representation
CREATE FUNCTION _timescaledb_internal.compressed_data_in(CSTRING)
   RETURNS _timescaledb_internal.compressed_data
   AS '@MODULE_PATHNAME@', 'ts_compressed_data_in'
   LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION _timescaledb_internal.compressed_data_out(_timescaledb_internal.compressed_data)
   RETURNS CSTRING
   AS '@MODULE_PATHNAME@', 'ts_compressed_data_out'
   LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION _timescaledb_internal.compressed_data_send(_timescaledb_internal.compressed_data)
   RETURNS BYTEA
   AS '@MODULE_PATHNAME@', 'ts_compressed_data_send'
   LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION _timescaledb_internal.compressed_data_recv(internal)
   RETURNS _timescaledb_internal.compressed_data
   AS '@MODULE_PATHNAME@', 'ts_compressed_data_recv'
   LANGUAGE C IMMUTABLE STRICT;

CREATE TYPE _timescaledb_internal.compressed_data (
    INTERNALLENGTH = VARIABLE,
    STORAGE = EXTERNAL,
    ALIGNMENT = DOUBLE, --needed for alignment in ARRAY type compression
    INPUT = _timescaledb_internal.compressed_data_in,
    OUTPUT = _timescaledb_internal.compressed_data_out,
    RECEIVE = _timescaledb_internal.compressed_data_recv,
    SEND = _timescaledb_internal.compressed_data_send
);
