-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION _timescaledb_internal.first_sfunc(internal, anyelement, "any")
RETURNS internal
AS '@MODULE_PATHNAME@', 'ts_first_sfunc'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.first_combinefunc(internal, internal)
RETURNS internal
AS '@MODULE_PATHNAME@', 'ts_first_combinefunc'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.last_sfunc(internal, anyelement, "any")
RETURNS internal
AS '@MODULE_PATHNAME@', 'ts_last_sfunc'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.last_combinefunc(internal, internal)
RETURNS internal
AS '@MODULE_PATHNAME@', 'ts_last_combinefunc'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.bookend_finalfunc(internal, anyelement, "any")
RETURNS anyelement
AS '@MODULE_PATHNAME@', 'ts_bookend_finalfunc'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.bookend_serializefunc(internal)
RETURNS bytea
AS '@MODULE_PATHNAME@', 'ts_bookend_serializefunc'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.bookend_deserializefunc(bytea, internal)
RETURNS internal
AS '@MODULE_PATHNAME@', 'ts_bookend_deserializefunc'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
