-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION _timescaledb_internal.recompress_chunk_sfunc(
tstate internal,compressed_chunk_name regclass, val anyelement)
RETURNS internal
AS '@MODULE_PATHNAME@', 'ts_recompress_chunk_sfunc'
LANGUAGE C IMMUTABLE ;;

DROP FUNCTION IF EXISTS _timescaledb_internal.recompress_chunk_ffunc;

CREATE OR REPLACE FUNCTION _timescaledb_internal.recompress_chunk_ffunc( tstate internal, compressed_chunk_name regclass, extra anyelement)
RETURNS anyarray
AS '@MODULE_PATHNAME@', 'ts_recompress_chunk_ffunc'
LANGUAGE C IMMUTABLE ;
