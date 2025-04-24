-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.


-- Functions have to be run in 2 places:
-- 1) In pre-install between types.pre.sql and types.post.sql to set up the types.
-- 2) On every update to make sure the function points to the correct versioned.so


-- PostgreSQL composite types do not support constraint checks. That is why any table having a ts_interval column must use the following
-- function for constraint validation.
-- This function needs to be defined before executing pre_install/tables.sql because it is used as
-- validation constraint for columns of type ts_interval.

--the textual input/output is simply base64 encoding of the binary representation
CREATE OR REPLACE FUNCTION _timescaledb_functions.compressed_data_in(CSTRING)
   RETURNS _timescaledb_internal.compressed_data
   AS '@MODULE_PATHNAME@', 'ts_compressed_data_in'
   LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_functions.compressed_data_out(_timescaledb_internal.compressed_data)
   RETURNS CSTRING
   AS '@MODULE_PATHNAME@', 'ts_compressed_data_out'
   LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_functions.compressed_data_send(_timescaledb_internal.compressed_data)
   RETURNS BYTEA
   AS '@MODULE_PATHNAME@', 'ts_compressed_data_send'
   LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_functions.compressed_data_recv(internal)
   RETURNS _timescaledb_internal.compressed_data
   AS '@MODULE_PATHNAME@', 'ts_compressed_data_recv'
   LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_functions.compressed_data_info(_timescaledb_internal.compressed_data)
    RETURNS TABLE (algorithm name, has_nulls bool)
    LANGUAGE C STRICT IMMUTABLE
    AS '@MODULE_PATHNAME@', 'ts_compressed_data_info';

CREATE OR REPLACE FUNCTION _timescaledb_functions.compressed_data_has_nulls(_timescaledb_internal.compressed_data)
    RETURNS BOOL
    LANGUAGE C STRICT IMMUTABLE
    AS '@MODULE_PATHNAME@', 'ts_compressed_data_has_nulls';

CREATE OR REPLACE FUNCTION _timescaledb_functions.dimension_info_in(cstring)
    RETURNS _timescaledb_internal.dimension_info
    LANGUAGE C STRICT IMMUTABLE
    AS '@MODULE_PATHNAME@', 'ts_dimension_info_in';

CREATE OR REPLACE FUNCTION _timescaledb_functions.dimension_info_out(_timescaledb_internal.dimension_info)
    RETURNS cstring
    LANGUAGE C STRICT IMMUTABLE
    AS '@MODULE_PATHNAME@', 'ts_dimension_info_out';

