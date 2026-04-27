-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION _timescaledb_functions.compressed_data_to_array(_timescaledb_internal.compressed_data, ANYELEMENT)
   RETURNS ANYARRAY
   AS '@MODULE_PATHNAME@', 'ts_compressed_data_to_array'
   LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.compressed_data_column_size(_timescaledb_internal.compressed_data, ANYELEMENT)
   RETURNS INTEGER
   AS '@MODULE_PATHNAME@', 'ts_compressed_data_column_size'
   LANGUAGE C IMMUTABLE PARALLEL SAFE;

-- Compute the generated metadata column name for a given metadata type and
-- ordered column list. Used by the #9578 migration to rename pre-fix composite
-- bloom columns to the post-fix collision-safe format.
CREATE OR REPLACE FUNCTION _timescaledb_functions.compressed_column_metadata_name(metadata_type TEXT, column_names TEXT[])
   RETURNS TEXT
   AS '@MODULE_PATHNAME@', 'ts_compressed_column_metadata_name'
   LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- Rename a column on a compressed chunk, bypassing the normal chunk-rename
-- restriction. Used exclusively by the #9578 upgrade path.
CREATE OR REPLACE FUNCTION _timescaledb_functions.rename_compressed_column(compress_relid REGCLASS, old_name TEXT, new_name TEXT)
   RETURNS VOID
   AS '@MODULE_PATHNAME@', 'ts_rename_compressed_column'
   LANGUAGE C VOLATILE STRICT;
