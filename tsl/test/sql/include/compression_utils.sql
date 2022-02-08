-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set ECHO errors

--
--  decompression functions for testing; not exposed in normal installation
--

CREATE OR REPLACE FUNCTION _timescaledb_internal.decompress_forward(_timescaledb_internal.compressed_data, ANYELEMENT)
   RETURNS TABLE (value ANYELEMENT)
   AS :MODULE_PATHNAME, 'ts_compressed_data_decompress_forward'
   ROWS 1000
   LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.decompress_reverse(_timescaledb_internal.compressed_data, ANYELEMENT)
   RETURNS TABLE (value ANYELEMENT)
   AS :MODULE_PATHNAME, 'ts_compressed_data_decompress_reverse'
   ROWS 1000
   LANGUAGE C IMMUTABLE PARALLEL SAFE;


--
-- compression-specific aggregates for testing; not exposed in normal installation
--
CREATE OR REPLACE FUNCTION _timescaledb_internal.deltadelta_compressor_append(internal, BIGINT)
   RETURNS internal
   AS :MODULE_PATHNAME, 'ts_deltadelta_compressor_append'
   LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.deltadelta_compressor_finish(internal)
   RETURNS _timescaledb_internal.compressed_data
   AS :MODULE_PATHNAME, 'ts_deltadelta_compressor_finish'
   LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_internal.gorilla_compressor_append(internal, DOUBLE PRECISION)
   RETURNS internal
   AS :MODULE_PATHNAME, 'ts_gorilla_compressor_append'
   LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.gorilla_compressor_finish(internal)
   RETURNS _timescaledb_internal.compressed_data
   AS :MODULE_PATHNAME, 'ts_gorilla_compressor_finish'
   LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_internal.compressor_append_timestamptz(internal, timestamptz)
   RETURNS internal
   AS :MODULE_PATHNAME, 'ts_deltadelta_compressor_append'
   LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.timestamptz_compress_finish(internal)
   RETURNS _timescaledb_internal.compressed_data
   AS :MODULE_PATHNAME, 'ts_deltadelta_compressor_finish'
   LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_internal.dictionary_compressor_append(internal, ANYELEMENT)
   RETURNS internal
   AS :MODULE_PATHNAME, 'ts_dictionary_compressor_append'
   LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.dictionary_compressor_finish(internal)
   RETURNS _timescaledb_internal.compressed_data
   AS :MODULE_PATHNAME, 'ts_dictionary_compressor_finish'
   LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_internal.array_compressor_append(internal, ANYELEMENT)
   RETURNS internal
   AS :MODULE_PATHNAME, 'ts_array_compressor_append'
   LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.array_compressor_finish(internal)
   RETURNS _timescaledb_internal.compressed_data
   AS :MODULE_PATHNAME, 'ts_array_compressor_finish'
   LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE AGGREGATE _timescaledb_internal.compress_deltadelta(BIGINT) (
    STYPE = internal,
    SFUNC = _timescaledb_internal.deltadelta_compressor_append,
    FINALFUNC = _timescaledb_internal.deltadelta_compressor_finish
);

CREATE AGGREGATE _timescaledb_internal.compress_deltadelta(timestamptz) (
    STYPE = internal,
    SFUNC = _timescaledb_internal.compressor_append_timestamptz,
    FINALFUNC = _timescaledb_internal.timestamptz_compress_finish
);

CREATE AGGREGATE _timescaledb_internal.compress_gorilla(DOUBLE PRECISION) (
    STYPE = internal,
    SFUNC = _timescaledb_internal.gorilla_compressor_append,
    FINALFUNC = _timescaledb_internal.gorilla_compressor_finish
);

CREATE AGGREGATE _timescaledb_internal.compress_dictionary(ANYELEMENT) (
    STYPE = internal,
    SFUNC = _timescaledb_internal.dictionary_compressor_append,
    FINALFUNC = _timescaledb_internal.dictionary_compressor_finish
);

CREATE AGGREGATE _timescaledb_internal.compress_array(ANYELEMENT) (
    STYPE = internal,
    SFUNC = _timescaledb_internal.array_compressor_append,
    FINALFUNC = _timescaledb_internal.array_compressor_finish
);

-- Helper functions to recompress all chunks of a hypertable
-- Parameters:
--   hypertable: The hypertable to recompress all chunks on
--   lim: number of chunks to compress
--   if_not_compressed: notice instead of error if chunk already compressed
CREATE OR REPLACE PROCEDURE recompress_all_chunks(
    hypertable REGCLASS,
    lim INT,
    if_not_compressed BOOLEAN
) AS $$
DECLARE
  chunk regclass;
BEGIN
  FOR chunk IN SELECT show_chunks(hypertable) ORDER BY 1 LIMIT lim
  LOOP
    CALL public.recompress_chunk(chunk, if_not_compressed);
  END LOOP;
END $$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE recompress_all_chunks(
    hypertable REGCLASS,
    if_not_compressed BOOLEAN
) AS $$
DECLARE
  chunk regclass;
BEGIN
  FOR chunk IN SELECT show_chunks(hypertable) ORDER BY 1
  LOOP
    CALL public.recompress_chunk(chunk, if_not_compressed);
  END LOOP;
END $$ LANGUAGE plpgsql;

\set ECHO all
