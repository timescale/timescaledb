-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION _timescaledb_functions.compression_codec_handler(internal) RETURNS index_am_handler
AS '@MODULE_PATHNAME@', 'ts_compression_codec_handler' LANGUAGE C;
