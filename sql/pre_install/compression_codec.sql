-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Virtual access method: Register an EXTERNAL compression codec for a
-- type by creating an operator class for this access method. Columns
-- use a registered codec through the per-column setting
-- timescaledb.compress_column_codec = '<column>:<operator class>, ...'
-- * Support function 1:    bytea compress(<type>[])
-- * Support function 2: <type>[] decompress(bytea)
CREATE ACCESS METHOD ts_compression_codec TYPE INDEX HANDLER _timescaledb_functions.compression_codec_handler;
COMMENT ON ACCESS METHOD ts_compression_codec IS 'Registry for compression codec operator classes';
