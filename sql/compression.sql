-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE FUNCTION ts_compressionam_handler(internal) RETURNS table_am_handler
AS '@MODULE_PATHNAME@', 'ts_compressionam_handler' LANGUAGE C;

CREATE ACCESS METHOD hyperstore TYPE TABLE HANDLER ts_compressionam_handler;
COMMENT ON ACCESS METHOD hyperstore IS 'TimescaleDB columnar compression';
