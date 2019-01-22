-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION _timescaledb_internal.get_tabledef(tbl REGCLASS) RETURNS TEXT
AS '@MODULE_PATHNAME@', 'ts_get_tabledef' LANGUAGE C VOLATILE STRICT;