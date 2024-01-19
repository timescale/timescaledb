-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- This file contains utility functions and views that are used for
-- debugging in release builds. These are all placed in the schema
-- _timescaledb_debug.

CREATE OR REPLACE FUNCTION _timescaledb_debug.extension_state() RETURNS TEXT
AS '@MODULE_PATHNAME@', 'ts_extension_get_state' LANGUAGE C;
