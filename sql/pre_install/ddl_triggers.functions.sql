-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION _timescaledb_functions.process_ddl_event() RETURNS event_trigger
AS '@MODULE_PATHNAME@', 'ts_timescaledb_process_ddl_event' LANGUAGE C;

