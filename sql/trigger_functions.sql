-- Make sure any trigger functions or event trigger functions defined here.
-- This file is called first in any upgrade or install script to make sure
-- these functions match the new .so before they are called.
-- All functions here should be disabled -- in c -- during upgrades.

-- This function is called for any ddl event.
CREATE OR REPLACE FUNCTION _timescaledb_internal.process_ddl_event() RETURNS event_trigger
AS '@MODULE_PATHNAME@', 'timescaledb_process_ddl_event' LANGUAGE C;
