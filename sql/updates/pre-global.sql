--need to keep this legacy functions around for 0.9 since it's still used in 0.8, delete after 0.9
CREATE OR REPLACE FUNCTION _timescaledb_internal.ddl_command_end() RETURNS event_trigger
AS '@MODULE_PATHNAME@', 'timescaledb_ddl_command_end' LANGUAGE C;
