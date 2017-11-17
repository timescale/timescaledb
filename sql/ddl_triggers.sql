CREATE OR REPLACE FUNCTION _timescaledb_internal.ddl_command_end() RETURNS event_trigger
AS '$libdir/timescaledb', 'timescaledb_ddl_command_end' LANGUAGE C;

DROP EVENT TRIGGER IF EXISTS timescaledb_ddl_command_end;
CREATE EVENT TRIGGER timescaledb_ddl_command_end ON ddl_command_end
EXECUTE PROCEDURE _timescaledb_internal.ddl_command_end();
