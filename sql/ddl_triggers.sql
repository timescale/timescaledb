CREATE OR REPLACE FUNCTION _timescaledb_internal.ddl_command_end() RETURNS event_trigger
AS '@MODULE_PATHNAME@', 'timescaledb_ddl_command_end' LANGUAGE C;

DROP EVENT TRIGGER IF EXISTS timescaledb_ddl_command_end;
--EVENT TRIGGER MUST exclude the ALTER EXTENSION tag.
CREATE EVENT TRIGGER timescaledb_ddl_command_end ON ddl_command_end
WHEN TAG IN ('ALTER TABLE','CREATE TRIGGER','CREATE TABLE','CREATE INDEX','ALTER INDEX')
EXECUTE PROCEDURE _timescaledb_internal.ddl_command_end();
