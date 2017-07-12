CREATE EVENT TRIGGER ddl_drop_trigger
ON sql_drop
EXECUTE PROCEDURE _timescaledb_internal.ddl_process_drop_trigger();
